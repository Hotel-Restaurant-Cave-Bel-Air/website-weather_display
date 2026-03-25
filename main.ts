// Bel-Air Proxy (Deno Deploy) — KV + in-memory dedupe
// Endpoints :
//   GET /water  -> { temp_c, updated_at }
//   GET /meteo  -> { air_temp_c, air_humidity, temp_trend, raining, rain_now_mm, rain_1h_mm,
//                    wind_kmh, gust_kmh, wind_dir_deg, pressure_hpa, pressure_trend,
//                    feels_like_c, feels_like_algo_used, updated_at }
//   GET /info   -> mix eau + meteo (mêmes champs, préfixes adaptés)
//   GET /health -> état caches + env
//
// Env (Settings → Environment Variables):
//   NETATMO_CLIENT_ID
//   NETATMO_CLIENT_SECRET
//   NETATMO_REFRESH_TOKEN
//   NETATMO_DEVICE_ID      (optionnel, fallback = 1ère station)
//
// Notes cache :
// - In-memory : rapide, par instance/région (disparaît au cold start).
// - Deno KV   : persistant cross-instances (expire après fenêtre “stale”).
//   On lit KV d’abord ; si TTL fresh expiré on tente réseau, sinon on renvoie “stale”.

// ---------- Base ----------
const CORS = { 'Access-Control-Allow-Origin': '*' };
const HEADERS_HTML = {
  'User-Agent': 'Mozilla/5.0 (Bel-Air Proxy)',
  'Accept': 'text/html'
};
const nowIso = () => new Date().toISOString();
const jres = (obj, status = 200, extra = {}) =>
  new Response(JSON.stringify(obj), { status, headers: { ...CORS, 'Content-Type': 'application/json; charset=utf-8', ...extra } });

// ---------- TTLs ----------
const METEO_TTL_MS   = 5 * 60 * 1000;   // 5 min fresh
const METEO_STALE_MS = 15 * 60 * 1000;  // 15 min stale allowed on error
const WATER_TTL_MS   = 60 * 60 * 6 * 1000;   // 6h fresh

// ---------- KV ----------
const kv = await Deno.openKv();
async function kvGet(keyArr) { const r = await kv.get(keyArr); return r.value || null; }
async function kvSet(keyArr, value, expireIn) { return kv.set(keyArr, value, { expireIn }); }

// Generic KV helper (fresh TTL + stale window)
async function getWithKv(key, fetcher, ttlMs, staleMs) {
  const keyArr = ['cache', key];
  const now = Date.now();
  const cached = await kvGet(keyArr); // { data, ts }
  if (cached && (now - cached.ts) < ttlMs) {
    return { data: cached.data, meta: { source: 'kv-fresh', age_ms: now - cached.ts } };
  }
  try {
    const data = await fetcher();
    await kvSet(keyArr, { data, ts: now }, staleMs); // expire after stale window
    return { data, meta: { source: 'refreshed', age_ms: 0 } };
  } catch (e) {
    if (cached && (now - cached.ts) < staleMs) {
      return { data: cached.data, meta: { source: 'kv-stale', age_ms: now - cached.ts, error: String(e?.message || e) } };
    }
    throw e;
  }
}

// ---------- In-memory dedupe ----------
let tokenCache = { access_token: null, exp: 0 };
let METEO_CACHE  = { data: null, ts: 0 }, METEO_PENDING  = null;
let WATER_CACHE  = { data: null, ts: 0 }, WATER_PENDING  = null;

// ---------- OAuth Netatmo ----------
async function getAccessToken() {
  const now = Math.floor(Date.now() / 1000);
  if (tokenCache.access_token && tokenCache.exp > now + 30) return tokenCache.access_token;

  const client_id     = (Deno.env.get('NETATMO_CLIENT_ID') || '').trim();
  const client_secret = (Deno.env.get('NETATMO_CLIENT_SECRET') || '').trim();
  const refresh_token = (Deno.env.get('NETATMO_REFRESH_TOKEN') || '').trim();
  if (!client_id || !client_secret || !refresh_token) throw new Error('netatmo_env_missing');

  const body = new URLSearchParams({ grant_type: 'refresh_token', client_id, client_secret, refresh_token });
  const r = await fetch('https://api.netatmo.com/oauth2/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body
  });
  if (!r.ok) throw new Error('netatmo_token_http_' + r.status);
  const j = await r.json();
  tokenCache.access_token = j.access_token;
  tokenCache.exp = Math.floor(Date.now() / 1000) + (j.expires_in || 1800);
  return tokenCache.access_token;
}

// ---------- Feels like (affiné) ----------
function windChillC(T, vKmh) {
  if (typeof T !== 'number' || typeof vKmh !== 'number') return T ?? null;
  if (T > 10 || vKmh <= 4.8) return T;
  return 13.12 + 0.6215*T - 11.37*Math.pow(vKmh,0.16) + 0.3965*T*Math.pow(vKmh,0.16);
}
function dewPointC(T, RH) {
  if (typeof T !== 'number' || typeof RH !== 'number' || RH <= 0) return null;
  const a=17.27, b=237.7; const g=(a*T)/(b+T) + Math.log(RH/100); return (b*g)/(a-g);
}
function humidex(T, RH) {
  const Td = dewPointC(T, RH); if (Td == null) return T;
  const e = 6.11 * Math.exp(5417.7530*(1/273.16 - 1/(273.15 + Td))); return T + 0.5555*(e-10.0);
}
function heatIndexC(Tc, RH) {
  if (typeof Tc !== 'number' || typeof RH !== 'number') return Tc ?? null;
  if (Tc < 27) return Tc; // NOAA approx
  const Tf = Tc*9/5+32;
  const HI = -42.379 + 2.04901523*Tf + 10.14333127*RH
    - 0.22475541*Tf*RH - 0.00683783*Tf*Tf - 0.05481717*RH*RH
    + 0.00122874*Tf*Tf*RH + 0.00085282*Tf*RH*RH - 0.00000199*Tf*Tf*RH*RH;
  return (HI - 32)*5/9;
}
// blend progressif 15→22°C pour éviter les sauts
function blendWarm(tempC, warmVal, t0=15, t1=22) {
  if (warmVal == null || typeof tempC !== 'number') return warmVal ?? tempC ?? null;
  if (tempC <= t0) return tempC;
  if (tempC >= t1) return warmVal;
  const w = (tempC - t0) / (t1 - t0);
  return tempC + w * (warmVal - tempC);
}
function feelsLikeC({ tempC, rh, windKmh, algoWarm='humidex' }) {
  if (typeof tempC !== 'number') return { value: null, algo: 'none' };
  if (tempC <= 10 && (windKmh||0) > 4.8) {
    return { value: windChillC(tempC, windKmh||0), algo: 'windchill' };
  }
  let useWarm = false;
  if (tempC >= 15) {
    if (typeof rh === 'number' && rh >= 50) useWarm = true;
    else {
      const Td = dewPointC(tempC, rh ?? 0);
      if (Td != null && Td >= 12) useWarm = true;
    }
  }
  if (useWarm) {
    if (algoWarm === 'heat-index') return { value: blendWarm(tempC, heatIndexC(tempC, rh ?? 0)), algo: 'heat-index' };
    return { value: blendWarm(tempC, humidex(tempC, rh ?? 0)), algo: 'humidex' };
  }
  return { value: tempC, algo: 'none' };
}

// ---------- Netatmo parse (EXT only pour air/humidité/trend) ----------
function parseNetatmo(json, deviceIdFromQuery) {
  const devices = json?.body?.devices || [];
  if (!devices.length) {
    return {
      air_temp_c: null, air_humidity: null, temp_trend: null,
      raining: null, rain_now_mm: null, rain_1h_mm: null,
      wind_kmh: null, gust_kmh: null, wind_dir_deg: null,
      pressure_hpa: null, pressure_trend: null,
      feels_like_c: null, feels_like_algo_used: 'none'
    };
  }

  const envDeviceId = Deno.env.get('NETATMO_DEVICE_ID');
  const dev =
    (deviceIdFromQuery && devices.find(d => d._id === deviceIdFromQuery)) ||
    (envDeviceId && devices.find(d => d._id === envDeviceId)) ||
    devices[0];

  const mods = dev.modules || [];
  const ext = mods.find(m => m?.type === 'NAModule1'); // Outdoor ONLY

  // Air/Humidité/Trend : uniquement module extérieur
  let air = null, humidity = null, temp_trend = null;
  if (ext?.dashboard_data) {
    const dd = ext.dashboard_data;
    if (typeof dd.Temperature === 'number') air = Math.round(dd.Temperature * 10) / 10; // garde 1 décimale (ex: 19.3)
    if (typeof dd.Humidity === 'number')    humidity = Math.round(dd.Humidity);
    if (typeof dd.temp_trend === 'string')  temp_trend = dd.temp_trend;
  }

  // Pluie : NAModule3
  let raining = null, rain_now_mm = null, rain_1h_mm = null;
  const rainMod = mods.find(m => m?.type === 'NAModule3');
  if (rainMod?.dashboard_data) {
    const dd = rainMod.dashboard_data;
    const nowRain  = (typeof dd.Rain === 'number') ? dd.Rain : 0;             // mm “instantané”
    const lastHour = (typeof dd.sum_rain_1 === 'number') ? dd.sum_rain_1 : 0; // mm dernière heure
    rain_now_mm = nowRain;
    rain_1h_mm  = lastHour;
    raining = (nowRain > 0) || (lastHour > 0);
  }

  // Vent : NAModule2
  let wind_kmh = null, gust_kmh = null, wind_dir_deg = null;
  const windMod = mods.find(m => m?.type === 'NAModule2');
  if (windMod?.dashboard_data) {
    const wd = windMod.dashboard_data;
    if (typeof wd.WindStrength === 'number') wind_kmh = Math.round(wd.WindStrength);
    if (typeof wd.GustStrength === 'number') gust_kmh = Math.round(wd.GustStrength);
    if (typeof wd.WindAngle === 'number')    wind_dir_deg = wd.WindAngle; // 0=N, 90=E, 180=S, 270=W
  }

  // Pression : device principal (intérieur)
  let pressure_hpa = null, pressure_trend = null;
  if (dev?.dashboard_data) {
    if (typeof dev.dashboard_data.Pressure === 'number')       pressure_hpa = Math.round(dev.dashboard_data.Pressure);
    if (typeof dev.dashboard_data.pressure_trend === 'string') pressure_trend = dev.dashboard_data.pressure_trend;
  }

  // Algo chaud Netatmo (0=humidex,1=heat-index) si présent
  let feel_like_algo_raw = null;
  if (typeof ext?.dashboard_data?.feel_like_algo === 'number') feel_like_algo_raw = ext.dashboard_data.feel_like_algo;
  else if (typeof dev?.dashboard_data?.feel_like_algo === 'number') feel_like_algo_raw = dev.dashboard_data.feel_like_algo;
  const algoWarm = (feel_like_algo_raw === 1) ? 'heat-index' : 'humidex';

  // Ressenti
  const flObj = feelsLikeC({ tempC: air, rh: humidity, windKmh: wind_kmh ?? 0, algoWarm });
  const feels_like_c = (typeof flObj.value === 'number') ? Math.round(flObj.value) : null;
  const feels_like_algo_used = flObj.algo;

  return {
    air_temp_c: air, air_humidity: humidity, temp_trend,
    raining, rain_now_mm, rain_1h_mm,
    wind_kmh, gust_kmh, wind_dir_deg,
    pressure_hpa, pressure_trend,
    feels_like_c, feels_like_algo_used
  };
}

async function getNetatmoMeteo(deviceIdFromQuery) {
  let access = await getAccessToken();
  const url = new URL('https://api.netatmo.com/api/getstationsdata');
  if (deviceIdFromQuery) url.searchParams.set('device_id', deviceIdFromQuery);

  let r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${access}` } });
  if (r.status === 401) {
    tokenCache = { access_token: null, exp: 0 }; // force refresh
    access = await getAccessToken();
    r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${access}` } });
  }
  if (!r.ok) throw new Error('netatmo_data_http_' + r.status);
  const j = await r.json();
  const parsed = parseNetatmo(j, deviceIdFromQuery);
  return { ...parsed, updated_at: nowIso() };
}

// KV + in-memory cache (dédup)
async function getNetatmoMeteoCached(deviceIdFromQuery) {
  const keyId = deviceIdFromQuery || (Deno.env.get('NETATMO_DEVICE_ID') || 'default');
  const kvKey = `meteo:${keyId}`;
  const age = Date.now() - METEO_CACHE.ts;
  if (METEO_CACHE.data && age < METEO_TTL_MS) {
    return { data: METEO_CACHE.data, meta: { source: 'mem-fresh', age_ms: age } };
  }
  if (METEO_PENDING) return METEO_PENDING;

  METEO_PENDING = (async () => {
    const res = await getWithKv(kvKey, () => getNetatmoMeteo(deviceIdFromQuery), METEO_TTL_MS, METEO_STALE_MS);
    METEO_CACHE = { data: res.data, ts: Date.now() };
    return res;
  })();

  return METEO_PENDING;
}

// ---------- Eau (Boat24) ----------
async function getWaterTempRaw() {
  const r = await fetch('https://www.bateau24.ch/chfr/service/temperaturen/lacdemorat/', { headers: HEADERS_HTML });
  if (!r.ok) throw new Error('water_http_' + r.status);
  const html = await r.text();
  const m = html.match(/(\d{1,2})\s*(?:°|&deg;|\u00B0)\s*(?:C|)/i);
  const temp = m ? parseInt(m[1], 10) : null;
  if (!temp || Number.isNaN(temp)) throw new Error('temp_not_found');
  return { temp_c: temp, updated_at: nowIso() };
}

async function getWaterTempCached() {
  const kvKey = 'water:lacdemorat';
  const age = Date.now() - WATER_CACHE.ts;
  if (WATER_CACHE.data && age < WATER_TTL_MS) {
    return { data: WATER_CACHE.data, meta: { source: 'mem-fresh', age_ms: age } };
  }
  if (WATER_PENDING) return WATER_PENDING;

  WATER_PENDING = (async () => {
    const res = await getWithKv(kvKey, () => getWaterTempRaw(), WATER_TTL_MS, WATER_TTL_MS);
    WATER_CACHE = { data: res.data, ts: Date.now() };
    return res;
  })();

  return WATER_PENDING;
}

// ---------- Router ----------
export default {
  async fetch(req) {
    const { pathname, searchParams } = new URL(req.url);

    // CORS preflight
    if (req.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          ...CORS,
          'Access-Control-Allow-Methods': 'GET,OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type,Authorization'
        }
      });
    }

    try {
      if (pathname === '/water') {
        const { data, meta } = await getWaterTempCached();
        const debug = searchParams.get('debug') === '1';
        const body = debug ? { ...data, _meta: meta } : data;
        return jres(body, 200, { 'Cache-Control': 'public, max-age=300' });
      }

      if (pathname === '/meteo') {
        const deviceId = searchParams.get('device_id') || undefined;
        const { data, meta } = await getNetatmoMeteoCached(deviceId);
        const debug = searchParams.get('debug') === '1';
        const body = debug ? { ...data, _meta: meta } : data;
        return jres(body, 200, { 'Cache-Control': 'public, max-age=60' });
      }

      if (pathname === '/info') {
        const deviceId = searchParams.get('device_id') || undefined;
        const [w, m] = await Promise.allSettled([ getWaterTempCached(), getNetatmoMeteoCached(deviceId) ]);
        const water = (w.status === 'fulfilled') ? w.value : null;
        const meteo = (m.status === 'fulfilled') ? m.value : null;

        const bodyData = {
          water_temp_c:        water?.data?.temp_c ?? null,
          air_temp_c:          meteo?.data?.air_temp_c ?? null,
          air_humidity:        meteo?.data?.air_humidity ?? null,
          temp_trend:          meteo?.data?.temp_trend ?? null,
          raining:             meteo?.data?.raining ?? null,
          rain_now_mm:         meteo?.data?.rain_now_mm ?? null,
          rain_1h_mm:          meteo?.data?.rain_1h_mm ?? null,
          wind_kmh:            meteo?.data?.wind_kmh ?? null,
          gust_kmh:            meteo?.data?.gust_kmh ?? null,
          wind_dir_deg:        meteo?.data?.wind_dir_deg ?? null,
          pressure_hpa:        meteo?.data?.pressure_hpa ?? null,
          pressure_trend:      meteo?.data?.pressure_trend ?? null,
          feels_like_c:        meteo?.data?.feels_like_c ?? null,
          feels_like_algo:     meteo?.data?.feels_like_algo_used ?? null,
          updated_at_water:    water?.data?.updated_at ?? null,
          updated_at_meteo:    meteo?.data?.updated_at ?? null
        };
        const ok = Object.values(bodyData).some(v => v !== null);

        const debug = searchParams.get('debug') === '1';
        const body = debug
          ? { ...bodyData, _meta: { water: water?.meta ?? null, meteo: meteo?.meta ?? null } }
          : bodyData;

        return jres(body, ok ? 200 : 502, { 'Cache-Control': 'public, max-age=60' });
      }

      if (pathname === '/health') {
        const env_ok = {
          client_id: !!(Deno.env.get('NETATMO_CLIENT_ID') || '').trim(),
          client_secret: !!(Deno.env.get('NETATMO_CLIENT_SECRET') || '').trim(),
          refresh_token: !!(Deno.env.get('NETATMO_REFRESH_TOKEN') || '').trim(),
          device_id: !!(Deno.env.get('NETATMO_DEVICE_ID') || '').trim()
        };
        const meteo_age_mem = METEO_CACHE.ts ? (Date.now() - METEO_CACHE.ts) : null;
        const water_age_mem = WATER_CACHE.ts ? (Date.now() - WATER_CACHE.ts) : null;
        const kvMeteo = await kvGet(['cache', `meteo:${(Deno.env.get('NETATMO_DEVICE_ID') || 'default')}`]);
        const kvWater = await kvGet(['cache', 'water:lacdemorat']);
        return jres({
          env_ok,
          mem_cache: {
            meteo: { has: !!METEO_CACHE.data, age_ms: meteo_age_mem, ttl_ms: METEO_TTL_MS, stale_ms: METEO_STALE_MS },
            water: { has: !!WATER_CACHE.data, age_ms: water_age_mem, ttl_ms: WATER_TTL_MS }
          },
          kv_cache: {
            meteo: kvMeteo ? { age_ms: Date.now() - kvMeteo.ts } : null,
            water: kvWater ? { age_ms: Date.now() - kvWater.ts } : null
          },
          now: nowIso()
        });
      }

      // Help
      return jres({
        endpoints: ['/water', '/meteo', '/info', '/health'],
        notes: [
          'Use ?debug=1 to include cache metadata',
          'Use ?device_id=XXXX on /meteo and /info to target a specific station'
        ]
      });

    } catch (e) {
      // Sur erreur réseau Netatmo, getWithKv renvoie déjà stale si possible.
      return jres({ error: 'server_error', detail: String(e?.message || e) }, 502);
    }
  }
};
