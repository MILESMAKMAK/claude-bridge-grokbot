// Box side of the muse-bridge relay: holds a wss to the Durable Object, replays requests to the local app.
import http from 'node:http';
import fs from 'node:fs';
import WebSocket from 'ws';
const RELAY = process.env.RELAY_URL; if (!RELAY) { console.error('RELAY_URL not set (e.g. wss://<name>.<subdomain>.workers.dev/__relay/agent)'); process.exit(1); }
const SECRET = (process.env.RELAY_SECRET || fs.readFileSync(process.env.RELAY_SECRET_FILE || new URL('./relay.secret', import.meta.url), 'utf8')).trim();
const TARGET = { host: '127.0.0.1', port: Number(process.env.RELAY_TARGET_PORT || 8787) };
const log = (...a) => console.log(new Date().toISOString(), ...a);
let backoff = 1000;

function connect() {
  const ws = new WebSocket(RELAY, { headers: { authorization: 'Bearer ' + SECRET } });
  const reqs = new Map(), socks = new Map();
  const send = (o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
  let ping;
  ws.on('open', () => { log('connected'); backoff = 1000; ping = setInterval(() => send({ t: 'ping' }), 25000); });
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.t === 'pong') return;
    if (m.t === 'req') {
      const headers = {}; for (const [k, v] of m.headers) headers[k] = headers[k] ? headers[k] + ', ' + v : v;
      headers['accept-encoding'] = 'identity';
      const r = http.request({ ...TARGET, method: m.method, path: m.url, headers }, (res) => {
        const h = []; for (let i = 0; i < res.rawHeaders.length; i += 2) h.push([res.rawHeaders[i], res.rawHeaders[i + 1]]);
        send({ t: 'res', id: m.id, status: res.statusCode, headers: h, head: m.method === 'HEAD' });
        res.on('data', (c) => send({ t: 'body', id: m.id, d: c.toString('base64') }));
        res.on('end', () => { send({ t: 'end', id: m.id }); reqs.delete(m.id); });
        res.on('error', (e) => send({ t: 'err', id: m.id, msg: e.message }));
      });
      r.on('error', (e) => { send({ t: 'err', id: m.id, msg: e.message }); reqs.delete(m.id); });
      reqs.set(m.id, r);
    } else if (m.t === 'body') reqs.get(m.id)?.write(Buffer.from(m.d, 'base64'));
    else if (m.t === 'end') reqs.get(m.id)?.end();
    else if (m.t === 'abort') { reqs.get(m.id)?.destroy(); reqs.delete(m.id); }
    else if (m.t === 'wsopen') {
      const headers = {}; for (const [k, v] of m.headers) if (k.toLowerCase() !== 'sec-websocket-protocol') headers[k] = v;
      const proto = m.headers.find(([k]) => k.toLowerCase() === 'sec-websocket-protocol')?.[1];
      const up = new WebSocket(`ws://${TARGET.host}:${TARGET.port}${m.url}`, proto ? proto.split(',').map((s) => s.trim()) : [], { headers });
      const q = []; socks.set(m.id, { up, q });
      up.on('open', () => { for (const x of q) up.send(x); q.length = 0; });
      up.on('message', (d, isBin) => send({ t: 'wsmsg', id: m.id, bin: isBin, d: isBin ? d.toString('base64') : d.toString() }));
      up.on('close', (code, reason) => { send({ t: 'wsclose', id: m.id, code, reason: reason.toString() }); socks.delete(m.id); });
      up.on('error', () => { send({ t: 'wsclose', id: m.id, code: 1011, reason: 'upstream error' }); socks.delete(m.id); });
    } else if (m.t === 'wsmsg') {
      const s = socks.get(m.id); if (!s) return; const d = m.bin ? Buffer.from(m.d, 'base64') : m.d;
      s.up.readyState === 1 ? s.up.send(d) : s.q.push(d);
    } else if (m.t === 'wsclose') { const s = socks.get(m.id); if (s) { try { s.up.close(m.code || 1000, m.reason); } catch { s.up.terminate(); } socks.delete(m.id); } }
  });
  const down = (why) => {
    clearInterval(ping); for (const r of reqs.values()) r.destroy(); for (const s of socks.values()) s.up.terminate();
    reqs.clear(); socks.clear();
    log('disconnected:', why, `retry in ${backoff}ms`); setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 30000);
  };
  let done = false;
  ws.on('close', (c) => { if (!done) { done = true; down('close ' + c); } });
  ws.on('error', (e) => { log('error', e.message); if (!done) { done = true; try { ws.terminate(); } catch {} down('error'); } });
}
connect();
