// muse-bridge relay: public HTTP/WS -> Durable Object -> box agent (wss) -> 127.0.0.1:8787
const enc = new TextEncoder();
const b64e = (u8) => { let s = ""; for (let i = 0; i < u8.length; i += 0x8000) s += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000)); return btoa(s); };
const b64d = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
const HOP = new Set(["host","connection","upgrade","keep-alive","transfer-encoding","te","trailer","proxy-connection","accept-encoding","content-length","cf-connecting-ip","cf-ray","cf-visitor","cf-ipcountry","cf-worker","cdn-loop","x-forwarded-for","x-real-ip","sec-websocket-key","sec-websocket-extensions","sec-websocket-version","sec-websocket-accept"]);

export default {
  async fetch(req, env) {
    const stub = env.RELAY.get(env.RELAY.idFromName("main"));
    return stub.fetch(req);
  },
};

export class Relay {
  constructor(ctx, env) {
    this.ctx = ctx; this.env = env; this.pending = new Map(); this.seq = 0;
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"t":"ping"}', '{"t":"pong"}'));
  }
  agent() { return this.ctx.getWebSockets("agent")[0]; }
  nid() { return crypto.randomUUID(); }

  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/__relay/agent") {
      const auth = req.headers.get("authorization") || "";
      const want = "Bearer " + (this.env.RELAY_SECRET || "");
      if (!this.env.RELAY_SECRET || auth.length !== want.length || !crypto.subtle.timingSafeEqual(enc.encode(auth), enc.encode(want)))
        return new Response("forbidden", { status: 403 });
      if (req.headers.get("upgrade") !== "websocket") return new Response("need ws", { status: 426 });
      for (const old of this.ctx.getWebSockets("agent")) { try { old.close(4000, "replaced"); } catch {} }
      const [c, s] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(s, ["agent"]);
      return new Response(null, { status: 101, webSocket: c });
    }
    const ag = this.agent();
    if (!ag) return new Response("Muse Bridge relay: box agent not connected", { status: 502 });

    const headers = [];
    for (const [k, v] of req.headers) if (!HOP.has(k.toLowerCase())) headers.push([k, v]);
    headers.push(["x-forwarded-host", url.host], ["x-forwarded-proto", "https"]);
    const ip = req.headers.get("cf-connecting-ip"); if (ip) headers.push(["x-forwarded-for", ip]);
    const id = this.nid();

    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const proto = req.headers.get("sec-websocket-protocol");
      const [c, s] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(s, ["c:" + id]);
      ag.send(JSON.stringify({ t: "wsopen", id, url: url.pathname + url.search, headers }));
      const h = proto ? { "Sec-WebSocket-Protocol": proto.split(",")[0].trim() } : {};
      return new Response(null, { status: 101, webSocket: c, headers: h });
    }

    let resolve; const p = new Promise((r) => (resolve = r));
    this.pending.set(id, { resolve, writer: null });
    ag.send(JSON.stringify({ t: "req", id, method: req.method, url: url.pathname + url.search, headers }));
    (async () => {
      try {
        if (req.body) { const rd = req.body.getReader(); for (;;) { const { done, value } = await rd.read(); if (done) break; ag.send(JSON.stringify({ t: "body", id, d: b64e(value) })); } }
        ag.send(JSON.stringify({ t: "end", id }));
      } catch (e) { try { ag.send(JSON.stringify({ t: "abort", id })); } catch {} }
    })();
    const timer = setTimeout(() => { const e = this.pending.get(id); if (e && !e.writer) { this.pending.delete(id); e.resolve(new Response("relay timeout", { status: 504 })); } }, 100000);
    const res = await p; clearTimeout(timer); return res;
  }

  webSocketMessage(ws, msg) {
    const tags = this.ctx.getTags(ws);
    if (tags.includes("agent")) return this.fromAgent(msg);
    const tag = tags.find((t) => t.startsWith("c:")); if (!tag) return;
    const ag = this.agent(); if (!ag) { ws.close(1011, "agent gone"); return; }
    const bin = typeof msg !== "string";
    ag.send(JSON.stringify({ t: "wsmsg", id: tag.slice(2), bin, d: bin ? b64e(new Uint8Array(msg)) : msg }));
  }
  webSocketClose(ws, code, reason) { this.closed(ws, code, reason); }
  webSocketError(ws) { this.closed(ws, 1011, "error"); }
  closed(ws, code, reason) {
    const tags = this.ctx.getTags(ws);
    if (tags.includes("agent")) {
      for (const [id, e] of this.pending) { if (e.writer) e.writer.abort("agent gone").catch(() => {}); else e.resolve(new Response("agent disconnected", { status: 502 })); }
      this.pending.clear();
      for (const c of this.ctx.getWebSockets()) if (!this.ctx.getTags(c).includes("agent")) { try { c.close(1012, "agent gone"); } catch {} }
      return;
    }
    const tag = tags.find((t) => t.startsWith("c:"));
    try { ws.close(code === 1005 || code === 1006 ? 1000 : code, reason); } catch {}
    const ag = this.agent();
    if (tag && ag) ag.send(JSON.stringify({ t: "wsclose", id: tag.slice(2), code: code === 1005 ? 1000 : code, reason }));
  }

  fromAgent(raw) {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t?.startsWith("ws")) {
      const c = this.ctx.getWebSockets("c:" + m.id)[0]; if (!c) return;
      if (m.t === "wsmsg") c.send(m.bin ? b64d(m.d) : m.d);
      else if (m.t === "wsclose") { try { c.close(m.code && m.code >= 1000 && m.code < 5000 && m.code !== 1005 && m.code !== 1006 ? m.code : 1000, m.reason || ""); } catch {} }
      return;
    }
    const e = this.pending.get(m.id); if (!e) return;
    if (m.t === "res") {
      const h = new Headers(); for (const [k, v] of m.headers) if (!HOP.has(k.toLowerCase())) h.append(k, v);
      const nobody = m.status === 204 || m.status === 304 || m.head;
      if (nobody) { this.pending.delete(m.id); e.resolve(new Response(null, { status: m.status, headers: h })); return; }
      const { readable, writable } = new TransformStream();
      e.writer = writable.getWriter();
      e.resolve(new Response(readable, { status: m.status, headers: h }));
    } else if (m.t === "body" && e.writer) e.writer.write(b64d(m.d)).catch(() => {});
    else if (m.t === "end") { this.pending.delete(m.id); if (e.writer) e.writer.close().catch(() => {}); }
    else if (m.t === "err") { this.pending.delete(m.id); if (e.writer) e.writer.abort(m.msg).catch(() => {}); else e.resolve(new Response("upstream error: " + m.msg, { status: 502 })); }
  }
}
