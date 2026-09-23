/* Coldstore Breach — public multiplayer relay server.
 *
 * A tiny room-based WebSocket relay: it does not understand the game at
 * all, it just (a) groups sockets into rooms by a short code, (b) keeps a
 * per-peer "presence" object that a peer can patch and everyone in the
 * room gets broadcast at a fixed rate, and (c) relays arbitrary
 * topic+data "emit" messages to every socket in the room (including the
 * sender, tagged isMe so the sender's own game code can tell). Almost
 * every gameplay decision (who's host, whether a hit lands, round state)
 * is computed client-side from that stream, exactly like the original
 * Claude-artifact `room` capability this replaces — so the client only
 * had to swap out its networking object, not its game logic.
 */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('./wsmini');

const PORT = process.env.PORT || 8787;
const MAX_ROOM_SIZE = 4;
const BROADCAST_HZ = 20;
const MAX_MSGS_PER_SEC = 120;
const MAX_MSG_BYTES = 8192;
const ROOM_IDLE_MS = 6 * 60 * 60 * 1000; // reap empty-room bookkeeping if it somehow lingers

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('coldstore relay ok\n'); return; }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server });

/** @type {Map<string, { peers: Map<string, {ws: import('ws').WebSocket, presence: object, lastSeen: number}>, dirty: boolean, timer: NodeJS.Timeout }>} */
const rooms = new Map();

const genPeerId = () => crypto.randomBytes(6).toString('hex');
const now = () => Date.now();

function getRoom(code) {
  let r = rooms.get(code);
  if (!r) {
    r = { peers: new Map(), dirty: false, timer: null };
    r.timer = setInterval(() => broadcastPeers(code), 1000 / BROADCAST_HZ);
    rooms.set(code, r);
  }
  return r;
}
function closeRoomIfEmpty(code) {
  const r = rooms.get(code);
  if (r && r.peers.size === 0) { clearInterval(r.timer); rooms.delete(code); }
}
function broadcastPeers(code) {
  const r = rooms.get(code);
  if (!r || !r.dirty) return;
  r.dirty = false;
  const list = [];
  for (const [peer, p] of r.peers) list.push({ peer, presence: p.presence, updatedAt: p.lastSeen });
  const msg = JSON.stringify({ type: 'peers', list });
  for (const [, p] of r.peers) if (p.ws.readyState === 1) p.ws.send(msg);
}
function relayEmit(code, fromPeer, topic, data) {
  const r = rooms.get(code); if (!r) return;
  const msg = peer => JSON.stringify({ type: 'emit', topic, data, peer: fromPeer, isMe: peer === fromPeer });
  for (const [peer, p] of r.peers) if (p.ws.readyState === 1) p.ws.send(msg(peer));
}

wss.on('connection', ws => {
  let code = null, peerId = null;
  ws._msgTimes = [];

  ws.on('message', raw => {
    if (raw.length > MAX_MSG_BYTES) return;
    const t = now();
    ws._msgTimes = ws._msgTimes.filter(x => t - x < 1000);
    ws._msgTimes.push(t);
    if (ws._msgTimes.length > MAX_MSGS_PER_SEC) return;

    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type === 'join') {
      if (code) return; // already joined this socket
      code = String(msg.room || 'LOBBY').slice(0, 24).toUpperCase() || 'LOBBY';
      const r = getRoom(code);
      if (r.peers.size >= MAX_ROOM_SIZE) { try { ws.send(JSON.stringify({ type: 'full' })); } catch (e) {} ws.close(); return; }
      peerId = genPeerId();
      r.peers.set(peerId, { ws, presence: {}, lastSeen: now() });
      r.dirty = true;
      try { ws.send(JSON.stringify({ type: 'joined', peer: peerId, room: code })); } catch (e) {}
      return;
    }
    if (!code || !peerId) return;
    const r = rooms.get(code); if (!r) return;
    const me = r.peers.get(peerId); if (!me) return;

    if (msg.type === 'presence') {
      const patch = msg.patch && typeof msg.patch === 'object' ? msg.patch : {};
      for (const k in patch) { if (patch[k] === null) delete me.presence[k]; else me.presence[k] = patch[k]; }
      me.lastSeen = now(); r.dirty = true;
      return;
    }
    if (msg.type === 'emit') {
      if (typeof msg.topic === 'string') relayEmit(code, peerId, msg.topic, msg.data);
      return;
    }
  });

  ws.on('close', () => {
    if (!code || !peerId) return;
    const r = rooms.get(code);
    if (r) { r.peers.delete(peerId); r.dirty = true; broadcastPeers(code); closeRoomIfEmpty(code); }
  });
  ws.on('error', () => {});
});

server.listen(PORT, () => console.log('coldstore relay listening on :' + PORT));
