/* Minimal, dependency-free WebSocket server (RFC 6455 subset).
 * Handles exactly what this relay needs: the opening handshake, text
 * frames in and out, basic fragmentation reassembly, close/ping/pong
 * control frames, and a server->client heartbeat ping so idle lobby
 * connections don't get silently dropped by an intermediary proxy.
 * No external packages -- this repo's npm registry access is
 * unavailable in the dev sandbox, and skipping the dependency also means
 * the deployed service has nothing to `npm install` before it can start.
 */
const crypto = require('crypto');
const { EventEmitter } = require('events');

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const PING_MS = 20000;

function acceptKey(key) { return crypto.createHash('sha1').update(key + GUID).digest('base64'); }

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  header[0] = 0x80 | opcode; // FIN=1
  return Buffer.concat([header, payload]);
}

class WSConn extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this._buf = Buffer.alloc(0);
    this._fragOpcode = null; this._fragChunks = [];
    this.readyState = 1; // OPEN
    socket.on('data', d => this._onData(d));
    socket.on('close', () => { this.readyState = 3; this.emit('close'); });
    socket.on('error', err => { this.emit('error', err); });
    this._pingT = setInterval(() => { if (this.readyState === 1) this._writeFrame(0x9, Buffer.alloc(0)); }, PING_MS);
  }
  _onData(chunk) {
    this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;
    for (;;) {
      if (this._buf.length < 2) return;
      const b0 = this._buf[0], b1 = this._buf[1];
      const fin = (b0 & 0x80) !== 0, opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this._buf.length < 4) return; len = this._buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this._buf.length < 10) return; len = Number(this._buf.readBigUInt64BE(2)); off = 10; }
      let maskKey = null;
      if (masked) { if (this._buf.length < off + 4) return; maskKey = this._buf.slice(off, off + 4); off += 4; }
      if (this._buf.length < off + len) return; // wait for more data
      let payload = this._buf.slice(off, off + len);
      if (masked) { const p = Buffer.from(payload); for (let i = 0; i < p.length; i++) p[i] ^= maskKey[i % 4]; payload = p; }
      this._buf = this._buf.slice(off + len);
      this._handleFrame(fin, opcode, payload);
    }
  }
  _handleFrame(fin, opcode, payload) {
    if (opcode === 0x8) { this.close(); return; }              // close
    if (opcode === 0x9) { this._writeFrame(0xA, payload); return; } // ping -> pong
    if (opcode === 0xA) return;                                  // pong: ignore
    if (opcode === 0x1 || opcode === 0x2) { this._fragOpcode = opcode; this._fragChunks = [payload]; }
    else if (opcode === 0x0 && this._fragOpcode !== null) { this._fragChunks.push(payload); }
    else return;
    if (fin) {
      const full = Buffer.concat(this._fragChunks);
      this._fragOpcode = null; this._fragChunks = [];
      this.emit('message', full);
    }
  }
  _writeFrame(opcode, payload) { try { if (this.readyState === 1) this.socket.write(encodeFrame(opcode, payload)); } catch (e) {} }
  send(str) { this._writeFrame(0x1, Buffer.from(str, 'utf8')); }
  close() {
    if (this.readyState !== 1) return;
    this.readyState = 3;
    clearInterval(this._pingT);
    try { this._writeFrame(0x8, Buffer.alloc(0)); } catch (e) {}
    try { this.socket.end(); } catch (e) {}
    this.emit('close');
  }
}

class WebSocketServer extends EventEmitter {
  constructor({ server }) {
    super();
    server.on('upgrade', (req, socket, head) => {
      const upgradeHdr = (req.headers['upgrade'] || '').toLowerCase();
      const key = req.headers['sec-websocket-key'];
      if (upgradeHdr !== 'websocket' || !key) { socket.destroy(); return; }
      const accept = acceptKey(key);
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
      );
      const ws = new WSConn(socket);
      if (head && head.length) ws._onData(head);
      this.emit('connection', ws, req);
    });
  }
}

module.exports = { WebSocketServer };
