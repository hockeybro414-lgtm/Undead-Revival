# Coldstore Breach — public relay server

A tiny, dependency-free Node WebSocket relay that lets the public build of
Coldstore Breach do real 4-player co-op without a Claude account. It just
groups players into rooms by a short code and relays presence/game events —
see `relay.js` for the full (short) implementation.

## Deploy

1. Push this folder to a new GitHub (or GitLab/Bitbucket) repo.
2. In Render: New → Web Service → point it at that repo.
   - Runtime: Node
   - Build command: (none needed)
   - Start command: `node relay.js`
   - Plan: Free is fine to start
3. Render will give you a URL like `https://coldstore-relay.onrender.com`.
   The game connects over WebSocket, so the client uses `wss://coldstore-relay.onrender.com`.

No environment variables or build step are required — there are zero
external dependencies (see `wsmini.js`), so nothing has to `npm install`
before the service can start.
