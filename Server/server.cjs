// server.cjs — Pure Node.js WebSocket room relay for Micro-Souls Co-op
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Micro-Souls Co-op WebSocket Server Running");
});

const wss = new WebSocketServer({ server });
const rooms = new Map();    // roomCode -> Set<ClientMeta>
const clients = new Map();  // clientId -> ClientMeta

function makeRoomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let code = "";
  for (let i = 0; i < 5; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

function broadcastToRoom(roomCode, payload, excludeId) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const raw = JSON.stringify(payload);
  for (const peer of room) {
    if (peer.id !== excludeId && peer.ws.readyState === 1) {
      peer.ws.send(raw);
    }
  }
}

function getHostId(roomCode) {
  const room = rooms.get(roomCode);
  if (!room || room.size === 0) return null;
  return room.values().next().value.id;
}

function syncRoomRoster(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const memberIds = Array.from(room).map((c) => c.id);
  const hostId = getHostId(roomCode);
  const raw = JSON.stringify({
    type: "room_roster",
    roomCode,
    hostId,
    players: memberIds,
  });
  for (const peer of room) {
    if (peer.ws.readyState === 1) {
      peer.ws.send(raw);
    }
  }
}

function leaveCurrentRoom(client) {
  if (!client.roomCode) return;
  const code = client.roomCode;
  const room = rooms.get(code);
  client.roomCode = null;
  if (!room) return;

  room.delete(client);
  broadcastToRoom(code, { type: "player_left", playerId: client.id });

  if (room.size === 0) {
    rooms.delete(code);
  } else {
    syncRoomRoster(code);
  }
}

function handleMessage(client, rawText) {
  let msg;
  try {
    msg = JSON.parse(rawText);
  } catch {
    return;
  }
  if (!msg || typeof msg.type !== "string") return;

  if (msg.type === "create_room") {
    leaveCurrentRoom(client);
    let code = makeRoomCode();
    while (rooms.has(code)) code = makeRoomCode();

    const set = new Set([client]);
    rooms.set(code, set);
    client.roomCode = code;

    client.ws.send(
      JSON.stringify({
        type: "room_joined",
        roomCode: code,
        playerId: client.id,
        hostId: client.id,
        players: [client.id],
      })
    );
    return;
  }

  if (msg.type === "join_room") {
    const code = String(msg.roomCode || "").trim().toUpperCase();
    const room = rooms.get(code);
    if (!room) {
      client.ws.send(JSON.stringify({ type: "error", message: "Room not found." }));
      return;
    }
    if (room.size >= 4) {
      client.ws.send(JSON.stringify({ type: "error", message: "Room is full." }));
      return;
    }

    leaveCurrentRoom(client);
    room.add(client);
    client.roomCode = code;

    client.ws.send(
      JSON.stringify({
        type: "room_joined",
        roomCode: code,
        playerId: client.id,
        hostId: getHostId(code),
        players: Array.from(room).map((c) => c.id),
      })
    );
    syncRoomRoster(code);
    return;
  }

  if (!client.roomCode) return;

  if (
    msg.type === "player_state" ||
    msg.type === "player_action" ||
    msg.type === "world_sync" ||
    msg.type === "combat_event" ||
    msg.type === "scene_change"
  ) {
    broadcastToRoom(
      client.roomCode,
      {
        ...msg,
        senderId: client.id,
      },
      client.id
    );
  }
}

wss.on("connection", (ws) => {
  const id = "p_" + Math.random().toString(36).slice(2, 8);
  const meta = { id, roomCode: null, ws };
  clients.set(id, meta);

  ws.send(JSON.stringify({ type: "welcome", playerId: id }));

  ws.on("message", (data) => {
    handleMessage(meta, data.toString());
  });

  ws.on("close", () => {
    clients.delete(meta.id);
    leaveCurrentRoom(meta);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`WebSocket server running on port ${PORT}`);
});