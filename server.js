const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();

app.use(
  cors({
    origin: [
      "https://elonixx.com",
      "https://www.elonixx.com"
    ],
    methods: ["GET", "POST"]
  })
);

app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: [
      "https://elonixx.com",
      "https://www.elonixx.com"
    ],
    methods: ["GET", "POST"]
  }
});

const messages = [];

app.get("/", (req, res) => {
  res.json({
    success: true,
    message: "Elonixx Socket.IO server is running"
  });
});

io.on("connection", (socket) => {
  console.log("User connected:", socket.id);

  socket.emit("chat-history", messages);

  socket.on("send-message", (newMsg) => {
    const message = {
      id: newMsg.id || Date.now(),
      from: newMsg.from || "client",
      text: newMsg.text || "",
      createdAt: new Date().toISOString()
    };

    messages.push(message);

    io.emit("new-message", message);
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
  });
});

const PORT = process.env.PORT || 5000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Socket.IO server running on port ${PORT}`);
});