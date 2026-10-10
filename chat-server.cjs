/* Secure Socket.IO customer-support service.
   Use the same MONGODB_URI and JWT_SECRET as the main API. */
require("dotenv").config();
const http = require("http");
const express = require("express");
const mongoose = require("mongoose");
const jwt = require("jsonwebtoken");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET;
const ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const allowedOrigins = String(process.env.FRONTEND_ORIGINS || "https://elonixx.com,https://www.elonixx.com")
  .split(",").map((origin) => origin.trim()).filter(Boolean);

if (!JWT_SECRET) throw new Error("JWT_SECRET must be configured.");
if (!process.env.MONGODB_URI) throw new Error("MONGODB_URI must be configured.");
if (!ADMIN_EMAIL) throw new Error("ADMIN_EMAIL must be configured.");

app.use(express.json());
app.get("/", (_req, res) => res.json({ success: true, message: "Support chat is online." }));

const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
  },
  transports: ["websocket"],
});

let dbPromise;
function connectDB() {
  if (mongoose.connection.readyState === 1) return Promise.resolve();
  if (!dbPromise) {
    dbPromise = mongoose.connect(process.env.MONGODB_URI, {
      serverSelectionTimeoutMS: 10000,
    }).catch((error) => {
      dbPromise = null;
      throw error;
    });
  }
  return dbPromise;
}

const User = mongoose.models.User || mongoose.model(
  "User",
  new mongoose.Schema({ name: String, email: String }, { collection: "users" })
);

const messageSchema = new mongoose.Schema({
  id: { type: String, required: true },
  senderRole: { type: String, enum: ["client", "admin"], required: true },
  senderName: { type: String, required: true },
  text: { type: String, required: true, maxlength: 2000 },
  createdAt: { type: Date, default: Date.now },
}, { _id: false });

const conversationSchema = new mongoose.Schema({
  userId: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true, unique: true },
  userName: { type: String, required: true },
  messages: { type: [messageSchema], default: [] },
}, { timestamps: true, collection: "supportconversations" });

const Conversation = mongoose.models.SupportConversation || mongoose.model("SupportConversation", conversationSchema);

const roomFor = (userId) => `support:${userId}`;
const isObjectId = (value) => mongoose.Types.ObjectId.isValid(String(value || ""));
const summarize = (conversation) => {
  const last = conversation.messages[conversation.messages.length - 1];
  return {
    userId: String(conversation.userId),
    userName: conversation.userName,
    lastMessage: last?.text?.slice(0, 160) || "Conversation opened",
    updatedAt: conversation.updatedAt,
  };
};

io.use(async (socket, next) => {
  try {
    await connectDB();
    const token = socket.handshake.auth?.token;
    if (!token) return next(new Error("Sign in is required for support chat."));
    const decoded = jwt.verify(token, JWT_SECRET);

    if (decoded.role === "admin") {
      const email = String(decoded.adminEmail || "").trim().toLowerCase();
      if (!email || email !== ADMIN_EMAIL) return next(new Error("Admin access denied."));
      socket.data.actor = { role: "admin", name: "Support team" };
      return next();
    }

    if (decoded.role !== "user" || !isObjectId(decoded.userId)) {
      return next(new Error("A valid client account is required."));
    }
    const user = await User.findById(decoded.userId).select("name").lean();
    if (!user) return next(new Error("Client account not found."));
    socket.data.actor = {
      role: "client",
      userId: String(decoded.userId),
      name: String(user.name || "Client").slice(0, 100),
    };
    return next();
  } catch (error) {
    return next(new Error(error.name === "TokenExpiredError" ? "Session expired. Sign in again." : "Chat authentication failed."));
  }
});

function ackError(ack, message) {
  if (typeof ack === "function") ack({ ok: false, error: message });
}

async function emitThreadList(socket) {
  const conversations = await Conversation.find({})
    .sort({ updatedAt: -1 })
    .limit(200)
    .lean();
  socket.emit("support:threads", conversations.map(summarize));
}

io.on("connection", async (socket) => {
  const actor = socket.data.actor;

  if (actor.role === "admin") {
    socket.join("support:admins");
    try { await emitThreadList(socket); }
    catch (error) { console.error("Support thread list error:", error.message); }
  }

  /* A client explicitly starts or resumes only their own support thread. */
  socket.on("support:open", async (ack) => {
    if (actor.role !== "client") return ackError(ack, "Only a signed-in client can start a conversation.");
    try {
      let conversation = await Conversation.findOne({ userId: actor.userId });
      if (!conversation) {
        try {
          conversation = await Conversation.create({ userId: actor.userId, userName: actor.name, messages: [] });
        } catch (error) {
          if (error.code !== 11000) throw error;
          conversation = await Conversation.findOne({ userId: actor.userId });
        }
      }
      if (!conversation) throw new Error("Conversation could not be opened.");
      socket.join(roomFor(actor.userId));
      const result = { ok: true, userId: actor.userId, messages: conversation.messages };
      if (typeof ack === "function") ack(result);
      socket.emit("support:history", result);
      io.to("support:admins").emit("support:thread-updated", summarize(conversation));
    } catch (error) {
      console.error("Support open error:", error.message);
      ackError(ack, "Could not open the support conversation. Please try again.");
    }
  });

  /* Admins can join only a conversation that a client has already created. */
  socket.on("support:join", async (payload, ack) => {
    if (actor.role !== "admin") return ackError(ack, "Admin access required.");
    const userId = String(payload?.userId || "");
    if (!isObjectId(userId)) return ackError(ack, "Invalid client conversation.");
    try {
      const conversation = await Conversation.findOne({ userId });
      if (!conversation) return ackError(ack, "That client has not started a support conversation.");
      socket.join(roomFor(userId));
      if (typeof ack === "function") ack({ ok: true, userId, messages: conversation.messages });
    } catch (error) {
      console.error("Support join error:", error.message);
      ackError(ack, "Could not load this conversation.");
    }
  });

  socket.on("support:send", async (payload, ack) => {
    const text = String(payload?.text || "").trim();
    if (!text) return ackError(ack, "Type a message before sending.");
    if (text.length > 2000) return ackError(ack, "Messages must be 2,000 characters or fewer.");

    let userId;
    let senderRole;
    let senderName;

    if (actor.role === "client") {
      userId = actor.userId;
      senderRole = "client";
      senderName = actor.name;
    } else {
      userId = String(payload?.userId || "");
      if (!isObjectId(userId)) return ackError(ack, "Select a client-started conversation first.");
      if (!socket.rooms.has(roomFor(userId))) {
        return ackError(ack, "Join this client-started conversation before replying.");
      }
      senderRole = "admin";
      senderName = actor.name;

      /* Prevent admin replies that ask for credentials or payment to release funds. */
      const unsafeRequest = /\b(one[- ]time (?:code|passcode)|verification code|security code|password|passcode|\bpin\b|cvv|card number|processing fee|release fee|unlock fee|payment to release|pay to release)\b/i;
      if (unsafeRequest.test(text)) {
        return ackError(ack, "This message appears to request sensitive credentials or a release payment and was blocked.");
      }
    }

    try {
      const conversation = await Conversation.findOne({ userId });
      if (!conversation) {
        return ackError(ack, senderRole === "admin"
          ? "This client has not started a support conversation."
          : "Open the support chat before sending a message.");
      }

      const message = {
        id: new mongoose.Types.ObjectId().toString(),
        senderRole,
        senderName,
        text,
        createdAt: new Date(),
      };
      conversation.messages.push(message);
      if (conversation.messages.length > 300) conversation.messages.splice(0, conversation.messages.length - 300);
      conversation.updatedAt = new Date();
      await conversation.save();

      const outgoing = { ...message, userId };
      io.to(roomFor(userId)).emit("support:message", outgoing);
      io.to("support:admins").emit("support:thread-updated", summarize(conversation));
      if (typeof ack === "function") ack({ ok: true, message: outgoing });
    } catch (error) {
      console.error("Support send error:", error.message);
      ackError(ack, "Message could not be saved. Please try again.");
    }
  });

  socket.on("disconnect", () => {
    // Socket.IO removes the socket from its rooms automatically.
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Support chat listening on port ${PORT}`);
});
