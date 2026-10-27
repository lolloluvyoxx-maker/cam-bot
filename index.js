import ffmpegPath from "ffmpeg-static";
process.env.FFMPEG_PATH = ffmpegPath;
// la libreria cerca ffmpeg nel PATH
process.env.PATH = `${ffmpegPath.replace(/[\\/][^\\/]*$/, "")}:${process.env.PATH}`;

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { spawnSync } from "node:child_process";
import dgram from "node:dgram";
import { Client } from "@lng2004/discord.js-selfbot-v13";
import { Streamer, prepareStream, playStream, Utils, Encoders } from "@dank074/discord-video-stream";

// --- Config (variabili Railway) ---
const TOKEN = process.env.TOKEN;
const GUILD_ID = process.env.GUILD_ID;
const CHANNEL_ID = process.env.CHANNEL_ID; // canale vocale

// Canale testuale da cui leggere i video (allegati)
const MEDIA_CHANNEL_ID = process.env.MEDIA_CHANNEL_ID || "";
const MEDIA_LIMIT = Number(process.env.MEDIA_LIMIT || 10); // quanti video recenti tenere
const MAX_MEDIA_MB = Number(process.env.MAX_MEDIA_MB || 150);
// Fallback opzionale: link/percorsi separati da virgola
const VIDEOS = (process.env.VIDEOS || "").split(",").map((v) => v.trim()).filter(Boolean);

// Impostazioni stream (modificabili senza toccare il codice)
const JOIN_TIMEOUT_SEC = Number(process.env.JOIN_TIMEOUT_SEC || 45);
const STREAM_TYPE = (process.env.STREAM_TYPE || "camera").toLowerCase(); // camera | go-live
const VIDEO_CODEC = (process.env.VIDEO_CODEC || "H264").toUpperCase(); // H264 | H265
const STREAM_WIDTH = Number(process.env.STREAM_WIDTH || 1280);
const STREAM_HEIGHT = Number(process.env.STREAM_HEIGHT || 720);
const STREAM_FPS = Number(process.env.STREAM_FPS || 30);
const STREAM_BITRATE = Number(process.env.STREAM_BITRATE || 2500); // kbps

const STATUSES = (process.env.STATUSES || "chill").split("|").map((s) => s.trim()).filter(Boolean);
const STATUS_TYPE = process.env.STATUS_TYPE || "PLAYING";
const STATUS_INTERVAL_MIN = Number(process.env.STATUS_INTERVAL_MIN || 15);

// Turni
const SESSIONS_PER_DAY = Number(process.env.SESSIONS_PER_DAY || 3);
const SESSION_MINUTES = Number(process.env.SESSION_MINUTES || 90);
const MIN_GAP_MINUTES = Number(process.env.MIN_GAP_MINUTES || 30);
const TIMEZONE = process.env.TIMEZONE || "Europe/Rome";
const SCHEDULE_SEED = process.env.SCHEDULE_SEED || "seed";
const START_ON_DEPLOY = (process.env.START_ON_DEPLOY || "true").toLowerCase() !== "false";

if (!TOKEN || !GUILD_ID || !CHANNEL_ID) {
  console.error("Mancano TOKEN, GUILD_ID o CHANNEL_ID");
  process.exit(1);
}
if (!MEDIA_CHANNEL_ID && VIDEOS.length === 0) {
  console.error("Imposta MEDIA_CHANNEL_ID (canale con i video) oppure VIDEOS");
  process.exit(1);
}

const MIN = 60_000;
const DAY = 24 * 60 * MIN;
if (SESSIONS_PER_DAY * SESSION_MINUTES + (SESSIONS_PER_DAY - 1) * MIN_GAP_MINUTES > 24 * 60) {
  console.error("Turni troppo lunghi o troppi per stare in un giorno");
  process.exit(1);
}

const client = new Client();
const streamer = new Streamer(client);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (min, max) => Math.floor(min + Math.random() * (max - min));
const withTimeout = (p, ms, label) =>
  Promise.race([
    p,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`${label}: timeout dopo ${ms / 1000}s`)), ms)),
  ]);

let stopping = false;

// BEGIN-SCHED
function tzOffset(ts, tz) {
  const p = new Intl.DateTimeFormat("en-US", {
    timeZone: tz, hourCycle: "h23",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).formatToParts(new Date(ts));
  const o = {};
  for (const x of p) o[x.type] = x.value;
  const asUTC = Date.UTC(+o.year, +o.month - 1, +o.day, +o.hour, +o.minute, +o.second);
  return asUTC - (ts - (ts % 1000));
}
function dayKeyOf(ts, tz) {
  return new Date(ts + tzOffset(ts, tz)).toISOString().slice(0, 10);
}
function localMidnight(key, tz) {
  const [y, m, d] = key.split("-").map(Number);
  const guess = Date.UTC(y, m - 1, d);
  return guess - tzOffset(guess, tz);
}
function hashStr(s) {
  let h = 1779033703 ^ s.length;
  for (let i = 0; i < s.length; i++) {
    h = Math.imul(h ^ s.charCodeAt(i), 3432918353);
    h = (h << 13) | (h >>> 19);
  }
  return h >>> 0;
}
function rng(seed) { // mulberry32
  let a = seed;
  return () => {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
function scheduleForDay(key, n, durMin, gapMin, tz, seed) {
  const start = localMidnight(key, tz);
  const rand = rng(hashStr(`${seed}:${key}`));
  const dur = durMin * MIN, gap = gapMin * MIN;
  const free = DAY - n * dur - (n - 1) * gap;
  const offs = Array.from({ length: n }, () => Math.floor(rand() * free)).sort((a, b) => a - b);
  return offs.map((o, i) => {
    const s = start + o + i * (dur + gap);
    return { start: s, end: s + dur };
  });
}
function nextSession(now, mustStartAfter = 0) {
  for (let d = 0; d < 3; d++) {
    const key = dayKeyOf(now + d * DAY, TIMEZONE);
    const list = scheduleForDay(key, SESSIONS_PER_DAY, SESSION_MINUTES, MIN_GAP_MINUTES, TIMEZONE, SCHEDULE_SEED);
    const s = list.find((x) => x.end > now + 2 * MIN && x.start >= mustStartAfter);
    if (s) return s;
  }
  return null;
}
// END-SCHED

const fmt = (ts) => new Date(ts).toLocaleString("it-IT", { timeZone: TIMEZONE });

// BEGIN-DL
const MEDIA_DIR = path.join(os.tmpdir(), "media");
fs.mkdirSync(MEDIA_DIR, { recursive: true });

const isVideo = (att) =>
  (att.contentType || "").startsWith("video/") || /\.(mp4|mov|webm|mkv|m4v)$/i.test(att.name || "");

async function downloadTo(url, dest, maxBytes) {
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const len = Number(res.headers.get("content-length") || 0);
  if (len && len > maxBytes) throw new Error(`file troppo grande (${(len / 1048576).toFixed(0)} MB)`);
  const tmp = dest + ".part";
  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmp));
  fs.renameSync(tmp, dest);
}
// END-DL

// --- Playlist dagli allegati del canale ---
let playlist = []; // { id, msgId, name, url, file }
let mediaIndex = 0;
let currentFile = null;

function itemsFromMessage(msg) {
  const out = [];
  for (const att of msg.attachments?.values?.() ?? []) {
    if (!isVideo(att)) continue;
    const ext = (path.extname(att.name || "") || ".mp4").toLowerCase();
    out.push({
      id: att.id,
      msgId: msg.id,
      name: att.name,
      url: att.url,
      file: path.join(MEDIA_DIR, `${att.id}${ext}`),
    });
  }
  return out;
}

function trimPlaylist() {
  while (playlist.length > MEDIA_LIMIT) {
    const old = playlist.shift();
    if (old.file !== currentFile) fs.rm(old.file, { force: true }, () => {});
  }
}

async function loadPlaylist() {
  if (!MEDIA_CHANNEL_ID) return;
  const ch = await client.channels.fetch(MEDIA_CHANNEL_ID);
  if (!ch?.messages) throw new Error("MEDIA_CHANNEL_ID non è un canale testuale leggibile");
  const msgs = [...(await ch.messages.fetch({ limit: 100 })).values()].sort(
    (a, b) => a.createdTimestamp - b.createdTimestamp
  );
  playlist = msgs.flatMap(itemsFromMessage);
  trimPlaylist();
  console.log(`Playlist: ${playlist.length} video dal canale → ${playlist.map((p) => p.name).join(", ") || "(nessuno)"}`);
}

async function ensureFile(item) {
  if (fs.existsSync(item.file) && fs.statSync(item.file).size > 0) return item.file;
  const maxBytes = MAX_MEDIA_MB * 1048576;
  console.log(`Scarico ${item.name}...`);
  try {
    await downloadTo(item.url, item.file, maxBytes);
  } catch (e) {
    // link scaduto: rileggo il messaggio per averne uno fresco
    console.warn(`Download fallito (${e.message}), provo con un link aggiornato`);
    const ch = await client.channels.fetch(MEDIA_CHANNEL_ID);
    const msg = await ch.messages.fetch(item.msgId);
    const fresh = itemsFromMessage(msg).find((x) => x.id === item.id);
    if (!fresh) throw new Error("allegato non più disponibile");
    item.url = fresh.url;
    await downloadTo(item.url, item.file, maxBytes);
  }
  console.log(`Scaricato ${item.name} (${(fs.statSync(item.file).size / 1048576).toFixed(1)} MB)`);
  return item.file;
}

let fallbackIndex = 0;
async function nextSource() {
  if (playlist.length > 0) {
    const item = playlist[mediaIndex++ % playlist.length];
    try {
      const file = await ensureFile(item);
      currentFile = file;
      return { src: file, label: item.name };
    } catch (e) {
      console.error(`Salto ${item.name}: ${e.message}`);
      playlist = playlist.filter((p) => p !== item);
      throw e;
    }
  }
  if (VIDEOS.length > 0) {
    const src = VIDEOS[fallbackIndex++ % VIDEOS.length];
    return { src, label: src };
  }
  return null;
}

client.on("messageCreate", (msg) => {
  if (!MEDIA_CHANNEL_ID || msg.channelId !== MEDIA_CHANNEL_ID) return;
  const items = itemsFromMessage(msg);
  if (items.length === 0) return;
  playlist.push(...items);
  trimPlaylist();
  console.log(`Nuovo video nel canale: ${items.map((i) => i.name).join(", ")} (playlist: ${playlist.length})`);
});
client.on("messageDelete", (msg) => {
  if (!MEDIA_CHANNEL_ID || msg.channelId !== MEDIA_CHANNEL_ID) return;
  const gone = playlist.filter((p) => p.msgId === msg.id);
  if (gone.length === 0) return;
  playlist = playlist.filter((p) => p.msgId !== msg.id);
  console.log(`Video rimosso dal canale: ${gone.map((g) => g.name).join(", ")}`);
});

// --- Controllo preliminare della sorgente video (diagnostica) ---
const checked = new Set();
function preflight(src) {
  if (checked.has(src)) return;
  checked.add(src);
  try {
    if (checked.size === 1) {
      const v = spawnSync(ffmpegPath, ["-version"], { encoding: "utf8", timeout: 10000 });
      console.log("[check] ffmpeg path:", ffmpegPath, "|", (v.stdout || "").split("\n")[0] || String(v.error || v.stderr));
    }
    if (!/^https?:\/\//i.test(src)) {
      if (!fs.existsSync(src)) {
        console.error(`[check] file NON trovato: ${src} | cwd=${process.cwd()} | file qui: ${fs.readdirSync(".").join(", ")}`);
        return;
      }
      const st = fs.statSync(src);
      const fd = fs.openSync(src, "r");
      const buf = Buffer.alloc(16);
      fs.readSync(fd, buf, 0, 16, 0);
      fs.closeSync(fd);
      console.log(`[check] ${src}: ${(st.size / 1048576).toFixed(2)} MB | header hex=${buf.toString("hex")} | testo=${JSON.stringify(buf.toString("latin1"))}`);
    }
    const r = spawnSync(ffmpegPath, ["-hide_banner", "-i", src], { encoding: "utf8", timeout: 20000 });
    const out = ((r.stderr || "") + (r.stdout || "")).split("\n").filter(Boolean).slice(0, 14).join(" | ");
    console.log("[check] ffmpeg legge:", out.slice(0, 900), r.error ? String(r.error) : "");
  } catch (e) {
    console.error("[check] errore:", e?.message);
  }
}

// --- Status rotator ---
let statusIndex = 0;
function rotateStatus() {
  if (!client.user || STATUSES.length === 0) return;
  try {
    client.user.setPresence({
      status: "online",
      activities: [{ name: STATUSES[statusIndex % STATUSES.length], type: STATUS_TYPE }],
    });
    statusIndex++;
  } catch (e) {
    console.error("Status:", e?.message);
  }
}
function scheduleStatus() {
  rotateStatus();
  const base = Math.max(5, STATUS_INTERVAL_MIN) * MIN;
  setTimeout(scheduleStatus, base + jitter(0, 2 * MIN));
}

// --- Test UDP: Discord voice richiede UDP in uscita E in risposta ---
function udpCheck(host = "stun.l.google.com", port = 19302, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const sock = dgram.createSocket("udp4");
    const tid = Buffer.from(Array.from({ length: 12 }, () => Math.floor(Math.random() * 256)));
    const req = Buffer.alloc(20);
    req.writeUInt16BE(0x0001, 0); // Binding Request
    req.writeUInt16BE(0, 2);
    req.writeUInt32BE(0x2112a442, 4);
    tid.copy(req, 8);
    const done = (r) => { try { sock.close(); } catch {} resolve(r); };
    const timer = setTimeout(() => done({ ok: false, reason: "nessuna risposta (UDP bloccato o filtrato)" }), timeoutMs);
    sock.on("error", (e) => { clearTimeout(timer); done({ ok: false, reason: e.message }); });
    sock.on("message", (msg) => {
      clearTimeout(timer);
      let ip = "?";
      // cerca XOR-MAPPED-ADDRESS (0x0020)
      for (let o = 20; o + 4 <= msg.length; ) {
        const type = msg.readUInt16BE(o), len = msg.readUInt16BE(o + 2);
        if (type === 0x0020 && len >= 8) {
          const m = msg.readUInt32BE(4);
          const a = msg.readUInt32BE(o + 8) ^ m;
          ip = [a >>> 24, (a >>> 16) & 255, (a >>> 8) & 255, a & 255].join(".");
        }
        o += 4 + len + ((4 - (len % 4)) % 4);
      }
      done({ ok: true, ip });
    });
    sock.send(req, port, host, (err) => { if (err) { clearTimeout(timer); done({ ok: false, reason: err.message }); } });
  });
}

async function joinVC() {
  for (let a = 1; ; a++) {
    try {
      console.log(`joinVoice (tentativo ${a}) guild=${GUILD_ID} channel=${CHANNEL_ID}`);
      await withTimeout(streamer.joinVoice(GUILD_ID, CHANNEL_ID), JOIN_TIMEOUT_SEC * 1000, "joinVoice");
      return;
    } catch (e) {
      console.error(`Join VC fallito (tentativo ${a}):`, e?.message || e);
      if (/timeout/i.test(e?.message || "")) {
        console.warn("joinVoice non risolto ma l'account è nel canale: provo comunque a trasmettere");
        return;
      }
      if (a >= 4) throw e;
      await sleep(5000 * a + jitter(0, 3000));
    }
  }
}

// --- Un turno: entra, trasmette i video in loop fino a fine turno, esce ---
async function runSession(end) {
  let failures = 0;
  let emptyLogged = false;
  try {
    await joinVC();
    console.log(`Entrato nel VC, resto fino alle ${fmt(end)}`);

    while (!stopping && Date.now() < end - 5000) {
      try {
        const next = await nextSource();
        if (!next) {
          if (!emptyLogged) console.log("Nessun video disponibile (canale vuoto?), aspetto...");
          emptyLogged = true;
          await sleep(30_000);
          continue;
        }
        emptyLogged = false;
        console.log(`Avvio video: ${next.label}`);
        preflight(next.src);

        const encoder = Encoders.software({
          x264: { preset: "superfast" },
          x265: { preset: "superfast" },
        });
        const { output, promise } = await prepareStream(next.src, {
          encoder,
          height: STREAM_HEIGHT, // solo altezza: mantiene le proporzioni
          frameRate: STREAM_FPS,
          fps: STREAM_FPS,
          bitrateVideo: STREAM_BITRATE,
          bitrateVideoMax: Math.round(STREAM_BITRATE * 1.5),
          videoCodec: Utils.normalizeVideoCodec(VIDEO_CODEC),
          audioStream: () => null, // solo video
        });
        promise?.catch?.((e) => console.error("transcode:", e?.message || e));

        console.log(`playStream in corso... tipo=${STREAM_TYPE} codec=${VIDEO_CODEC} h=${STREAM_HEIGHT}@${STREAM_FPS} ${STREAM_BITRATE}kbps`);
        const played = playStream(output, streamer, { type: STREAM_TYPE });
        played.catch(() => {});
        // taglia lo stream esattamente a fine turno
        let cutTimer;
        const cut = new Promise((r) => { cutTimer = setTimeout(() => r("cut"), Math.max(0, end - Date.now())); });
        const res = await Promise.race([played, cut]);
        clearTimeout(cutTimer);
        if (res === "cut") {
          try { output.destroy(); } catch {}
          break;
        }
        console.log("playStream terminato");
        failures = 0;
      } catch (err) {
        if (Date.now() >= end - 5000) break;
        failures++;
        console.error(`Errore stream (#${failures}):`, err?.message || err);
        if (failures >= 8) {
          throw new Error("Troppi errori consecutivi: chiudo questo turno e riprovo al prossimo");
        }
        if (/WebRTC|not ready/i.test(err?.message || "")) {
          // connessione media non pronta: esco e rientro nel canale
          try { streamer.leaveVoice(); } catch {}
          await sleep(3000);
          try { await joinVC(); } catch (e) { console.error("Rientro fallito:", e?.message || e); }
        }
        await sleep(Math.min(10_000 * 2 ** (failures - 1), 300_000) + jitter(0, 5000));
      }
      await sleep(jitter(1000, 3000));
    }
  } catch (err) {
    console.error("Errore nel turno:", err?.message || err);
  } finally {
    try { streamer.leaveVoice(); } catch {}
    console.log("Uscito dal VC");
  }
}

// --- Scheduler ---
async function scheduler() {
  let mustStartAfter = 0;

  if (START_ON_DEPLOY) {
    const end = Date.now() + SESSION_MINUTES * MIN;
    console.log("Avvio: entro subito e inizio il turno");
    mustStartAfter = end;
    await runSession(end);
  }

  while (!stopping) {
    const now = Date.now();
    const s = nextSession(now, mustStartAfter);
    if (!s) { await sleep(10 * MIN); continue; }

    if (s.start > now) {
      console.log(`Prossimo turno: ${fmt(s.start)} → ${fmt(s.end)}`);
      while (!stopping && Date.now() < s.start) {
        await sleep(Math.min(30_000, s.start - Date.now()));
      }
      if (stopping) break;
    } else {
      console.log("Turno già in corso, entro per il tempo restante");
    }
    await runSession(s.end);
  }
}

// --- Spegnimento pulito ---
function shutdown() {
  stopping = true;
  try { streamer.leaveVoice(); } catch {}
  try { client.destroy(); } catch {}
  process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("unhandledRejection", (e) => console.error("Unhandled:", e));

client.on("voiceStateUpdate", (oldS, newS) => {
  if (newS.id !== client.user?.id) return;
  console.log(
    `[voice] canale=${newS.channelId} selfDeaf=${newS.selfDeaf} selfMute=${newS.selfMute} selfVideo=${newS.selfVideo} streaming=${newS.streaming}`
  );
});

client.on("ready", async () => {
  console.log(`Loggato come ${client.user.tag}`);
  try {
    await loadPlaylist();
  } catch (e) {
    console.error("Playlist non caricata:", e?.message || e, "→ controlla MEDIA_CHANNEL_ID e che l'account veda il canale");
  }
  const udp = await udpCheck();
  console.log(
    udp.ok
      ? `[udp] OK: il container riceve risposte UDP (IP pubblico ${udp.ip})`
      : `[udp] PROBLEMA: ${udp.reason} → Discord voice/video non può funzionare da qui`
  );
  scheduleStatus();
  scheduler();
});

client.login(TOKEN);
