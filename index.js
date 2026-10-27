import ffmpegPath from "ffmpeg-static";
process.env.FFMPEG_PATH = ffmpegPath;

import { Client } from "discord.js-selfbot-v13";
import { Streamer, prepareStream, playStream } from "@dank074/discord-video-stream";

// --- Config (variabili Railway) ---
const TOKEN = process.env.TOKEN;
const GUILD_ID = process.env.GUILD_ID;
const CHANNEL_ID = process.env.CHANNEL_ID;
const VIDEOS = (process.env.VIDEOS || "./video.mp4").split(",").map((v) => v.trim());

const STATUSES = (process.env.STATUSES || "chill").split("|").map((s) => s.trim()).filter(Boolean);
const STATUS_TYPE = process.env.STATUS_TYPE || "PLAYING";
const STATUS_INTERVAL_MIN = Number(process.env.STATUS_INTERVAL_MIN || 15);

// Turni
const SESSIONS_PER_DAY = Number(process.env.SESSIONS_PER_DAY || 3);
const SESSION_MINUTES = Number(process.env.SESSION_MINUTES || 90);
const MIN_GAP_MINUTES = Number(process.env.MIN_GAP_MINUTES || 30);
const TIMEZONE = process.env.TIMEZONE || "Europe/Rome";
const SCHEDULE_SEED = process.env.SCHEDULE_SEED || "seed";

if (!TOKEN || !GUILD_ID || !CHANNEL_ID) {
  console.error("Mancano TOKEN, GUILD_ID o CHANNEL_ID");
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
// Turni casuali ma stabili per quel giorno (un riavvio non cambia gli orari)
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
function nextSession(now) {
  for (let d = 0; d < 3; d++) {
    const key = dayKeyOf(now + d * DAY, TIMEZONE);
    const list = scheduleForDay(key, SESSIONS_PER_DAY, SESSION_MINUTES, MIN_GAP_MINUTES, TIMEZONE, SCHEDULE_SEED);
    const s = list.find((x) => x.end > now + 2 * MIN);
    if (s) return s;
  }
  return null;
}
// END-SCHED

const fmt = (ts) => new Date(ts).toLocaleString("it-IT", { timeZone: TIMEZONE });

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

// --- Un turno: entra, trasmette i video in loop fino a fine turno, esce ---
let videoIndex = 0;
async function runSession(end) {
  let failures = 0;
  try {
    await streamer.joinVoice(GUILD_ID, CHANNEL_ID);
    console.log(`Entrato nel VC, resto fino alle ${fmt(end)}`);

    while (!stopping && Date.now() < end - 5000) {
      let killer;
      try {
        const { command, output } = prepareStream(VIDEOS[videoIndex % VIDEOS.length], {
          width: 1280, height: 720, frameRate: 30,
          bitrateKbps: 2500, maxBitrateKbps: 3000,
          videoCodec: "H264", includeAudio: false,
        });
        command.on("error", (err) => console.error("ffmpeg:", err?.message));
        // taglia lo stream esattamente a fine turno
        killer = setTimeout(() => { try { command.kill("SIGKILL"); } catch {} }, Math.max(0, end - Date.now()));

        await playStream(output, streamer, { type: "camera" }); // o "go-live"
        videoIndex++;
        failures = 0;
      } catch (err) {
        if (Date.now() >= end - 5000) break; // errore dovuto al taglio di fine turno
        failures++;
        console.error(`Errore stream (#${failures}):`, err?.message || err);
        if (failures >= 8) {
          console.error("Troppi errori consecutivi, mi fermo. Controlla token/account.");
          process.exit(1);
        }
        await sleep(Math.min(10_000 * 2 ** (failures - 1), 300_000) + jitter(0, 5000));
      } finally {
        clearTimeout(killer);
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
  while (!stopping) {
    const now = Date.now();
    const s = nextSession(now);
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

client.on("ready", () => {
  console.log(`Loggato come ${client.user.tag}`);
  scheduleStatus();
  scheduler();
});

client.login(TOKEN);
