import { TikTokLiveConnection, WebcastEvent } from "tiktok-live-connector";
import pkg from "pg";

const { Pool } = pkg;

/* ================= DATABASE ================= */

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

/* ================= CONFIG ================= */

const POLL_INTERVAL_SECONDS = 60;
const CONNECT_COOLDOWN_MS = 5 * 60 * 1000;
const CONNECT_STAGGER_MS = 4000;

/* ================= STATE ================= */

const activeConnections = new Map();
const failedConnections = new Map();
const liveSessionLock = new Set();

/* ================= HELPERS ================= */

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getCreators() {
  const { rows } = await pool.query(`
    select distinct creator_id, tiktok_username
    from users
    where tiktok_username is not null
      and tiktok_username <> ''
  `);

  return rows.map(r => ({
    creator_id: String(r.creator_id),
    username: String(r.tiktok_username).replace(/^@/, "")
  }));
}

async function isLive(username) {
  const conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    fetchRoomInfoOnConnect: false
  });

  try {
    return Boolean(await conn.fetchIsLive());
  } catch {
    return false;
  }
}

/* ================= CORE TRACKING ================= */

async function createBattleIfMissing(creator, opponent, battleRef) {
  const { rows } = await pool.query(
    `
    insert into battles (
      creator_id,
      creator_username,
      opponent_username,
      tiktok_battle_id,
      started_at,
      creator_score,
      opponent_score
    ) values ($1,$2,$3,$4,now(),0,0)
    returning id
    `,
    [
      creator.creator_id,
      creator.username,
      opponent || "unknown",
      battleRef || null
    ]
  );

  return rows[0].id;
}

async function startTracking(creator) {
  if (activeConnections.has(creator.creator_id)) return;
  if (liveSessionLock.has(creator.creator_id)) return;

  const lastFailed = failedConnections.get(creator.creator_id);
  if (lastFailed && Date.now() - lastFailed < CONNECT_COOLDOWN_MS) return;

  liveSessionLock.add(creator.creator_id);

  const conn = new TikTokLiveConnection(creator.username);
  let activeBattleId = null;

  /* ===== BATTLE START (if emitted) ===== */
  conn.on(WebcastEvent.BATTLE_START, async e => {
    if (activeBattleId) return;

    activeBattleId = await createBattleIfMissing(
      creator,
      e?.opponent?.username,
      e?.battleId
    );
  });

  /* ===== BATTLE UPDATE (PRIMARY TRIGGER) ===== */
  conn.on(WebcastEvent.BATTLE_UPDATE, async e => {
    if (!activeBattleId) {
      activeBattleId = await createBattleIfMissing(
        creator,
        e?.opponent?.username,
        e?.battleId
      );
    }

    await pool.query(
      `
      update battles
      set creator_score = $1,
          opponent_score = $2
      where id = $3
      `,
      [
        Number(e?.score || 0),
        Number(e?.opponentScore || 0),
        activeBattleId
      ]
    );
  });

  /* ===== BATTLE END ===== */
  conn.on(WebcastEvent.BATTLE_END, async e => {
    if (!activeBattleId) return;

    const creatorScore = Number(e?.score || 0);
    const opponentScore = Number(e?.opponentScore || 0);

    const winner =
      creatorScore > opponentScore
        ? "creator"
        : creatorScore < opponentScore
        ? "opponent"
        : "draw";

    await pool.query(
      `
      update battles
      set ended_at = now(),
          creator_score = $1,
          opponent_score = $2,
          winner = $3
      where id = $4
      `,
      [creatorScore, opponentScore, winner, activeBattleId]
    );

    activeBattleId = null;
  });

  /* ===== GIFTS (ATTACHED TO ACTIVE BATTLE) ===== */
  conn.on(WebcastEvent.GIFT, async g => {
    if (!activeBattleId) return;

    const diamondValue = Number(g?.gift?.diamondCount || 0);
    const quantity = Number(g?.repeatCount || 1);

    await pool.query(
      `
      insert into battle_gifts (
        battle_id,
        gifter_username,
        gift_name,
        gift_id,
        diamond_value,
        quantity,
        total_diamonds,
        gifted_at
      ) values ($1,$2,$3,$4,$5,$6,$7,now())
      `,
      [
        activeBattleId,
        g?.user?.uniqueId || "unknown",
        g?.gift?.name || "unknown",
        g?.gift?.id || null,
        diamondValue,
        quantity,
        diamondValue * quantity
      ]
    );
  });

  try {
    await conn.connect();
    activeConnections.set(creator.creator_id, conn);
    failedConnections.delete(creator.creator_id);
  } catch (err) {
    failedConnections.set(creator.creator_id, Date.now());
  } finally {
    liveSessionLock.delete(creator.creator_id);
  }
}

async function stopTracking(creatorId) {
  const conn = activeConnections.get(creatorId);
  if (!conn) return;

  try {
    await conn.disconnect();
  } catch {}

  activeConnections.delete(creatorId);
  liveSessionLock.delete(creatorId);
}

/* ================= MAIN LOOP ================= */

async function poll() {
  const creators = await getCreators();

  for (const creator of creators) {
    const live = await isLive(creator.username);

    if (live) {
      await startTracking(creator);
      await sleep(CONNECT_STAGGER_MS);
    } else {
      await stopTracking(creator.creator_id);
    }
  }
}

console.log("Battle tracking worker started");

poll();
setInterval(poll, POLL_INTERVAL_SECONDS * 1000);
