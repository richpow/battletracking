import { TikTokLiveConnection, WebcastEvent } from "tiktok-live-connector";
import pkg from "pg";

const { Pool } = pkg;

/* =======================
   DATABASE
======================= */

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

/* =======================
   CONFIG
======================= */

const POLL_INTERVAL_SECONDS = 60;
const activeConnections = new Map();

/* =======================
   HELPERS
======================= */

async function getCreators() {
  const { rows } = await pool.query(`
    select distinct
      creator_id,
      tiktok_username
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

/* =======================
   TRACKING
======================= */

async function startTracking(creator) {
  if (activeConnections.has(creator.creator_id)) return;

  console.log(`[TRACKING START] ${creator.username}`);

  const conn = new TikTokLiveConnection(creator.username);
  let activeBattleId = null;

  conn.on(WebcastEvent.BATTLE_START, async e => {
    try {
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
          e?.opponent?.username || "unknown",
          e?.battleId || null
        ]
      );

      activeBattleId = rows[0].id;
      console.log(`[BATTLE START] ${creator.username}`);
    } catch (err) {
      console.error("Battle start error", err);
    }
  });

  conn.on(WebcastEvent.BATTLE_UPDATE, async e => {
    if (!activeBattleId) return;

    try {
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
    } catch (err) {
      console.error("Battle update error", err);
    }
  });

  conn.on(WebcastEvent.BATTLE_END, async e => {
    if (!activeBattleId) return;

    try {
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

      console.log(`[BATTLE END] ${creator.username}`);
      activeBattleId = null;
    } catch (err) {
      console.error("Battle end error", err);
    }
  });

  conn.on(WebcastEvent.GIFT, async g => {
    if (!activeBattleId) return;

    try {
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
    } catch (err) {
      console.error("Gift insert error", err);
    }
  });

  try {
    await conn.connect();
    activeConnections.set(creator.creator_id, conn);
  } catch (err) {
    console.error(`Connection failed for ${creator.username}`, err);
  }
}

async function stopTracking(creatorId) {
  const conn = activeConnections.get(creatorId);
  if (!conn) return;

  try {
    await conn.disconnect();
  } catch {}

  activeConnections.delete(creatorId);
}

/* =======================
   MAIN LOOP
======================= */

async function poll() {
  let creators = [];

  try {
    creators = await getCreators();
  } catch (err) {
    console.error("Failed to load creators", err);
    return;
  }

  for (const creator of creators) {
    const live = await isLive(creator.username);

    if (live) {
      await startTracking(creator);
    } else {
      await stopTracking(creator.creator_id);
    }
  }
}

console.log("Battle tracking worker started");

poll();
setInterval(poll, POLL_INTERVAL_SECONDS * 1000);
