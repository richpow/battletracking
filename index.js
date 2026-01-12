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

/* ================= BATTLE HELPERS ================= */

async function getOpenBattle(creatorId) {
  const { rows } = await pool.query(
    `
    select id
    from tiktok_live_battles
    where creator_id = $1
      and ended_at is null
    order by started_at desc
    limit 1
    `,
    [creatorId]
  );

  return rows[0]?.id || null;
}

async function createBattle(creator, opponent, battleRef) {
  const { rows } = await pool.query(
    `
    insert into tiktok_live_battles (
      creator_id,
      creator_username,
      opponent_username,
      tiktok_battle_id,
      started_at,
      creator_score,
      opponent_score
    )
    values ($1,$2,$3,$4,now(),0,0)
    returning id
    `,
    [
      creator.creator_id,
      creator.username,
      opponent || null,
      battleRef || null
    ]
  );

  return rows[0].id;
}

async function ensureBattle(creator, opponent, battleRef) {
  let battleId = await getOpenBattle(creator.creator_id);

  if (battleId) {
    if (opponent) {
      await pool.query(
        `
        update tiktok_live_battles
        set opponent_username = $1
        where id = $2
          and opponent_username is null
        `,
        [opponent, battleId]
      );
    }
    return battleId;
  }

  return createBattle(creator, opponent, battleRef);
}

/* ================= TRACKING ================= */

async function startTracking(creator) {
  if (activeConnections.has(creator.creator_id)) return;
  if (liveSessionLock.has(creator.creator_id)) return;

  const lastFailed = failedConnections.get(creator.creator_id);
  if (lastFailed && Date.now() - lastFailed < CONNECT_COOLDOWN_MS) return;

  liveSessionLock.add(creator.creator_id);

  const conn = new TikTokLiveConnection(creator.username);
  let activeBattleId = null;
  const seenGiftKeys = new Set();

  conn.on(WebcastEvent.BATTLE_START, async e => {
    activeBattleId = await ensureBattle(
      creator,
      e?.opponent?.username,
      e?.battleId
    );
  });

  conn.on(WebcastEvent.BATTLE_UPDATE, async e => {
    activeBattleId = await ensureBattle(
      creator,
      e?.opponent?.username,
      e?.battleId
    );

    await pool.query(
      `
      update tiktok_live_battles
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

  conn.on(WebcastEvent.GIFT, async g => {
    activeBattleId = await ensureBattle(creator, null, null);

    const dedupeKey = [
      g?.user?.uniqueId,
      g?.gift?.id,
      g?.repeatCount,
      Math.floor((g?.timestamp || Date.now()) / 1000)
    ].join(":");

    if (seenGiftKeys.has(dedupeKey)) return;
    seenGiftKeys.add(dedupeKey);

    const diamondValue = Number(g?.gift?.diamondCount || 0);
    const quantity = Number(g?.repeatCount || 1);

    await pool.query(
      `
      insert into tiktok_battle_gifts (
        battle_id,
        gifter_username,
        gift_name,
        gift_id,
        diamond_value,
        quantity,
        total_diamonds,
        gifted_at
      )
      values ($1,$2,$3,$4,$5,$6,$7,now())
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
      update tiktok_live_battles
      set ended_at = now(),
          creator_score = $1,
          opponent_score = $2,
          winner = $3
      where id = $4
      `,
      [creatorScore, opponentScore, winner, activeBattleId]
    );

    activeBattleId = null;
    seenGiftKeys.clear();
  });

  try {
    await conn.connect();
    activeConnections.set(creator.creator_id, conn);
    failedConnections.delete(creator.creator_id);
    console.log(`[CONNECT] ${creator.username}`);
  } catch {
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
}

/* ================= LOOP ================= */

async function poll() {
  const creators = await getCreators();

  console.log(
    `[TRACKING] eligible creators: ${creators.length} | active connections: ${activeConnections.size}`
  );

  for (const creator of creators) {
    await startTracking(creator);
    await sleep(CONNECT_STAGGER_MS);
  }
}

console.log("Battle tracking worker started");

poll();
setInterval(poll, POLL_INTERVAL_SECONDS * 1000);
