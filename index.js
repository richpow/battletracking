import { TikTokLiveConnection } from "@adamjessop/tiktok-live-connector";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const POLL_SECONDS = 60;
const activeConnections = new Map();

async function getCreators() {
  const { rows } = await pool.query(`
    select distinct creator_id, tiktok_username
    from users
    where tiktok_username is not null
  `);
  return rows;
}

async function isLive(username) {
  const conn = new TikTokLiveConnection(username, {
    processInitialData: false,
    fetchRoomInfoOnConnect: false
  });
  return Boolean(await conn.fetchIsLive());
}

async function startTracking(creator) {
  if (activeConnections.has(creator.creator_id)) return;

  const conn = new TikTokLiveConnection(creator.tiktok_username);
  let activeBattle = null;

  conn.on("battleStart", async e => {
    const { rows } = await pool.query(
      `
      insert into battles (
        creator_id,
        creator_username,
        opponent_username,
        tiktok_battle_id,
        started_at
      ) values ($1,$2,$3,$4,now())
      returning id
      `,
      [
        creator.creator_id,
        creator.tiktok_username,
        e.opponent?.username || "unknown",
        e.battleId
      ]
    );
    activeBattle = rows[0].id;
  });

  conn.on("battleUpdate", async e => {
    if (!activeBattle) return;
    await pool.query(
      `
      update battles
      set creator_score = $1,
          opponent_score = $2
      where id = $3
      `,
      [e.score, e.opponentScore, activeBattle]
    );
  });

  conn.on("battleEnd", async e => {
    if (!activeBattle) return;
    const winner =
      e.score > e.opponentScore
        ? "creator"
        : e.score < e.opponentScore
        ? "opponent"
        : "draw";

    await pool.query(
      `
      update battles
      set ended_at = now(),
          winner = $1
      where id = $2
      `,
      [winner, activeBattle]
    );

    activeBattle = null;
  });

  conn.on("gift", async g => {
    if (!activeBattle) return;

    await pool.query(
      `
      insert into battle_gifts (
        battle_id,
        gifter_username,
        gift_name,
        gift_id,
        diamond_value,
        quantity,
        total_diamonds
      ) values ($1,$2,$3,$4,$5,$6,$7)
      `,
      [
        activeBattle,
        g.uniqueId,
        g.giftName,
        g.giftId,
        g.diamondCount,
        g.repeatCount,
        g.diamondCount * g.repeatCount
      ]
    );
  });

  await conn.connect();
  activeConnections.set(creator.creator_id, conn);
}

async function stopTracking(creatorId) {
  const conn = activeConnections.get(creatorId);
  if (!conn) return;
  await conn.disconnect();
  activeConnections.delete(creatorId);
}

async function loop() {
  const creators = await getCreators();

  for (const c of creators) {
    const live = await isLive(c.tiktok_username);
    if (live) await startTracking(c);
    else await stopTracking(c.creator_id);
  }
}

setInterval(loop, POLL_SECONDS * 1000);
loop();
