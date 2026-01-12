import { TikTokLiveConnection, WebcastEvent, ControlEvent } from "tiktok-live-connector";
import pkg from "pg";

const { Pool } = pkg;

/* ================= DATABASE ================= */

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

/* ================= CONFIG ================= */

const POLL_INTERVAL_SECONDS = Number(process.env.POLL_INTERVAL_SECONDS || 60);
const CONNECT_COOLDOWN_MS = Number(process.env.CONNECT_COOLDOWN_MS || 5 * 60 * 1000);
const CONNECT_STAGGER_MS = Number(process.env.CONNECT_STAGGER_MS || 4000);

const DIAG_BATTLE_LOGS = String(process.env.DIAG_BATTLE_LOGS || "0") === "1";

/* ================= STATE ================= */

const activeConnections = new Map(); // creator_id -> { conn, state }
const failedConnections = new Map(); // creator_id -> timestamp ms
const liveSessionLock = new Set(); // creator_id

let lastStatusLogAt = 0;
let diagLastLogAt = 0;

/* ================= HELPERS ================= */

const sleep = ms => new Promise(r => setTimeout(r, ms));

function logStatusOncePerMinute(eligibleCount) {
  const now = Date.now();
  if (now - lastStatusLogAt < 60_000) return;
  lastStatusLogAt = now;
  console.log(`[TRACKING] eligible ${eligibleCount} active ${activeConnections.size}`);
}

function diagLog(message) {
  if (!DIAG_BATTLE_LOGS) return;

  const now = Date.now();
  if (now - diagLastLogAt < 15_000) return;
  diagLastLogAt = now;
  console.log(message);
}

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

/* ================= DB HELPERS ================= */

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

async function createBattle(creator, opponentUsername, battleRef) {
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
      opponentUsername || null,
      battleRef || null
    ]
  );
  return rows[0].id;
}

async function ensureBattle(creator, opponentUsername, battleRef) {
  let battleId = await getOpenBattle(creator.creator_id);

  if (battleId) {
    if (opponentUsername) {
      await pool.query(
        `
        update tiktok_live_battles
        set opponent_username = $1
        where id = $2
          and opponent_username is null
        `,
        [opponentUsername, battleId]
      );
    }
    return battleId;
  }

  if (!opponentUsername) return null;
  return createBattle(creator, opponentUsername, battleRef);
}

async function endBattleIfOpen(activeBattleId, winner, creatorScore, opponentScore) {
  if (!activeBattleId) return;

  await pool.query(
    `
    update tiktok_live_battles
    set ended_at = now(),
        creator_score = $1,
        opponent_score = $2,
        winner = $3
    where id = $4
    `,
    [Number(creatorScore || 0), Number(opponentScore || 0), winner, activeBattleId]
  );
}

/* ================= BATTLE PARSING ================= */

/**
 * LINK_MIC_BATTLE provides participants.
 * Schema: WebcastLinkMicBattle { battleUsers: [{ battleGroup: { user } }] }  [oai_citation:2‡jsDelivr](https://cdn.jsdelivr.net/npm/%40adamjessop/tiktok-live-connector%402.0.1/dist/types/tiktok-schema.d.ts)
 */
function extractOpponentFromLinkMicBattle(creatorUsername, e) {
  const users =
    e?.battleUsers
      ?.map(x => x?.battleGroup?.user)
      ?.filter(Boolean)
      ?.map(u => String(u.uniqueId || "").replace(/^@/, "").trim())
      ?.filter(Boolean) || [];

  const creator = String(creatorUsername).replace(/^@/, "").trim().toLowerCase();
  const opponent = users.find(u => u.toLowerCase() !== creator);

  return opponent || null;
}

/**
 * LINK_MIC_ARMIES provides points.
 * Schema: WebcastLinkMicArmies { battleItems: [{ battleGroups: [{ users, points }] }] }  [oai_citation:3‡jsDelivr](https://cdn.jsdelivr.net/npm/%40adamjessop/tiktok-live-connector%402.0.1/dist/types/tiktok-schema.d.ts)
 */
function extractScoresFromArmies(creatorUsername, e) {
  const creator = String(creatorUsername).replace(/^@/, "").trim().toLowerCase();

  const item = e?.battleItems?.[0];
  const groups = item?.battleGroups || [];
  if (!Array.isArray(groups) || groups.length < 2) return null;

  const normalizedGroups = groups.map(g => {
    const users = Array.isArray(g?.users) ? g.users : [];
    const usernames = users
      .map(u => String(u?.uniqueId || "").replace(/^@/, "").trim())
      .filter(Boolean);

    return {
      usernames,
      points: Number(g?.points || 0)
    };
  });

  const creatorGroup = normalizedGroups.find(g =>
    g.usernames.some(u => u.toLowerCase() === creator)
  );
  if (!creatorGroup) return null;

  const opponentGroup = normalizedGroups.find(g =>
    !g.usernames.some(u => u.toLowerCase() === creator)
  );
  if (!opponentGroup) return null;

  const opponentUsername = opponentGroup.usernames[0] || null;

  return {
    creatorScore: creatorGroup.points,
    opponentScore: opponentGroup.points,
    opponentUsername
  };
}

/* ================= TRACKING ================= */

async function startTracking(creator) {
  if (activeConnections.has(creator.creator_id)) return;
  if (liveSessionLock.has(creator.creator_id)) return;

  const lastFailed = failedConnections.get(creator.creator_id);
  if (lastFailed && Date.now() - lastFailed < CONNECT_COOLDOWN_MS) return;

  liveSessionLock.add(creator.creator_id);

  const conn = new TikTokLiveConnection(creator.username, {
    processInitialData: true,
    fetchRoomInfoOnConnect: true
  });

  const state = {
    activeBattleId: null,
    activeOpponent: null,
    seenGiftKeys: new Set(),
    lastCreatorScore: 0,
    lastOpponentScore: 0
  };

  // Participants event (battle detected)
  conn.on(WebcastEvent.LINK_MIC_BATTLE, async e => {
    const opponent = extractOpponentFromLinkMicBattle(creator.username, e);
    if (!opponent) return;

    state.activeOpponent = opponent;
    state.activeBattleId = await ensureBattle(creator, opponent, null);

    diagLog(`[DIAG] linkMicBattle for ${creator.username} vs ${opponent}`);
  });

  // Points event (battle updates)
  conn.on(WebcastEvent.LINK_MIC_ARMIES, async e => {
    const scores = extractScoresFromArmies(creator.username, e);
    if (!scores) return;

    // Ensure we have a battle row as soon as we see score packets
    state.activeOpponent = state.activeOpponent || scores.opponentUsername || null;
    state.activeBattleId = await ensureBattle(creator, state.activeOpponent, null);
    if (!state.activeBattleId) return;

    state.lastCreatorScore = scores.creatorScore;
    state.lastOpponentScore = scores.opponentScore;

    await pool.query(
      `
      update tiktok_live_battles
      set creator_score = $1,
          opponent_score = $2,
          opponent_username = coalesce(opponent_username, $3)
      where id = $4
      `,
      [
        Number(scores.creatorScore || 0),
        Number(scores.opponentScore || 0),
        scores.opponentUsername,
        state.activeBattleId
      ]
    );

    diagLog(`[DIAG] linkMicArmies for ${creator.username}`);
  });

  // Gifts only during an active battle
  conn.on(WebcastEvent.GIFT, async g => {
    if (!state.activeBattleId) return;

    const dedupeKey = [
      g?.user?.uniqueId,
      g?.giftId,
      g?.repeatCount,
      Math.floor(Number(g?.giftExtra?.timestamp || Date.now()) / 1000)
    ].join(":");

    if (state.seenGiftKeys.has(dedupeKey)) return;
    state.seenGiftKeys.add(dedupeKey);

    const diamondValue = Number(g?.giftDetails?.diamondCount || 0);
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
        state.activeBattleId,
        String(g?.user?.uniqueId || "unknown").replace(/^@/, ""),
        g?.giftDetails?.giftName || "unknown",
        g?.giftId || null,
        diamondValue,
        quantity,
        diamondValue * quantity
      ]
    );
  });

  // Stream end, close open battle
  conn.on(WebcastEvent.STREAM_END, async () => {
    if (state.activeBattleId) {
      const winner =
        state.lastCreatorScore > state.lastOpponentScore
          ? "creator"
          : state.lastCreatorScore < state.lastOpponentScore
          ? "opponent"
          : "draw";

      await endBattleIfOpen(
        state.activeBattleId,
        winner,
        state.lastCreatorScore,
        state.lastOpponentScore
      );

      state.activeBattleId = null;
      state.activeOpponent = null;
      state.seenGiftKeys.clear();
    }

    await stopTracking(creator.creator_id);
  });

  // Disconnect, close open battle defensively
  conn.on(ControlEvent.DISCONNECTED, async () => {
    if (state.activeBattleId) {
      const winner =
        state.lastCreatorScore > state.lastOpponentScore
          ? "creator"
          : state.lastCreatorScore < state.lastOpponentScore
          ? "opponent"
          : "draw";

      await endBattleIfOpen(
        state.activeBattleId,
        winner,
        state.lastCreatorScore,
        state.lastOpponentScore
      );

      state.activeBattleId = null;
      state.activeOpponent = null;
      state.seenGiftKeys.clear();
    }

    await stopTracking(creator.creator_id);
  });

  try {
    await conn.connect();
    activeConnections.set(creator.creator_id, { conn, state });
    failedConnections.delete(creator.creator_id);
  } catch {
    failedConnections.set(creator.creator_id, Date.now());
  } finally {
    liveSessionLock.delete(creator.creator_id);
  }
}

async function stopTracking(creatorId) {
  const entry = activeConnections.get(creatorId);
  if (!entry) return;

  try {
    await entry.conn.disconnect();
  } catch {}

  activeConnections.delete(creatorId);
  liveSessionLock.delete(creatorId);
}

/* ================= LOOP ================= */

async function poll() {
  let creators;
  try {
    creators = await getCreators();
  } catch {
    return;
  }

  logStatusOncePerMinute(creators.length);

  for (const creator of creators) {
    await startTracking(creator);
    await sleep(CONNECT_STAGGER_MS);
  }
}

console.log("Battle tracking worker started");

poll();
setInterval(poll, POLL_INTERVAL_SECONDS * 1000);
