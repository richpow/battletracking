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

const BATTLE_SILENCE_MS = Number(process.env.BATTLE_SILENCE_MS || 90_000);

const DIAG_BATTLE_LOGS = String(process.env.DIAG_BATTLE_LOGS || "0") === "1";

/* ================= STATE ================= */

const activeConnections = new Map(); // creator_id -> { conn, state }
const failedConnections = new Map(); // creator_id -> timestamp ms
const liveSessionLock = new Set(); // creator_id

let lastStatusLogAt = 0;
let diagLastLogAt = 0;

let lastMinuteAttempts = 0;
let lastMinuteSuccess = 0;
let lastMinuteFail = 0;
let lastFailReason = "";

/* ================= HELPERS ================= */

const sleep = ms => new Promise(r => setTimeout(r, ms));

function logStatusOncePerMinute(eligibleCount) {
  const now = Date.now();
  if (now - lastStatusLogAt < 60_000) return;
  lastStatusLogAt = now;

  console.log(
    `[TRACKING] eligible ${eligibleCount} active ${activeConnections.size} attempts ${lastMinuteAttempts} ok ${lastMinuteSuccess} fail ${lastMinuteFail}${lastFailReason ? " lastFail " + lastFailReason : ""}`
  );

  lastMinuteAttempts = 0;
  lastMinuteSuccess = 0;
  lastMinuteFail = 0;
  lastFailReason = "";
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

async function closeBattleFromTimeout(creator, state) {
  if (!state.activeBattleId) return;

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

  diagLog(`[DIAG] battle closed by silence for ${creator.username}`);

  state.activeBattleId = null;
  state.activeOpponent = null;
  state.seenGiftKeys.clear();
  state.lastCreatorScore = 0;
  state.lastOpponentScore = 0;
  state.lastArmyAt = null;
  if (state.battleTimeout) {
    clearTimeout(state.battleTimeout);
    state.battleTimeout = null;
  }
}

function armSilenceTimerReset(creator, state) {
  state.lastArmyAt = Date.now();
  if (state.battleTimeout) clearTimeout(state.battleTimeout);

  state.battleTimeout = setTimeout(() => {
    closeBattleFromTimeout(creator, state).catch(() => {});
  }, BATTLE_SILENCE_MS);
}

async function startTracking(creator) {
  if (activeConnections.has(creator.creator_id)) return;
  if (liveSessionLock.has(creator.creator_id)) return;

  const lastFailed = failedConnections.get(creator.creator_id);
  if (lastFailed && Date.now() - lastFailed < CONNECT_COOLDOWN_MS) return;

  liveSessionLock.add(creator.creator_id);

  const conn = new TikTokLiveConnection(creator.username, {
    processInitialData: true,
    fetchRoomInfoOnConnect: false,
    enableRequestPolling: true,
    requestPollingIntervalMs: 2000,
    enableExtendedGiftInfo: true
  });

  const state = {
    activeBattleId: null,
    activeOpponent: null,
    seenGiftKeys: new Set(),
    lastCreatorScore: 0,
    lastOpponentScore: 0,
    lastArmyAt: null,
    battleTimeout: null
  };

  conn.on(WebcastEvent.LINK_MIC_BATTLE, async e => {
    const opponent = extractOpponentFromLinkMicBattle(creator.username, e);
    if (!opponent) return;

    state.activeOpponent = opponent;
    state.activeBattleId = await ensureBattle(creator, opponent, null);

    diagLog(`[DIAG] linkMicBattle ${creator.username} vs ${opponent}`);
  });

  conn.on(WebcastEvent.LINK_MIC_ARMIES, async e => {
    const scores = extractScoresFromArmies(creator.username, e);
    if (!scores) return;

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

    armSilenceTimerReset(creator, state);
    diagLog(`[DIAG] linkMicArmies ${creator.username}`);
  });

  conn.on(WebcastEvent.GIFT, async g => {
    if (!state.activeBattleId) return;

    const gifter =
      String(g?.user?.uniqueId ?? g?.userId ?? "unknown").replace(/^@/, "");

    const giftId = g?.giftId ?? g?.giftDetails?.giftId ?? null;
    const giftName = g?.giftDetails?.giftName ?? "unknown";
    const diamondValue = Number(g?.giftDetails?.diamondCount || 0);
    const quantity = Number(g?.repeatCount || 1);

    const dedupeKey = [
      gifter,
      giftId,
      quantity,
      Math.floor(Date.now() / 1000)
    ].join(":");

    if (state.seenGiftKeys.has(dedupeKey)) return;
    state.seenGiftKeys.add(dedupeKey);

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
        gifter,
        giftName,
        giftId,
        diamondValue,
        quantity,
        diamondValue * quantity
      ]
    );
  });

  conn.on(WebcastEvent.STREAM_END, async () => {
    // Do not end battles here, battles end on army silence.
    await stopTracking(creator.creator_id);
  });

  conn.on(ControlEvent.DISCONNECTED, async () => {
    // Do not end battles here, battles end on army silence.
    await stopTracking(creator.creator_id);
  });

  try {
    lastMinuteAttempts += 1;
    await conn.connect();
    activeConnections.set(creator.creator_id, { conn, state });
    failedConnections.delete(creator.creator_id);
    lastMinuteSuccess += 1;
  } catch (err) {
    failedConnections.set(creator.creator_id, Date.now());
    lastMinuteFail += 1;

    const msg = String(err?.message || err || "");
    lastFailReason = msg ? msg.slice(0, 120) : "connect failed";
  } finally {
    liveSessionLock.delete(creator.creator_id);
  }
}

async function stopTracking(creatorId) {
  const entry = activeConnections.get(creatorId);
  if (!entry) return;

  try {
    if (entry.state?.battleTimeout) clearTimeout(entry.state.battleTimeout);
  } catch {}

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
