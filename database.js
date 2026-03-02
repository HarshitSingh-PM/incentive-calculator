const Database = require('better-sqlite3');
const path = require('path');

const DB_PATH = path.join(__dirname, 'data', 'incentives.db');

let db;

function getDb() {
  if (!db) {
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    initTables();
  }
  return db;
}

function initTables() {
  const d = getDb();

  d.exec(`
    CREATE TABLE IF NOT EXISTS incentive_config (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      month TEXT NOT NULL UNIQUE,
      fixed_incentive REAL NOT NULL DEFAULT 300,
      over_target_rate REAL NOT NULL DEFAULT 500,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS agent_targets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      agent_name TEXT NOT NULL,
      month TEXT NOT NULL,
      target INTEGER NOT NULL DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')),
      UNIQUE(agent_name, month)
    );

    CREATE TABLE IF NOT EXISTS incentive_results (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      agent_name TEXT NOT NULL,
      month TEXT NOT NULL,
      target INTEGER NOT NULL,
      actual_sales INTEGER NOT NULL,
      target_met INTEGER NOT NULL DEFAULT 0,
      fixed_incentive REAL NOT NULL DEFAULT 0,
      over_target_sales INTEGER NOT NULL DEFAULT 0,
      over_target_incentive REAL NOT NULL DEFAULT 0,
      total_incentive REAL NOT NULL DEFAULT 0,
      calculated_at TEXT DEFAULT (datetime('now')),
      UNIQUE(agent_name, month)
    );
  `);
}

// --- Incentive Config ---

function getConfig(month) {
  const d = getDb();
  return d.prepare('SELECT * FROM incentive_config WHERE month = ?').get(month);
}

function upsertConfig(month, fixedIncentive, overTargetRate) {
  const d = getDb();
  d.prepare(`
    INSERT INTO incentive_config (month, fixed_incentive, over_target_rate)
    VALUES (?, ?, ?)
    ON CONFLICT(month) DO UPDATE SET
      fixed_incentive = excluded.fixed_incentive,
      over_target_rate = excluded.over_target_rate,
      updated_at = datetime('now')
  `).run(month, fixedIncentive, overTargetRate);
  return getConfig(month);
}

// --- Agent Targets ---

function getTargets(month) {
  const d = getDb();
  return d.prepare('SELECT * FROM agent_targets WHERE month = ?').all(month);
}

function upsertTarget(agentName, month, target) {
  const d = getDb();
  d.prepare(`
    INSERT INTO agent_targets (agent_name, month, target)
    VALUES (?, ?, ?)
    ON CONFLICT(agent_name, month) DO UPDATE SET
      target = excluded.target,
      updated_at = datetime('now')
  `).run(agentName, month, target);
}

function upsertTargetsBulk(month, targets) {
  const d = getDb();
  const stmt = d.prepare(`
    INSERT INTO agent_targets (agent_name, month, target)
    VALUES (?, ?, ?)
    ON CONFLICT(agent_name, month) DO UPDATE SET
      target = excluded.target,
      updated_at = datetime('now')
  `);
  const transaction = d.transaction((items) => {
    for (const item of items) {
      stmt.run(item.agent_name, month, item.target);
    }
  });
  transaction(targets);
  return getTargets(month);
}

// --- Incentive Results ---

function calculateAndSave(month, salesData, config, targets) {
  const d = getDb();
  const targetMap = {};
  for (const t of targets) {
    targetMap[t.agent_name] = t.target;
  }

  const results = [];

  for (const agent of salesData) {
    const agentName = agent['Sales Agent'];
    const actualSales = agent['Count'];
    const target = targetMap[agentName] || 0;
    const targetMet = actualSales >= target && target > 0 ? 1 : 0;
    const fixedInc = targetMet ? actualSales * config.fixed_incentive : 0;
    const overTargetSales = targetMet ? Math.max(0, actualSales - target) : 0;
    const overTargetInc = overTargetSales * config.over_target_rate;
    const totalInc = fixedInc + overTargetInc;

    results.push({
      agent_name: agentName,
      month,
      target,
      actual_sales: actualSales,
      target_met: targetMet,
      fixed_incentive: fixedInc,
      over_target_sales: overTargetSales,
      over_target_incentive: overTargetInc,
      total_incentive: totalInc,
    });
  }

  const stmt = d.prepare(`
    INSERT INTO incentive_results (agent_name, month, target, actual_sales, target_met, fixed_incentive, over_target_sales, over_target_incentive, total_incentive)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(agent_name, month) DO UPDATE SET
      target = excluded.target,
      actual_sales = excluded.actual_sales,
      target_met = excluded.target_met,
      fixed_incentive = excluded.fixed_incentive,
      over_target_sales = excluded.over_target_sales,
      over_target_incentive = excluded.over_target_incentive,
      total_incentive = excluded.total_incentive,
      calculated_at = datetime('now')
  `);

  const transaction = d.transaction((items) => {
    for (const r of items) {
      stmt.run(
        r.agent_name, r.month, r.target, r.actual_sales,
        r.target_met, r.fixed_incentive, r.over_target_sales,
        r.over_target_incentive, r.total_incentive
      );
    }
  });
  transaction(results);

  return results;
}

function getResults(month) {
  const d = getDb();
  if (month) {
    return d.prepare('SELECT * FROM incentive_results WHERE month = ? ORDER BY total_incentive DESC').all(month);
  }
  return d.prepare('SELECT * FROM incentive_results ORDER BY month DESC, total_incentive DESC').all();
}

module.exports = {
  getDb,
  getConfig,
  upsertConfig,
  getTargets,
  upsertTarget,
  upsertTargetsBulk,
  calculateAndSave,
  getResults,
};
