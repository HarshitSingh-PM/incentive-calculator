const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST || '127.0.0.1',
  port: process.env.PG_PORT || 5432,
  database: process.env.PG_DATABASE || 'incentive_calculator',
  user: process.env.PG_USER || 'incentive',
  password: process.env.PG_PASSWORD || 'halacarly2026',
});

async function initTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS incentive_config (
      id SERIAL PRIMARY KEY,
      month TEXT NOT NULL UNIQUE,
      fixed_incentive NUMERIC NOT NULL DEFAULT 300,
      over_target_rate NUMERIC NOT NULL DEFAULT 500,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS agent_targets (
      id SERIAL PRIMARY KEY,
      agent_name TEXT NOT NULL,
      month TEXT NOT NULL,
      target INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(agent_name, month)
    );

    CREATE TABLE IF NOT EXISTS incentive_results (
      id SERIAL PRIMARY KEY,
      agent_name TEXT NOT NULL,
      month TEXT NOT NULL,
      target INTEGER NOT NULL,
      actual_sales INTEGER NOT NULL,
      target_met BOOLEAN NOT NULL DEFAULT FALSE,
      fixed_incentive NUMERIC NOT NULL DEFAULT 0,
      over_target_sales INTEGER NOT NULL DEFAULT 0,
      over_target_incentive NUMERIC NOT NULL DEFAULT 0,
      total_incentive NUMERIC NOT NULL DEFAULT 0,
      calculated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(agent_name, month)
    );
  `);
}

// Initialize tables on startup
initTables().catch(err => console.error('Failed to init tables:', err));

// --- Incentive Config ---

async function getConfig(month) {
  const { rows } = await pool.query('SELECT * FROM incentive_config WHERE month = $1', [month]);
  return rows[0] || null;
}

async function upsertConfig(month, fixedIncentive, overTargetRate) {
  await pool.query(`
    INSERT INTO incentive_config (month, fixed_incentive, over_target_rate)
    VALUES ($1, $2, $3)
    ON CONFLICT(month) DO UPDATE SET
      fixed_incentive = EXCLUDED.fixed_incentive,
      over_target_rate = EXCLUDED.over_target_rate,
      updated_at = NOW()
  `, [month, fixedIncentive, overTargetRate]);
  return getConfig(month);
}

// --- Agent Targets ---

async function getTargets(month) {
  const { rows } = await pool.query('SELECT * FROM agent_targets WHERE month = $1', [month]);
  return rows;
}

async function upsertTarget(agentName, month, target) {
  await pool.query(`
    INSERT INTO agent_targets (agent_name, month, target)
    VALUES ($1, $2, $3)
    ON CONFLICT(agent_name, month) DO UPDATE SET
      target = EXCLUDED.target,
      updated_at = NOW()
  `, [agentName, month, target]);
}

async function upsertTargetsBulk(month, targets) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const item of targets) {
      await client.query(`
        INSERT INTO agent_targets (agent_name, month, target)
        VALUES ($1, $2, $3)
        ON CONFLICT(agent_name, month) DO UPDATE SET
          target = EXCLUDED.target,
          updated_at = NOW()
      `, [item.agent_name, month, item.target]);
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
  return getTargets(month);
}

// --- Incentive Results ---

async function calculateAndSave(month, salesData, config, targets) {
  const targetMap = {};
  for (const t of targets) {
    targetMap[t.agent_name] = t.target;
  }

  const results = [];

  for (const agent of salesData) {
    const agentName = agent['Sales Agent'];
    const actualSales = agent['Count'];
    const target = targetMap[agentName] || 0;
    const targetMet = actualSales >= target && target > 0;
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

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const r of results) {
      await client.query(`
        INSERT INTO incentive_results (agent_name, month, target, actual_sales, target_met, fixed_incentive, over_target_sales, over_target_incentive, total_incentive)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT(agent_name, month) DO UPDATE SET
          target = EXCLUDED.target,
          actual_sales = EXCLUDED.actual_sales,
          target_met = EXCLUDED.target_met,
          fixed_incentive = EXCLUDED.fixed_incentive,
          over_target_sales = EXCLUDED.over_target_sales,
          over_target_incentive = EXCLUDED.over_target_incentive,
          total_incentive = EXCLUDED.total_incentive,
          calculated_at = NOW()
      `, [
        r.agent_name, r.month, r.target, r.actual_sales,
        r.target_met, r.fixed_incentive, r.over_target_sales,
        r.over_target_incentive, r.total_incentive
      ]);
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  return results;
}

async function getResults(month) {
  if (month) {
    const { rows } = await pool.query('SELECT * FROM incentive_results WHERE month = $1 ORDER BY total_incentive DESC', [month]);
    return rows;
  }
  const { rows } = await pool.query('SELECT * FROM incentive_results ORDER BY month DESC, total_incentive DESC');
  return rows;
}

module.exports = {
  getConfig,
  upsertConfig,
  getTargets,
  upsertTarget,
  upsertTargetsBulk,
  calculateAndSave,
  getResults,
};
