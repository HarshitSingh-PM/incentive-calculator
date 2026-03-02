const express = require('express');
const crypto = require('crypto');
const path = require('path');
const db = require('./database');

const app = express();
const PORT = process.env.PORT || 3456;

const METABASE_BASE = 'http://13.53.103.159/metabase';
const METABASE_API_KEY = 'mb_kJbDzdVD7ruWV+s+qIRFZdS6lvJ8VEFs7AzV0NjBEq4=';

const AUTH_USER = 'harshit';
const AUTH_PASS = 'halacarly@123';

// Active tokens (in-memory, cleared on restart)
const tokens = new Set();

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// --- Auth ---

app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  if (username === AUTH_USER && password === AUTH_PASS) {
    const token = crypto.randomBytes(32).toString('hex');
    tokens.add(token);
    return res.json({ token });
  }
  res.status(401).json({ error: 'Invalid username or password' });
});

app.post('/api/logout', (req, res) => {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (token) tokens.delete(token);
  res.json({ ok: true });
});

// Auth middleware — protects all /api/* routes except login
function requireAuth(req, res, next) {
  if (req.path === '/api/login') return next();
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token || !tokens.has(token)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

app.use('/api', requireAuth);

// --- Metabase Proxy ---

async function fetchMetabase(cardId) {
  const res = await fetch(`${METABASE_BASE}/api/card/${cardId}/query/json`, {
    method: 'POST',
    headers: {
      'x-api-key': METABASE_API_KEY,
      'Content-Type': 'application/json',
    },
    body: '{}',
  });
  if (!res.ok) {
    throw new Error(`Metabase API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

app.get('/api/sales/:month?', async (req, res) => {
  try {
    const data = await fetchMetabase(140);
    if (req.params.month) {
      const filtered = data.filter(
        (r) => r['Deliveries - DeliveryId → CreatedAt: Month'].startsWith(req.params.month)
      );
      return res.json(filtered);
    }
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/agents', async (req, res) => {
  try {
    const data = await fetchMetabase(141);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Config ---

app.get('/api/config/:month', async (req, res) => {
  try {
    const config = await db.getConfig(req.params.month);
    res.json(config || { month: req.params.month, fixed_incentive: 300, over_target_rate: 500 });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/config', async (req, res) => {
  try {
    const { month, fixed_incentive, over_target_rate } = req.body;
    const config = await db.upsertConfig(month, fixed_incentive, over_target_rate);
    res.json(config);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Targets ---

app.get('/api/targets/:month', async (req, res) => {
  try {
    const targets = await db.getTargets(req.params.month);
    res.json(targets);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/targets', async (req, res) => {
  try {
    const { month, targets } = req.body;
    const result = await db.upsertTargetsBulk(month, targets);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Calculate ---

app.post('/api/calculate', async (req, res) => {
  try {
    const { month } = req.body;

    const salesData = await fetchMetabase(140);
    const monthlySales = salesData.filter(
      (r) => r['Deliveries - DeliveryId → CreatedAt: Month'].startsWith(month)
    );

    const config = await db.getConfig(month);
    if (!config) {
      return res.status(400).json({ error: 'Please save incentive config for this month first.' });
    }

    const targets = await db.getTargets(month);
    if (targets.length === 0) {
      return res.status(400).json({ error: 'Please set agent targets for this month first.' });
    }

    const results = await db.calculateAndSave(month, monthlySales, config, targets);
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Results ---

app.get('/api/results/:month?', async (req, res) => {
  try {
    const results = await db.getResults(req.params.month);
    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Incentive Calculator running at http://localhost:${PORT}`);
});
