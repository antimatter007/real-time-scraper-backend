const express = require('express');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const amqp = require('amqplib');
const cors = require('cors');

const app = express();

app.use(cors()); // Ensure CORS is enabled
app.use(bodyParser.json());

// Connect to DB
const pool = new Pool({
  host: 'localhost',
  database: 'postgres',
  user: 'postgres',
  password: 'mysecretpassword',
  port: 5432
});

let channel;
const queueName = 'jobs_queue';

async function connectRabbitMQ() {
  const conn = await amqp.connect('amqp://localhost');
  channel = await conn.createChannel();
  await channel.assertQueue(queueName, { durable: true });
}

connectRabbitMQ().catch(console.error);

// POST /api/jobs
app.post('/api/jobs', async (req, res) => {
  const { query } = req.body;

  const result = await pool.query(
    'INSERT INTO jobs (query, status) VALUES ($1, $2) RETURNING id',
    [query, 'pending']
  );
  const jobId = result.rows[0].id;

  channel.sendToQueue(queueName, Buffer.from(JSON.stringify({ jobId, query })), { persistent: true });

  res.json({ jobId });
});

// GET /api/jobs/:id
app.get('/api/jobs/:id', async (req, res) => {
  const { id } = req.params;
  const jobResult = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  if (jobResult.rowCount === 0) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const job = jobResult.rows[0];
  if (job.status === 'completed') {
    const results = await pool.query('SELECT * FROM results WHERE job_id = $1', [id]);
    res.json({
      status: job.status,
      results: results.rows.map(r => ({
        tweet_id: r.tweet_id,
        tweet_text: r.tweet_text,
        author_handle: r.author_handle,
        timestamp: r.timestamp
      }))
    });
  } else {
    res.json({ status: job.status });
  }
});

app.listen(3001, () => console.log('Backend listening on port 3001'));
