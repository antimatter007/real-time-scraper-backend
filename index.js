// backend/index.js

const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const cors = require('cors');

const app = express();

// Middleware Configuration
app.use(cors({
  origin: 'https://real-time-twitter-scraper-frontend.vercel.app', // Your frontend's production URL
  methods: ['GET', 'POST'],
  credentials: true,
}));
app.use(express.json());

// Hardcoded PostgreSQL Configuration
const pool = new Pool({
  host: 'autorack.proxy.rlwy.net',
  port: 20823,
  database: 'railway',
  user: 'postgres',
  password: 'suFzdtdvTXFdhgQloNbxzOHMjLsisThP',
  ssl: {
    rejectUnauthorized: false, // Set to true if you have proper SSL certificates
  },
});

pool.connect()
  .then(() => console.log('Backend connected to PostgreSQL'))
  .catch(err => console.error('Backend connection error:', err.stack));

// Hardcoded RabbitMQ Configuration
const RABBITMQ_URL = 'amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc';
const queueName = 'jobs_queue';

let channel;

// Function to Connect to RabbitMQ
async function connectRabbitMQ() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();
    await channel.assertQueue(queueName, { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('Failed to connect to RabbitMQ:', error);
    process.exit(1); // Exit if connection fails
  }
}

// Initialize RabbitMQ Connection
connectRabbitMQ();

// POST /api/jobs - Submit a New Scraping Job or Return Cached Results
app.post('/api/jobs', async (req, res) => {
  const { query } = req.body;

  if (!query) {
    return res.status(400).json({ error: 'Query parameter is required.' });
  }

  try {
    // Normalize the query for consistent caching (e.g., trim and lowercase)
    const normalizedQuery = query.trim().toLowerCase();

    // Check for a cached job: same query, status 'completed', within last 24 hours
    const cachedJobResult = await pool.query(
      `SELECT id FROM jobs 
       WHERE LOWER(query) = $1 AND status = $2 AND updated_at >= NOW() - INTERVAL '24 HOURS'
       ORDER BY updated_at DESC LIMIT 1`,
      [normalizedQuery, 'completed']
    );

    if (cachedJobResult.rowCount > 0) {
      const cachedJobId = cachedJobResult.rows[0].id;
      console.log(`Returning cached job ${cachedJobId} for query "${query}"`);

      // Fetch results for the cached job
      const results = await pool.query(
        'SELECT tweet_id, tweet_text, author_handle, timestamp FROM results WHERE job_id = $1',
        [cachedJobId]
      );

      const formattedResults = results.rows.map(r => ({
        tweet_id: r.tweet_id,
        tweet_text: r.tweet_text,
        author_handle: r.author_handle,
        timestamp: r.timestamp,
      }));

      // Return the cached results directly
      return res.status(200).json({
        jobId: cachedJobId,
        cached: true,
        status: 'completed',
        results: formattedResults,
      });
    }

    // If no cached data, proceed to create a new job
    const result = await pool.query(
      'INSERT INTO jobs (query, status) VALUES ($1, $2) RETURNING id',
      [query, 'pending']
    );
    const jobId = result.rows[0].id;

    // Send job to RabbitMQ queue
    const jobData = { jobId, query: normalizedQuery };
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(jobData)), { persistent: true });

    console.log(`Job ${jobId} submitted with query "${query}"`);

    res.status(201).json({ jobId, cached: false });
  } catch (error) {
    console.error('Error submitting job:', error);
    res.status(500).json({ error: 'Failed to submit job.' });
  }
});

// GET /api/jobs/:id - Get the Status and Results of a Job
app.get('/api/jobs/:id', async (req, res) => {
  const { id } = req.params;

  try {
    // Fetch job details from the 'jobs' table
    const jobResult = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);

    if (jobResult.rowCount === 0) {
      return res.status(404).json({ error: 'Job not found.' });
    }

    const job = jobResult.rows[0];

    if (job.status === 'completed') {
      // Fetch results from the 'results' table
      const results = await pool.query('SELECT tweet_id, tweet_text, author_handle, timestamp FROM results WHERE job_id = $1', [id]);
      const formattedResults = results.rows.map(r => ({
        tweet_id: r.tweet_id,
        tweet_text: r.tweet_text,
        author_handle: r.author_handle,
        timestamp: r.timestamp,
      }));

      res.json({
        status: job.status,
        results: formattedResults,
      });
    } else {
      res.json({ status: job.status });
    }
  } catch (error) {
    console.error('Error fetching job:', error);
    res.status(500).json({ error: 'Failed to fetch job status.' });
  }
});

app.get('/api/search-history', async (req, res) => {
  try {
    // Optional: Allow specifying the number of results via query parameters
    const limit = parseInt(req.query.limit, 10) || 10;

    // Fetch distinct queries ordered by most recent updated_at, limited by 'limit'
    const historyResult = await pool.query(`
      SELECT DISTINCT ON (LOWER(query)) query, updated_at
      FROM jobs
      WHERE status = 'completed'
      ORDER BY LOWER(query), updated_at DESC
      LIMIT $1
    `, [limit]);

    const history = historyResult.rows.map(row => row.query);

    res.json({ history });
  } catch (error) {
    console.error('Error fetching search history:', error);
    res.status(500).json({ error: 'Failed to fetch search history.' });
  }
});

// Start the Server on Port 3001
const PORT = 3001;
app.listen(PORT, () => {
  console.log(`Backend listening on port ${PORT}`);
});
