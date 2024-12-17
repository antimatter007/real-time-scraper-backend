// backend/index.js

const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const cors = require('cors');

const app = express();

// Middleware
app.use(cors({
  origin: 'https://real-time-twitter-scraper-frontend.vercel.app', // Replace with your actual frontend URL
  methods: ['GET', 'POST'],
  credentials: true,
}));
app.use(express.json());

// Production PostgreSQL Database URL from Railway
const DATABASE_URL = 'postgresql://postgres:fzBKMaLxqMFZKWLXEnnAoqSwUAMslaMm@autorack.proxy.rlwy.net:29248/railway';

// Configure the PostgreSQL pool with production credentials
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false, // Required if Railway PostgreSQL uses self-signed certificates
  },
});

// Production RabbitMQ URL from CloudAMQP
const RABBITMQ_URL = 'amqps://pcudcyxc:CT6kMcrw_pXH7kFpqzpqWgoWnu5J04LU@duck.lmq.cloudamqp.com/pcudcyxc';

let channel;
const queueName = 'jobs_queue';

// Function to connect to RabbitMQ
async function connectRabbitMQ() {
  try {
    const conn = await amqp.connect(RABBITMQ_URL);
    channel = await conn.createChannel();
    await channel.assertQueue(queueName, { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('Failed to connect to RabbitMQ:', error);
    process.exit(1); // Exit process if connection fails
  }
}

// Initialize RabbitMQ connection
connectRabbitMQ();

// POST /api/jobs - Submit a new scraping job
app.post('/api/jobs', async (req, res) => {
  const { query } = req.body;

  if (!query) {
    return res.status(400).json({ error: 'Query parameter is required.' });
  }

  try {
    // Insert new job into the jobs table with status 'pending'
    const result = await pool.query(
      'INSERT INTO jobs (query, status) VALUES ($1, $2) RETURNING id',
      [query, 'pending']
    );
    const jobId = result.rows[0].id;

    // Send job to RabbitMQ queue
    const jobData = { jobId, query };
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(jobData)), { persistent: true });

    console.log(`Job ${jobId} submitted with query "${query}"`);

    res.status(201).json({ jobId });
  } catch (error) {
    console.error('Error submitting job:', error);
    res.status(500).json({ error: 'Failed to submit job.' });
  }
});

// GET /api/jobs/:id - Get the status and results of a job
app.get('/api/jobs/:id', async (req, res) => {
  const { id } = req.params;

  try {
    // Fetch job details from the jobs table
    const jobResult = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);

    if (jobResult.rowCount === 0) {
      return res.status(404).json({ error: 'Job not found.' });
    }

    const job = jobResult.rows[0];

    if (job.status === 'completed') {
      // Fetch results from the results table
      const results = await pool.query('SELECT * FROM results WHERE job_id = $1', [id]);
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

// Start the server on port 3001
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Backend listening on port ${PORT}`);
});
