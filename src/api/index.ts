import express, { Request, Response, NextFunction } from 'express';
import { config } from '../config/index.js';
import { logger } from '../lib/logger.js';
import { reportsRouter } from './routes/reports.js';
import { clickhouseClient } from '../lib/clickhouse.js';
import { registry } from '../lib/metrics.js';

const app = express();

app.use(express.json());

// Serve the static frontend assets from public/ folder
app.use(express.static('public'));

// Prometheus scraper target endpoint
app.get('/metrics', async (_req, res) => {
  try {
    res.set('Content-Type', registry.contentType);
    res.end(await registry.metrics());
  } catch (err) {
    logger.error({ err }, 'Failed to collect metrics');
    res.status(500).end(String(err));
  }
});

// Deep infrastructure health probe
app.get('/health', async (_req, res) => {
  try {
    await clickhouseClient.ping();
    res.json({ status: 'UP', timestamp: new Date().toISOString(), database: 'CONNECTED' });
  } catch (error) {
    logger.error({ error }, 'Health check failed');
    res.status(503).json({ status: 'DOWN', reason: 'Database connection failed' });
  }
});

// Mount specialized domain reporting endpoints
app.use('/reports', reportsRouter);

// Centralized JSON Error Handler Middleware
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error({ err }, 'Express route execution error caught');
  res.status(500).json({
    success: false,
    error: {
      message: err.message || 'Internal server error occurred',
      timestamp: new Date().toISOString()
    }
  });
});

app.listen(config.API_PORT, () => {
  logger.info(`Reporting API & UI Dashboard server operating on port ${config.API_PORT}`);
});
