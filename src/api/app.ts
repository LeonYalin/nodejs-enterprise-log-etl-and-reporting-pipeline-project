import express, { type Express, type Request, type Response, type NextFunction } from 'express';
import type { ClickHouseClient } from '@clickhouse/client';
import { logger } from '../lib/logger.js';
import { registry } from '../lib/metrics.js';
import { createReportQueries } from './queries.js';
import { createReportsRouter } from './routes/reports.js';

export interface AppDeps {
  client: ClickHouseClient;
}

/**
 * Builds the Express app without binding a port, so tests can drive it in-process
 * over an ephemeral server (supertest) while the entrypoint listens for real.
 */
export function createApp({ client }: AppDeps): Express {
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
      await client.ping();
      res.json({ status: 'UP', timestamp: new Date().toISOString(), database: 'CONNECTED' });
    } catch (error) {
      logger.error({ error }, 'Health check failed');
      res.status(503).json({ status: 'DOWN', reason: 'Database connection failed' });
    }
  });

  // Mount specialized domain reporting endpoints
  app.use('/reports', createReportsRouter(createReportQueries(client)));

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

  return app;
}
