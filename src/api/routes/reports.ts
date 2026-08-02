import { Router } from 'express';
import type { ReportQueries } from '../queries.js';

// Express 5 natively forwards rejected promises from async handlers to the
// error middleware, so no try/catch or wrapper is needed here.

export function createReportsRouter(reportQueries: ReportQueries): Router {
  const reportsRouter = Router();

  reportsRouter.get('/throughput', async (req, res) => {
    const minutes = req.query.minutes ? Number(req.query.minutes) : undefined;
    const data = await reportQueries.getThroughput({ minutes });
    res.json({ success: true, data });
  });

  reportsRouter.get('/errors-by-service', async (req, res) => {
    const minutes = req.query.minutes ? Number(req.query.minutes) : undefined;
    const data = await reportQueries.getErrorsByService({ minutes });
    res.json({ success: true, data });
  });

  reportsRouter.get('/latency-percentiles', async (req, res) => {
    const minutes = req.query.minutes ? Number(req.query.minutes) : undefined;
    const data = await reportQueries.getLatencyPercentiles({ minutes });
    res.json({ success: true, data });
  });

  reportsRouter.get('/top-services', async (req, res) => {
    const minutes = req.query.minutes ? Number(req.query.minutes) : undefined;
    const data = await reportQueries.getTopServices({ minutes });
    res.json({ success: true, data });
  });

  return reportsRouter;
}
