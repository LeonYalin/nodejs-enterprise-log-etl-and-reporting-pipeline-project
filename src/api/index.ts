import { pathToFileURL } from 'node:url';
import { config } from '../config/index.js';
import { logger } from '../lib/logger.js';
import { clickhouseClient } from '../lib/clickhouse.js';
import { createApp } from './app.js';

// Only self-start when executed directly, so importing this module (e.g. from a
// test) has no side effects.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const app = createApp({ client: clickhouseClient });

  app.listen(config.API_PORT, () => {
    logger.info(`Reporting API & UI Dashboard server operating on port ${config.API_PORT}`);
  });
}
