import { createClient } from '@clickhouse/client';
import { clickhouseClient } from '../src/lib/clickhouse.js';
import { config } from '../src/config/index.js';
import { logger } from '../src/lib/logger.js';
import { applyInitScripts } from '../src/lib/schema-init.js';

const bootstrapClient = createClient({
  url: config.CLICKHOUSE_URL,
  username: config.CLICKHOUSE_USER,
  password: config.CLICKHOUSE_PASSWORD,
});

applyInitScripts({
  client: clickhouseClient,
  bootstrapClient,
  database: config.CLICKHOUSE_DB,
})
  .then(async () => {
    await bootstrapClient.close();
    await clickhouseClient.close();
    process.exit(0);
  })
  .catch(async (err) => {
    logger.error({ err }, 'ClickHouse schema initialization failed');
    await bootstrapClient.close();
    await clickhouseClient.close();
    process.exit(1);
  });
