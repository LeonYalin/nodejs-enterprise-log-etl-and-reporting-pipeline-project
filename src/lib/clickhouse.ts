import { createClient } from "@clickhouse/client";
import { config } from "../config/index.js";

export const clickhouseClient = createClient({
  url: config.CLICKHOUSE_URL,
  database: config.CLICKHOUSE_DB,
  username: config.CLICKHOUSE_USER,
  password: config.CLICKHOUSE_PASSWORD,
  // Optimize for analytical stream uploads
  clickhouse_settings: {
    async_insert: 1,
    wait_for_async_insert: 0,
  }
});
