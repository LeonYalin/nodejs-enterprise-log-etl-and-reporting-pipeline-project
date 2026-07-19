import http from "node:http";
import { registry } from "./metrics.js";
import { logger } from "./logger.js";

export function startMetricsServer(port: number) {
  const server = http.createServer((req, res) => {
    if (req.url === "/metrics") {
      registry
        .metrics()
        .then(body => {
          res.writeHead(200, { "Content-Type": registry.contentType });
          res.end(body);
        })
        .catch(err => {
          res.writeHead(500);
          res.end(String(err));
        });
      return;
    }
    res.writeHead(404);
    res.end();
  });

  server.listen(port, () => {
    logger.info({ port }, "metrics server listening");
  });

  return server;
}
