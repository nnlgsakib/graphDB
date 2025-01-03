import { Command } from 'commander';
import { HostServer } from '../core/network/HostServer';
import { getAvailablePort } from '../core/utils/portutils';
import  logger  from '../core/utils/logger';

export function hostCommand() {
  const cmd = new Command('host');
  cmd
    .description('Run as the main node (host) which providers connect to.')
    .option('--boot-port <number>', 'WebSocket port for providers (default=3000)', '3000')
    .option('--api-port <number>', 'REST API port for data ops (default=3001)', '3001')
    .option('--db-dir <path>', 'Path for LevelDB data (default=./db-host)', './db-host')
    .action(async (opts) => {
      let bootPort = parseInt(opts.bootPort, 10);
      let apiPort = parseInt(opts.apiPort, 10);

      if (Number.isNaN(bootPort)) {
        bootPort = await getAvailablePort(3000);
      }
      if (Number.isNaN(apiPort)) {
        apiPort = await getAvailablePort(3001);
      }

      const server = new HostServer(bootPort, apiPort, opts.dbDir);
      await server.start();

      logger.info(`Host started (bootPort=${bootPort}, apiPort=${apiPort}, dbDir=${opts.dbDir})`);
    });

  return cmd;
}
