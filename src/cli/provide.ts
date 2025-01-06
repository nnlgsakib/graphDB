import { Command } from 'commander';
import { ProviderClient } from '../core/network/ProviderClient';
import  logger  from '../core/utils/logger';

export function provideCommand() {
  const cmd = new Command('provide');
  cmd
    .description('Run as a provider node storing data & responding to chunk proofs.')
    .option('--boot-host <string>', 'Host server address (default=localhost)', 'localhost')
    .option('--boot-port <number>', 'Host server WebSocket port (default=3000)', '3000')
    .option('--datadir <path>', 'Path for LevelDB data (default=./db-provider)', './db-provider')
    .option('--name <string>', 'Provider name (default=alice)', 'alice')  
    .action(async (opts) => {
      const bootHost = opts.bootHost;
      const bootPort = parseInt(opts.bootPort, 10);
      const dbDir = opts.datadir;
      const providerName = opts.name;   // Added option for provider name

      const provider = new ProviderClient(bootHost, bootPort, dbDir, providerName);
      await provider.start();

      logger.info(`Provider started (host=${bootHost}, port=${bootPort}, dbDir=${dbDir})`);
    });

  return cmd;
}
