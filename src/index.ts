#!/usr/bin/env node
import { Command } from 'commander';
import dotenv from 'dotenv';
import  logger  from './core/utils/logger';
import { hostCommand } from './cli/host';
import { provideCommand } from './cli/provide';

// Load environment variables
dotenv.config();

const program = new Command();

program
  .name('nlg-dstorage')
  .description('Production-grade Distributed Storage (ECC + AES-GCM + Merkle Proofs)')
  .version('5.2.0');

program.addCommand(hostCommand());
program.addCommand(provideCommand());

program.parseAsync(process.argv).catch((err) => {
  logger.error('CLI Error:', err.message);
  process.exit(1);
});
