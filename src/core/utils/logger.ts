import path from 'path';

// Universal, eye-catching emojis
const LOG_EMOJIS = {
  fatal: '🚨',
  error: '❌',
  warn: '⚠️',
  info: '🔍',
  debug: '🔬',
  trace: '🧭',
};

// Enhanced color palette
class ColorPalette {
  static RESET = '\x1b[0m';

  static colors = {
    fatal: { text: '\x1b[1;37;41m', bg: '\x1b[48;5;196m\x1b[30m' },
    error: { text: '\x1b[1;31m', bg: '\x1b[48;5;160m\x1b[37m' },
    warn: { text: '\x1b[1;33m', bg: '\x1b[48;5;214m\x1b[30m' },
    info: { text: '\x1b[1;36m', bg: '\x1b[48;5;39m\x1b[37m' },
    debug: { text: '\x1b[1;35m', bg: '\x1b[48;5;93m\x1b[37m' },
    trace: { text: '\x1b[1;32m', bg: '\x1b[48;5;82m\x1b[37m' },
  };

  static getColor(level: keyof typeof LOG_EMOJIS) {
    return this.colors[level]?.text || '';
  }
}

class AdvancedLogger {
  private logLevelPriority: { [key: string]: number } = {
    fatal: 1,
    error: 2,
    warn: 3,
    info: 4,
    debug: 5,
    trace: 6,
  };

  constructor(private logLevel: keyof typeof LOG_EMOJIS = 'info') {}

  private shouldLog(level: keyof typeof LOG_EMOJIS): boolean {
    return this.logLevelPriority[level] <= this.logLevelPriority[this.logLevel];
  }

  private formatLog(
    level: keyof typeof LOG_EMOJIS,
    message: string,
    ...args: any[]
  ): string {
    const timestamp = new Date().toISOString();
    const emoji = LOG_EMOJIS[level];
    const context = args.length ? JSON.stringify(args) : '';
    return `${timestamp} [${level.toUpperCase()}] ${emoji} ${message} ${context}`;
  }

  private log(level: keyof typeof LOG_EMOJIS, message: string, ...args: any[]) {
    if (!this.shouldLog(level)) return;

    const formattedLog = this.formatLog(level, message, ...args);
    const color = ColorPalette.getColor(level);
    console.log(`${color}${formattedLog}${ColorPalette.RESET}`);
  }

  fatal(message: string, ...args: any[]) {
    this.log('fatal', message, ...args);
  }

  error(message: string, ...args: any[]) {
    this.log('error', message, ...args);
  }

  warn(message: string, ...args: any[]) {
    this.log('warn', message, ...args);
  }

  info(message: string, ...args: any[]) {
    this.log('info', message, ...args);
  }

  debug(message: string, ...args: any[]) {
    this.log('debug', message, ...args);
  }

  trace(message: string, ...args: any[]) {
    this.log('trace', message, ...args);
  }

  setLogLevel(level: keyof typeof LOG_EMOJIS) {
    this.logLevel = level;
  }
}

// Initialize logger
const logger = new AdvancedLogger();

export default logger;
