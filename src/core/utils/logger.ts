import path from 'path';
import fs from 'fs';
import zlib from 'zlib';

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
    fatal: { text: '\x1b[1;37;41m', bg: '\x1b[48;5;196m\x1b[30m' }, // Bright red background with white text
    error: { text: '\x1b[1;31m', bg: '\x1b[48;5;160m\x1b[37m' }, // Red background with white text
    warn: { text: '\x1b[1;33m', bg: '\x1b[48;5;214m\x1b[30m' }, // Orange background with black text
    info: { text: '\x1b[1;36m', bg: '\x1b[48;5;39m\x1b[37m' }, // Cyan background with white text
    debug: { text: '\x1b[1;35m', bg: '\x1b[48;5;93m\x1b[37m' }, // Purple background with white text
    trace: { text: '\x1b[1;32m', bg: '\x1b[48;5;82m\x1b[37m' }, // Green background with white text
  };

  static getColor(level: keyof typeof LOG_EMOJIS) {
    return this.colors[level]?.text || '';
  }
}

// Log rotation with compression
class LogRotator {
  constructor(private logsDir: string, private maxLogFiles = 5) {
    fs.mkdirSync(logsDir, { recursive: true });
    this.rotateLogs();
  }

  private rotateLogs() {
    const files = fs
      .readdirSync(this.logsDir)
      .filter((file) => file.endsWith('.log'))
      .map((file) => ({
        name: file,
        path: path.join(this.logsDir, file),
        birthtime: fs.statSync(path.join(this.logsDir, file)).birthtime,
      }))
      .sort((a, b) => a.birthtime.getTime() - b.birthtime.getTime());

    while (files.length >= this.maxLogFiles) {
      const oldest = files.shift();
      if (oldest) {
        const compressedPath = `${oldest.path}.gz`;
        const fileContents = fs.readFileSync(oldest.path);
        fs.writeFileSync(compressedPath, zlib.gzipSync(fileContents));
        fs.unlinkSync(oldest.path);
      }
    }
  }

  getNewLogFilePath(): string {
    return path.join(this.logsDir, `app-${new Date().toISOString().replace(/:/g, '-')}.log`);
  }
}

class AdvancedLogger {
  private writeStream: fs.WriteStream;
  private logLevelPriority: { [key: string]: number } = {
    fatal: 1,
    error: 2,
    warn: 3,
    info: 4,
    debug: 5,
    trace: 6,
  };

  constructor(
    private logFile: string,
    private logLevel: keyof typeof LOG_EMOJIS = 'info',
    private bufferSize = 1024
  ) {
    this.writeStream = fs.createWriteStream(logFile, { flags: 'a' });
  }

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

    // Write to log file with buffering
    if (this.writeStream.writableLength >= this.bufferSize) {
      this.writeStream.write(formattedLog + '\n');
    } else {
      fs.appendFileSync(this.logFile, formattedLog + '\n');
    }
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
const logsDir = path.join(process.cwd(), 'logs');
const rotator = new LogRotator(logsDir);
const logFilePath = rotator.getNewLogFilePath();
const logger = new AdvancedLogger(logFilePath);

export default logger;
