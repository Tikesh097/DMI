import fs from 'fs'
import path from 'path'

const LOG_FILE = path.join(process.cwd(), 'database-operations.log')

// Log levels
export const LOG_LEVELS = {
  INFO: 'INFO',
  WARNING: 'WARNING',
  ERROR: 'ERROR',
  SUCCESS: 'SUCCESS'
}

// Write log entry to file
const writeLog = (level, action, details, userId = 'system') => {
  const timestamp = new Date().toISOString()
  const logEntry = {
    timestamp,
    level,
    userId,
    action,
    details
  }

  const logLine = JSON.stringify(logEntry) + '\n'

  try {
    fs.appendFileSync(LOG_FILE, logLine)
  } catch (error) {
    console.error('Failed to write to log file:', error)
  }

  // Also log to console
  const emoji = {
    INFO: 'ℹ️',
    WARNING: '⚠️',
    ERROR: '❌',
    SUCCESS: '✅'
  }

  console.log(`${emoji[level]} [${timestamp}] [${userId}] ${action}: ${JSON.stringify(details)}`)
}

// Log database creation
export const logDatabaseCreation = (dbName, userId, success, error = null) => {
  if (success) {
    writeLog(LOG_LEVELS.SUCCESS, 'DATABASE_CREATED', { database: dbName }, userId)
  } else {
    writeLog(LOG_LEVELS.ERROR, 'DATABASE_CREATION_FAILED', { database: dbName, error: error?.message }, userId)
  }
}

// Log database access
export const logDatabaseAccess = (dbName, userId, success, error = null) => {
  if (success) {
    writeLog(LOG_LEVELS.INFO, 'DATABASE_ACCESSED', { database: dbName }, userId)
  } else {
    writeLog(LOG_LEVELS.ERROR, 'DATABASE_ACCESS_FAILED', { database: dbName, error: error?.message }, userId)
  }
}

// Log database migration
export const logDatabaseMigration = (sourceDb, targetDb, userId, success, details = {}) => {
  if (success) {
    writeLog(LOG_LEVELS.SUCCESS, 'DATABASE_MIGRATED', { source: sourceDb, target: targetDb, ...details }, userId)
  } else {
    writeLog(LOG_LEVELS.ERROR, 'DATABASE_MIGRATION_FAILED', { source: sourceDb, target: targetDb, error: details.error }, userId)
  }
}

// Log user operations
export const logUserOperation = (operation, details, userId = 'system') => {
  writeLog(LOG_LEVELS.INFO, `USER_${operation.toUpperCase()}`, details, userId)
}

// Read logs (for admin interface)
export const readLogs = (limit = 100) => {
  try {
    if (!fs.existsSync(LOG_FILE)) {
      return []
    }

    const content = fs.readFileSync(LOG_FILE, 'utf-8')
    const lines = content.trim().split('\n').filter(line => line)

    const logs = lines.map(line => {
      try {
        return JSON.parse(line)
      } catch {
        return null
      }
    }).filter(log => log !== null)

    return logs.slice(-limit).reverse()
  } catch (error) {
    console.error('Failed to read log file:', error)
    return []
  }
}

export default {
  logDatabaseCreation,
  logDatabaseAccess,
  logDatabaseMigration,
  logUserOperation,
  readLogs,
  LOG_LEVELS
}
