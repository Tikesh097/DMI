import express from 'express'
import cors from 'cors'
import dotenv from 'dotenv'
import mainPool, {
  createDatabase,
  databaseExists,
  listDatabases,
  initDatabase,
  exportDatabase,
  migrateDatabase,
  getPoolForDatabase
} from './db.js'
import {
  logDatabaseCreation,
  logDatabaseAccess,
  logDatabaseMigration,
  logUserOperation,
  readLogs
} from './logger.js'
import { authMiddleware, DEFAULT_API_KEY } from './auth.js'

dotenv.config()

const app = express()
const PORT = process.env.PORT || 5000

// Middleware
app.use(cors())
app.use(express.json())
app.use(express.urlencoded({ extended: true }))

// Request logging middleware
app.use((req, res, next) => {
  console.log(`${req.method} ${req.path}`)
  next()
})

// Apply authentication middleware (disabled for development)
// Uncomment to enable authentication
// app.use(authMiddleware)

// ==================== DATABASE MANAGEMENT ROUTES ====================

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'OK',
    message: 'Server is running',
    timestamp: new Date().toISOString(),
    apiKey: DEFAULT_API_KEY
  })
})

// List all databases
app.get('/api/databases', async (req, res) => {
  try {
    const databases = await listDatabases()
    logDatabaseAccess('all', req.userId || 'anonymous', true)
    res.json(databases)
  } catch (error) {
    console.error('Error listing databases:', error)
    logDatabaseAccess('all', req.userId || 'anonymous', false, error)
    res.status(500).json({
      error: 'Failed to list databases',
      message: error.message
    })
  }
})

// Create new schema (database)
app.post('/api/databases', async (req, res) => {
  try {
    const { name } = req.body

    if (!name) {
      return res.status(400).json({
        error: 'Schema name is required'
      })
    }

    const result = await createDatabase(name)
    logDatabaseCreation(name, req.userId || 'anonymous', true)

    // Initialize the schema with default tables
    await initDatabase(name)

    res.status(201).json({
      ...result,
      database: name
    })
  } catch (error) {
    console.error('Error creating schema:', error)
    logDatabaseCreation(req.body.name, req.userId || 'anonymous', false, error)
    res.status(400).json({
      error: 'Failed to create schema',
      message: error.message
    })
  }
})

// Check if database exists
app.get('/api/databases/:name/exists', async (req, res) => {
  try {
    const { name } = req.params
    const exists = await databaseExists(name)

    logDatabaseAccess(name, req.userId || 'anonymous', true)

    res.json({
      database: name,
      exists
    })
  } catch (error) {
    console.error('Error checking database:', error)
    logDatabaseAccess(req.params.name, req.userId || 'anonymous', false, error)
    res.status(500).json({
      error: 'Failed to check database',
      message: error.message
    })
  }
})

// Initialize schema tables
app.post('/api/databases/:name/initialize', async (req, res) => {
  try {
    const { name } = req.params

    const exists = await databaseExists(name)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found',
        message: `Schema '${name}' does not exist`
      })
    }

    const result = await initDatabase(name)
    logDatabaseAccess(name, req.userId || 'anonymous', true)

    res.json(result)
  } catch (error) {
    console.error('Error initializing schema:', error)
    res.status(500).json({
      error: 'Failed to initialize schema',
      message: error.message
    })
  }
})

// Export schema
app.get('/api/databases/:name/export', async (req, res) => {
  try {
    const { name } = req.params

    const exists = await databaseExists(name)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found',
        message: `Schema '${name}' does not exist`
      })
    }

    const exportData = await exportDatabase(name)
    logDatabaseAccess(name, req.userId || 'anonymous', true)

    res.json(exportData)
  } catch (error) {
    console.error('Error exporting schema:', error)
    res.status(500).json({
      error: 'Failed to export schema',
      message: error.message
    })
  }
})

// Migrate schema
app.post('/api/databases/migrate', async (req, res) => {
  try {
    const { sourceDatabase, targetDatabase } = req.body

    if (!sourceDatabase || !targetDatabase) {
      return res.status(400).json({
        error: 'Source and target schema names are required'
      })
    }

    // Check if source exists
    const sourceExists = await databaseExists(sourceDatabase)
    if (!sourceExists) {
      return res.status(404).json({
        error: 'Source schema not found',
        message: `Schema '${sourceDatabase}' does not exist`
      })
    }

    // Check if target exists
    const targetExists = await databaseExists(targetDatabase)
    if (!targetExists) {
      return res.status(404).json({
        error: 'Target schema not found',
        message: `Schema '${targetDatabase}' does not exist. Create it first.`
      })
    }

    const result = await migrateDatabase(sourceDatabase, targetDatabase)
    logDatabaseMigration(sourceDatabase, targetDatabase, req.userId || 'anonymous', true, result)

    res.json(result)
  } catch (error) {
    console.error('Error migrating schema:', error)
    logDatabaseMigration(req.body.sourceDatabase, req.body.targetDatabase, req.userId || 'anonymous', false, { error: error.message })
    res.status(500).json({
      error: 'Failed to migrate schema',
      message: error.message
    })
  }
})

// Get activity logs
app.get('/api/logs', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100
    const logs = readLogs(limit)

    res.json({
      logs,
      count: logs.length
    })
  } catch (error) {
    console.error('Error reading logs:', error)
    res.status(500).json({
      error: 'Failed to read logs',
      message: error.message
    })
  }
})

// ==================== USER MANAGEMENT ROUTES ====================

// Get all users from specific schema
app.get('/api/databases/:dbName/users', async (req, res) => {
  try {
    const { dbName } = req.params

    const exists = await databaseExists(dbName)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found'
      })
    }

    const pool = getPoolForDatabase(dbName)
    const result = await pool.query(
      `SELECT * FROM "${dbName}".users ORDER BY created_at DESC`
    )

    logUserOperation('list', { database: dbName, count: result.rows.length }, req.userId || 'anonymous')
    res.json(result.rows)
  } catch (error) {
    console.error('Error fetching users:', error)
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    })
  }
})

// Get single user by ID
app.get('/api/databases/:dbName/users/:id', async (req, res) => {
  try {
    const { dbName, id } = req.params

    const exists = await databaseExists(dbName)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found'
      })
    }

    const pool = getPoolForDatabase(dbName)
    const result = await pool.query(
      `SELECT * FROM "${dbName}".users WHERE id = $1`,
      [id]
    )

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' })
    }

    res.json(result.rows[0])
  } catch (error) {
    console.error('Error fetching user:', error)
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    })
  }
})

// Create new user
app.post('/api/databases/:dbName/users', async (req, res) => {
  try {
    const { dbName } = req.params
    const { name, email, age } = req.body

    // Validation
    if (!name || !email) {
      return res.status(400).json({
        error: 'Name and email are required'
      })
    }

    if (age && (age < 1 || age > 150)) {
      return res.status(400).json({
        error: 'Age must be between 1 and 150'
      })
    }

    const exists = await databaseExists(dbName)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found'
      })
    }

    const pool = getPoolForDatabase(dbName)
    const result = await pool.query(
      `INSERT INTO "${dbName}".users (name, email, age)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [name, email, age || null]
    )

    logUserOperation('create', { database: dbName, userId: result.rows[0].id, email }, req.userId || 'anonymous')
    res.status(201).json(result.rows[0])
  } catch (error) {
    console.error('Error creating user:', error)

    // Handle unique constraint violation
    if (error.code === '23505') {
      return res.status(400).json({
        error: 'Email already exists'
      })
    }

    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    })
  }
})

// Update user
app.put('/api/databases/:dbName/users/:id', async (req, res) => {
  try {
    const { dbName, id } = req.params
    const { name, email, age } = req.body

    // Validation
    if (!name || !email) {
      return res.status(400).json({
        error: 'Name and email are required'
      })
    }

    if (age && (age < 1 || age > 150)) {
      return res.status(400).json({
        error: 'Age must be between 1 and 150'
      })
    }

    const exists = await databaseExists(dbName)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found'
      })
    }

    const pool = getPoolForDatabase(dbName)
    const result = await pool.query(
      `UPDATE "${dbName}".users
       SET name = $1, email = $2, age = $3, updated_at = CURRENT_TIMESTAMP
       WHERE id = $4
       RETURNING *`,
      [name, email, age || null, id]
    )

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' })
    }

    logUserOperation('update', { database: dbName, userId: id, email }, req.userId || 'anonymous')
    res.json(result.rows[0])
  } catch (error) {
    console.error('Error updating user:', error)

    // Handle unique constraint violation
    if (error.code === '23505') {
      return res.status(400).json({
        error: 'Email already exists'
      })
    }

    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    })
  }
})

// Delete user
app.delete('/api/databases/:dbName/users/:id', async (req, res) => {
  try {
    const { dbName, id } = req.params

    const exists = await databaseExists(dbName)
    if (!exists) {
      return res.status(404).json({
        error: 'Schema not found'
      })
    }

    const pool = getPoolForDatabase(dbName)
    const result = await pool.query(
      `DELETE FROM "${dbName}".users WHERE id = $1 RETURNING *`,
      [id]
    )

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found' })
    }

    logUserOperation('delete', { database: dbName, userId: id }, req.userId || 'anonymous')
    res.json({
      message: 'User deleted successfully',
      user: result.rows[0]
    })
  } catch (error) {
    console.error('Error deleting user:', error)
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    })
  }
})

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Route not found' })
})

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err)
  res.status(500).json({
    error: 'Something went wrong!',
    message: err.message
  })
})

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server is running on http://localhost:${PORT}`)
  console.log(`📊 API endpoints available at http://localhost:${PORT}/api`)
  console.log(`🔑 Default API Key: ${DEFAULT_API_KEY}`)
})
