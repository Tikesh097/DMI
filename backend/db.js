import pg from 'pg'
import dotenv from 'dotenv'

dotenv.config()

const { Pool, Client } = pg

// Main connection pool for default database
const mainPool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
})

// Store for database-specific pools
const dbPools = new Map()

// Test database connection
mainPool.on('connect', () => {
  console.log('✅ Connected to NeonDB PostgreSQL database')
})

mainPool.on('error', (err) => {
  console.error('❌ Unexpected error on idle client', err)
})

// Get connection pool for specific schema (using same database, different schemas)
export const getPoolForDatabase = (schemaName) => {
  // For NeonDB, we use the main pool with schema search path
  return mainPool
}

// Create a new schema (NeonDB doesn't allow creating databases, use schemas instead)
export const createDatabase = async (schemaName) => {
  // Validate schema name (alphanumeric and underscore only)
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(schemaName)) {
    throw new Error('Invalid schema name. Use only alphanumeric characters and underscores, starting with a letter or underscore.')
  }

  if (schemaName.length > 63) {
    throw new Error('Schema name must be 63 characters or less.')
  }

  try {
    // Check if schema already exists
    const checkResult = await mainPool.query(
      `SELECT 1 FROM information_schema.schemata WHERE schema_name = $1`,
      [schemaName]
    )

    if (checkResult.rows.length > 0) {
      throw new Error(`Schema '${schemaName}' already exists.`)
    }

    // Create the schema - using parameterized identifier
    await mainPool.query(`CREATE SCHEMA "${schemaName}"`)

    return { success: true, message: `Schema '${schemaName}' created successfully.` }
  } catch (error) {
    console.error('Error creating schema:', error)
    throw error
  }
}

// Check if schema exists
export const databaseExists = async (schemaName) => {
  const result = await mainPool.query(
    `SELECT 1 FROM information_schema.schemata WHERE schema_name = $1`,
    [schemaName]
  )

  return result.rows.length > 0
}

// List all schemas (excluding system schemas)
export const listDatabases = async () => {
  const result = await mainPool.query(
    `SELECT
      schema_name as datname,
      0 as size
     FROM information_schema.schemata
     WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
     ORDER BY schema_name`
  )

  return result.rows
}

// Initialize tables in a specific schema
export const initDatabase = async (schemaName) => {
  try {
    // Create users table in the specified schema
    await mainPool.query(`
      CREATE TABLE IF NOT EXISTS "${schemaName}".users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        age INTEGER CHECK (age > 0 AND age < 150),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `)

    // Create index on email for faster lookups
    await mainPool.query(`
      CREATE INDEX IF NOT EXISTS idx_${schemaName}_users_email ON "${schemaName}".users(email)
    `)

    console.log(`✅ Schema '${schemaName}' initialized successfully`)
    return { success: true, message: `Schema '${schemaName}' initialized successfully` }
  } catch (error) {
    console.error(`❌ Error initializing schema '${schemaName}':`, error)
    throw error
  }
}

// Export schema and data
export const exportDatabase = async (sourceSchemaName) => {
  try {
    // Get all tables in the schema
    const tablesResult = await mainPool.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = $1
    `, [sourceSchemaName])

    const exportData = {
      database: sourceSchemaName,
      timestamp: new Date().toISOString(),
      tables: []
    }

    // For each table, export schema and data
    for (const row of tablesResult.rows) {
      const tableName = row.table_name

      // Get table schema
      const schemaResult = await mainPool.query(`
        SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `, [sourceSchemaName, tableName])

      // Get table data
      const dataResult = await mainPool.query(`SELECT * FROM "${sourceSchemaName}"."${tableName}"`)

      // Get constraints
      const constraintsResult = await mainPool.query(`
        SELECT constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE table_schema = $1 AND table_name = $2
      `, [sourceSchemaName, tableName])

      exportData.tables.push({
        name: tableName,
        schema: schemaResult.rows,
        data: dataResult.rows,
        constraints: constraintsResult.rows
      })
    }

    return exportData
  } catch (error) {
    console.error(`Error exporting schema '${sourceSchemaName}':`, error)
    throw error
  }
}

// Import schema from export data
export const importDatabase = async (targetSchemaName, exportData) => {
  const client = await mainPool.connect()

  try {
    await client.query('BEGIN')

    // Import each table
    for (const table of exportData.tables) {
      // Build CREATE TABLE statement from schema
      const columns = table.schema.map(col => {
        let def = `"${col.column_name}" ${col.data_type}`
        if (col.character_maximum_length) {
          def += `(${col.character_maximum_length})`
        }
        if (col.is_nullable === 'NO') {
          def += ' NOT NULL'
        }
        if (col.column_default) {
          def += ` DEFAULT ${col.column_default}`
        }
        return def
      }).join(', ')

      // Drop table if exists and create new in target schema
      await client.query(`DROP TABLE IF EXISTS "${targetSchemaName}"."${table.name}" CASCADE`)
      await client.query(`CREATE TABLE "${targetSchemaName}"."${table.name}" (${columns})`)

      // Insert data
      if (table.data && table.data.length > 0) {
        const columnNames = table.schema.map(col => `"${col.column_name}"`).join(', ')

        for (const row of table.data) {
          const values = table.schema.map(col => row[col.column_name])
          const placeholders = values.map((_, i) => `$${i + 1}`).join(', ')

          await client.query(
            `INSERT INTO "${targetSchemaName}"."${table.name}" (${columnNames}) VALUES (${placeholders})`,
            values
          )
        }
      }
    }

    await client.query('COMMIT')
    return { success: true, message: `Schema imported successfully to '${targetSchemaName}'` }
  } catch (error) {
    await client.query('ROLLBACK')
    console.error(`Error importing to schema '${targetSchemaName}':`, error)
    throw error
  } finally {
    client.release()
  }
}

// Migrate schema (export from source and import to target)
export const migrateDatabase = async (sourceSchemaName, targetSchemaName) => {
  try {
    // Export source schema
    const exportData = await exportDatabase(sourceSchemaName)

    // Import to target schema
    await importDatabase(targetSchemaName, exportData)

    return {
      success: true,
      message: `Successfully migrated from '${sourceSchemaName}' to '${targetSchemaName}'`,
      tablesCount: exportData.tables.length,
      timestamp: exportData.timestamp
    }
  } catch (error) {
    console.error(`Error migrating schema:`, error)
    throw error
  }
}

export default mainPool
