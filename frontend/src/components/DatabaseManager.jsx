import { useState } from 'react'
import axios from 'axios'

function DatabaseManager({ databases, currentDatabase, onDatabaseChange, onDatabasesUpdate, setError, setSuccess }) {
  const [newDbName, setNewDbName] = useState('')
  const [existingDbName, setExistingDbName] = useState('')
  const [sourceDb, setSourceDb] = useState('')
  const [targetDb, setTargetDb] = useState('')
  const [creating, setCreating] = useState(false)
  const [checking, setChecking] = useState(false)
  const [migrating, setMigrating] = useState(false)
  const [migrationProgress, setMigrationProgress] = useState(null)

  const handleCreateDatabase = async (e) => {
    e.preventDefault()

    if (!newDbName.trim()) {
      setError('Database name is required')
      return
    }

    setCreating(true)
    try {
      const response = await axios.post('/api/databases', { name: newDbName })
      setSuccess(response.data.message)
      setNewDbName('')
      onDatabasesUpdate()
      onDatabaseChange(newDbName)
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to create database')
    } finally {
      setCreating(false)
    }
  }

  const handleCheckDatabase = async (e) => {
    e.preventDefault()

    if (!existingDbName.trim()) {
      setError('Database name is required')
      return
    }

    setChecking(true)
    try {
      const response = await axios.get(`/api/databases/${existingDbName}/exists`)

      if (response.data.exists) {
        setSuccess(`Database '${existingDbName}' found and selected!`)
        onDatabaseChange(existingDbName)
        setExistingDbName('')
      } else {
        setError(`Database '${existingDbName}' does not exist. Please create it first.`)
      }
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to check database')
    } finally {
      setChecking(false)
    }
  }

  const handleMigrateDatabase = async (e) => {
    e.preventDefault()

    if (!sourceDb.trim() || !targetDb.trim()) {
      setError('Both source and target database names are required')
      return
    }

    if (sourceDb === targetDb) {
      setError('Source and target databases cannot be the same')
      return
    }

    setMigrating(true)
    setMigrationProgress('Exporting source database...')

    try {
      const response = await axios.post('/api/databases/migrate', {
        sourceDatabase: sourceDb,
        targetDatabase: targetDb
      })

      setSuccess(`Migration successful! Migrated ${response.data.tablesCount} tables from '${sourceDb}' to '${targetDb}'`)
      setSourceDb('')
      setTargetDb('')
      setMigrationProgress(null)
      onDatabasesUpdate()
    } catch (err) {
      setError(err.response?.data?.message || 'Failed to migrate database')
      setMigrationProgress(null)
    } finally {
      setMigrating(false)
    }
  }

  return (
    <div className="database-manager">
      <h2>Database Management</h2>

      <div className="db-controls">
        {/* Create New Database */}
        <div className="db-control-card">
          <h3>Create New Database</h3>
          <form onSubmit={handleCreateDatabase} className="db-form">
            <input
              type="text"
              value={newDbName}
              onChange={(e) => setNewDbName(e.target.value)}
              placeholder="Enter database name"
              disabled={creating}
            />
            <button type="submit" disabled={creating} className="btn btn-primary">
              {creating ? 'Creating...' : 'Create Database'}
            </button>
          </form>
          <p className="help-text">Use alphanumeric characters and underscores only</p>
        </div>

        {/* Select Existing Database */}
        <div className="db-control-card">
          <h3>Select Existing Database</h3>
          <form onSubmit={handleCheckDatabase} className="db-form">
            <input
              type="text"
              value={existingDbName}
              onChange={(e) => setExistingDbName(e.target.value)}
              placeholder="Enter database name"
              disabled={checking}
            />
            <button type="submit" disabled={checking} className="btn btn-secondary">
              {checking ? 'Checking...' : 'Select Database'}
            </button>
          </form>
          <p className="help-text">Or select from the list below</p>
        </div>

        {/* Database Migration */}
        <div className="db-control-card full-width">
          <h3>Migrate Database</h3>
          <form onSubmit={handleMigrateDatabase} className="db-form migration-form">
            <input
              type="text"
              value={sourceDb}
              onChange={(e) => setSourceDb(e.target.value)}
              placeholder="Source database"
              disabled={migrating}
            />
            <span className="arrow">→</span>
            <input
              type="text"
              value={targetDb}
              onChange={(e) => setTargetDb(e.target.value)}
              placeholder="Target database"
              disabled={migrating}
            />
            <button type="submit" disabled={migrating} className="btn btn-migrate">
              {migrating ? 'Migrating...' : 'Migrate'}
            </button>
          </form>
          {migrationProgress && (
            <div className="migration-progress">{migrationProgress}</div>
          )}
          <p className="help-text">Copy all tables and data from source to target database</p>
        </div>
      </div>

      {/* Database List */}
      {databases.length > 0 && (
        <div className="database-list">
          <h3>Available Databases</h3>
          <div className="db-grid">
            {databases.map((db) => (
              <div
                key={db.datname}
                className={`db-card ${currentDatabase === db.datname ? 'active' : ''}`}
                onClick={() => onDatabaseChange(db.datname)}
              >
                <div className="db-name">{db.datname}</div>
                <div className="db-size">
                  {(parseInt(db.size) / 1024 / 1024).toFixed(2)} MB
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default DatabaseManager
