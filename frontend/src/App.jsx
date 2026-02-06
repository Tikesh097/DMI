import { useState, useEffect } from 'react'
import axios from 'axios'
import DatabaseManager from './components/DatabaseManager.jsx'
import UserForm from './components/UserForm.jsx'
import UserList from './components/UserList.jsx'
import './App.css'

function App() {
  const [currentDatabase, setCurrentDatabase] = useState(null)
  const [databases, setDatabases] = useState([])
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [success, setSuccess] = useState(null)
  const [editingUser, setEditingUser] = useState(null)

  useEffect(() => {
    fetchDatabases()
  }, [])

  useEffect(() => {
    if (currentDatabase) {
      fetchUsers()
    }
  }, [currentDatabase])

  const fetchDatabases = async () => {
    try {
      const response = await axios.get('/api/databases')
      setDatabases(response.data)
    } catch (err) {
      console.error('Error fetching databases:', err)
    }
  }

  const fetchUsers = async () => {
    if (!currentDatabase) return

    try {
      setLoading(true)
      setError(null)
      const response = await axios.get(`/api/databases/${currentDatabase}/users`)
      setUsers(response.data)
    } catch (err) {
      setError('Failed to fetch users')
      console.error('Error fetching users:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleDatabaseChange = (dbName) => {
    setCurrentDatabase(dbName)
    setUsers([])
    setEditingUser(null)
  }

  const handleCreateUser = async (userData) => {
    if (!currentDatabase) {
      setError('Please select a database first')
      return false
    }

    try {
      setError(null)
      const response = await axios.post(`/api/databases/${currentDatabase}/users`, userData)
      setUsers([response.data, ...users])
      setSuccess('User created successfully')
      setTimeout(() => setSuccess(null), 3000)
      return true
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to create user')
      console.error('Error creating user:', err)
      return false
    }
  }

  const handleUpdateUser = async (id, userData) => {
    if (!currentDatabase) return false

    try {
      setError(null)
      const response = await axios.put(`/api/databases/${currentDatabase}/users/${id}`, userData)
      setUsers(users.map(user => user.id === id ? response.data : user))
      setEditingUser(null)
      setSuccess('User updated successfully')
      setTimeout(() => setSuccess(null), 3000)
      return true
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to update user')
      console.error('Error updating user:', err)
      return false
    }
  }

  const handleDeleteUser = async (id) => {
    if (!currentDatabase) return

    if (!window.confirm('Are you sure you want to delete this user?')) {
      return
    }

    try {
      setError(null)
      await axios.delete(`/api/databases/${currentDatabase}/users/${id}`)
      setUsers(users.filter(user => user.id !== id))
      setSuccess('User deleted successfully')
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError('Failed to delete user')
      console.error('Error deleting user:', err)
    }
  }

  const handleEdit = (user) => {
    setEditingUser(user)
  }

  const handleCancelEdit = () => {
    setEditingUser(null)
  }

  return (
    <div className="app-container">
      <header className="app-header">
        <h1>React + Vite + NeonDB</h1>
        <p>Database Management & User System</p>
      </header>

      <main className="app-main">
        {error && (
          <div className="error-message">
            {error}
            <button onClick={() => setError(null)}>×</button>
          </div>
        )}

        {success && (
          <div className="success-message">
            {success}
            <button onClick={() => setSuccess(null)}>×</button>
          </div>
        )}

        <DatabaseManager
          databases={databases}
          currentDatabase={currentDatabase}
          onDatabaseChange={handleDatabaseChange}
          onDatabasesUpdate={fetchDatabases}
          setError={setError}
          setSuccess={setSuccess}
        />

        {currentDatabase && (
          <div className="content-grid">
            <section className="form-section">
              <h2>{editingUser ? 'Edit User' : 'Add New User'}</h2>
              <p className="current-db-label">Database: <strong>{currentDatabase}</strong></p>
              <UserForm
                onSubmit={editingUser ? handleUpdateUser : handleCreateUser}
                editingUser={editingUser}
                onCancel={handleCancelEdit}
              />
            </section>

            <section className="list-section">
              <h2>Users List</h2>
              {loading ? (
                <div className="loading">Loading users...</div>
              ) : (
                <UserList
                  users={users}
                  onDelete={handleDeleteUser}
                  onEdit={handleEdit}
                />
              )}
            </section>
          </div>
        )}

        {!currentDatabase && (
          <div className="empty-state-main">
            <h2>Get Started</h2>
            <p>Create a new database or select an existing one to begin managing users.</p>
          </div>
        )}
      </main>
    </div>
  )
}

export default App
