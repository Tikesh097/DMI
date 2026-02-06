import { useState, useEffect } from 'react'

function UserForm({ onSubmit, editingUser, onCancel }) {
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    age: ''
  })

  useEffect(() => {
    if (editingUser) {
      setFormData({
        name: editingUser.name || '',
        email: editingUser.email || '',
        age: editingUser.age || ''
      })
    } else {
      setFormData({ name: '', email: '', age: '' })
    }
  }, [editingUser])

  const handleChange = (e) => {
    const { name, value } = e.target
    setFormData(prev => ({
      ...prev,
      [name]: value
    }))
  }

  const handleSubmit = async (e) => {
    e.preventDefault()

    const userData = {
      name: formData.name,
      email: formData.email,
      age: formData.age ? parseInt(formData.age) : null
    }

    const success = editingUser
      ? await onSubmit(editingUser.id, userData)
      : await onSubmit(userData)

    if (success) {
      setFormData({ name: '', email: '', age: '' })
    }
  }

  return (
    <form onSubmit={handleSubmit} className="user-form">
      <div className="form-group">
        <label htmlFor="name">Name *</label>
        <input
          type="text"
          id="name"
          name="name"
          value={formData.name}
          onChange={handleChange}
          placeholder="Enter name"
          required
        />
      </div>

      <div className="form-group">
        <label htmlFor="email">Email *</label>
        <input
          type="email"
          id="email"
          name="email"
          value={formData.email}
          onChange={handleChange}
          placeholder="Enter email"
          required
        />
      </div>

      <div className="form-group">
        <label htmlFor="age">Age</label>
        <input
          type="number"
          id="age"
          name="age"
          value={formData.age}
          onChange={handleChange}
          placeholder="Enter age"
          min="1"
          max="150"
        />
      </div>

      <div className="form-actions">
        <button type="submit" className="btn btn-primary">
          {editingUser ? 'Update User' : 'Add User'}
        </button>
        {editingUser && (
          <button type="button" onClick={onCancel} className="btn btn-secondary">
            Cancel
          </button>
        )}
      </div>
    </form>
  )
}

export default UserForm
