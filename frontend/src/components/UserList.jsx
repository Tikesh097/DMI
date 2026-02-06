function UserList({ users, onDelete, onEdit }) {
  if (users.length === 0) {
    return (
      <div className="empty-state">
        <p>No users found. Add your first user!</p>
      </div>
    )
  }

  return (
    <div className="user-list">
      {users.map(user => (
        <div key={user.id} className="user-card">
          <div className="user-info">
            <h3>{user.name}</h3>
            <p className="user-email">{user.email}</p>
            {user.age && <p className="user-age">Age: {user.age}</p>}
            <p className="user-date">
              Created: {new Date(user.created_at).toLocaleDateString()}
            </p>
          </div>
          <div className="user-actions">
            <button
              onClick={() => onEdit(user)}
              className="btn btn-edit"
            >
              Edit
            </button>
            <button
              onClick={() => onDelete(user.id)}
              className="btn btn-delete"
            >
              Delete
            </button>
          </div>
        </div>
      ))}
    </div>
  )
}

export default UserList
