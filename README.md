# React + Vite + NeonDB Database Management System

A full-stack database management application with complete CRUD operations, built with React (JSX), Vite, Node.js, Express, and NeonDB PostgreSQL.

----

### deployed url - https://newdmi.netlify.app/

## Features

### Database Management
- ✅ **Create New Databases** - Dynamically create PostgreSQL databases with validation
- ✅ **Database Selection** - Browse and select from existing databases
- ✅ **Database Migration** - Export and import complete database schemas and data
- ✅ **Database Validation** - SQL injection prevention and naming convention enforcement
- ✅ **Database Listing** - View all databases with size information

### User Management
- ✅ Full CRUD operations (Create, Read, Update, Delete)
- ✅ Multi-database support - Manage users across different databases
- ✅ Form validation and error handling
- ✅ Real-time updates

### Security & Logging
- ✅ **Activity Logging** - Track all database operations and user actions
- ✅ **API Key Authentication** - Secure API endpoints (optional, disabled for development)
- ✅ **SQL Injection Prevention** - Parameterized queries and input validation
- ✅ **Transaction Management** - Atomic operations for data integrity

### UI/UX
- ✅ Beautiful gradient design
- ✅ Responsive layout
- ✅ Loading states and progress indicators
- ✅ Success and error notifications
- ✅ Real-time database switching

## Project Structure

```
react-vite-neondb-app/
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── DatabaseManager.jsx
│   │   │   ├── UserForm.jsx
│   │   │   └── UserList.jsx
│   │   ├── App.jsx
│   │   ├── App.css
│   │   ├── main.jsx
│   │   └── index.css
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── backend/
│   ├── server.js
│   ├── db.js
│   ├── auth.js
│   ├── logger.js
│   ├── .env.example
│   └── package.json
└── README.md
```

## Prerequisites

- Node.js (v18 or higher)
- npm or yarn
- NeonDB account (free tier available at https://neon.tech)

## Setup Instructions

### 1. Get NeonDB Connection String

1. Go to [https://neon.tech](https://neon.tech)
2. Sign up for a free account
3. Create a new project
4. Copy your connection string from the dashboard
5. Format: `postgresql://username:password@ep-xxx-xxx.region.aws.neon.tech/dbname?sslmode=require`

### 2. Backend Setup

```bash
# Navigate to backend directory
cd backend

# Create .env file from example
cp .env.example .env

# Edit .env and add your NeonDB connection string
# DATABASE_URL=your_neondb_connection_string_here

# Install dependencies
npm install

# Start backend server
npm run dev
```

The backend will run on `http://localhost:5000`

**Note:** When the server starts, it will display a default API key in the console. Save this for authentication if enabled.

### 3. Frontend Setup

Open a new terminal window:

```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
npm install

# Start frontend dev server
npm run dev
```

The frontend will run on `http://localhost:3000`

## API Endpoints

### Database Management

- `GET /api/health` - Health check and API key
- `GET /api/databases` - List all databases
- `POST /api/databases` - Create new database
  ```json
  { "name": "my_database" }
  ```
- `GET /api/databases/:name/exists` - Check if database exists
- `POST /api/databases/:name/initialize` - Initialize database tables
- `GET /api/databases/:name/export` - Export database schema and data
- `POST /api/databases/migrate` - Migrate from source to target database
  ```json
  {
    "sourceDatabase": "source_db",
    "targetDatabase": "target_db"
  }
  ```

### User Management

- `GET /api/databases/:dbName/users` - Get all users from database
- `GET /api/databases/:dbName/users/:id` - Get single user by ID
- `POST /api/databases/:dbName/users` - Create new user
  ```json
  {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }
  ```
- `PUT /api/databases/:dbName/users/:id` - Update user
- `DELETE /api/databases/:dbName/users/:id` - Delete user

### Activity Logs

- `GET /api/logs?limit=100` - Get activity logs

## Database Schema

```sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(100) UNIQUE NOT NULL,
  age INTEGER CHECK (age > 0 AND age < 150),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Tech Stack

### Frontend
- React 18 (JSX only, no TypeScript)
- Vite (Fast build tool)
- Axios (HTTP client)
- CSS3 (Modern styling)

### Backend
- Node.js
- Express (Web framework)
- PostgreSQL via NeonDB (Cloud database)
- pg (PostgreSQL client)
- dotenv (Environment variables)
- cors (Cross-origin resource sharing)

## Security Features

### SQL Injection Prevention
- Database names validated with regex: `^[a-zA-Z_][a-zA-Z0-9_]*$`
- All queries use parameterized statements
- Maximum name length: 63 characters

### Authentication (Optional)
- API key-based authentication
- Disabled by default for development
- Enable by uncommenting `app.use(authMiddleware)` in `server.js`

### Transaction Management
- Atomic database migrations
- Rollback on errors
- ACID compliance

### Activity Logging
All operations are logged to `database-operations.log`:
- Database creation/access
- User CRUD operations
- Migration activities
- Timestamps and user IDs

## Development Scripts

### Frontend
- `npm run dev` - Start development server (port 3000)
- `npm run build` - Build for production
- `npm run preview` - Preview production build

### Backend
- `npm run dev` - Start with auto-reload (recommended)
- `npm start` - Start production server

## Environment Variables

Create a `.env` file in the `backend` directory:

```env
DATABASE_URL=postgresql://username:password@host/database?sslmode=require
PORT=5000
NODE_ENV=development
```

## Usage Guide

### Creating a New Database

1. Enter a database name in the "Create New Database" field
2. Click "Create Database"
3. The database will be created and automatically initialized with the users table
4. The new database will be selected automatically

### Selecting an Existing Database

1. Enter the database name in "Select Existing Database" field, or
2. Click on any database from the "Available Databases" list
3. The selected database will highlight in purple

### Migrating Data Between Databases

1. Enter the source database name
2. Enter the target database name (must exist)
3. Click "Migrate"
4. All tables, schemas, data, and constraints will be copied
5. Progress will be shown during migration

### Managing Users

1. Select a database first
2. Fill in the user form (Name and Email required, Age optional)
3. Click "Add User" to create
4. Click "Edit" on any user to modify
5. Click "Delete" to remove (with confirmation)

## Error Handling

The application handles:
- Invalid database names
- Duplicate database creation
- Missing databases
- Unique constraint violations (duplicate emails)
- Connection failures
- Invalid user data
- Migration errors

All errors are displayed to the user with clear messages.

## Logging

Activity logs are stored in `backend/database-operations.log`:

```json
{
  "timestamp": "2024-01-01T12:00:00.000Z",
  "level": "SUCCESS",
  "userId": "admin",
  "action": "DATABASE_CREATED",
  "details": { "database": "my_database" }
}
```

View logs via API: `GET /api/logs?limit=100`

## Production Deployment

### Backend

1. Set `NODE_ENV=production` in `.env`
2. Enable authentication by uncommenting `app.use(authMiddleware)` in `server.js`
3. Store API keys securely (use environment variables or secret manager)
4. Use a process manager like PM2

### Frontend

```bash
npm run build
# Deploy the dist/ folder to your hosting service
```

### Security Checklist

- [ ] Enable authentication middleware
- [ ] Use HTTPS in production
- [ ] Secure API keys in environment variables
- [ ] Enable rate limiting
- [ ] Set up proper CORS policies
- [ ] Regular database backups
- [ ] Monitor activity logs

## Limitations

- NeonDB free tier has connection and storage limits
- Database creation requires appropriate PostgreSQL permissions
- Large database migrations may take time
- Browser localStorage not used (state resets on refresh)

## Troubleshooting

### Cannot create database
- Check NeonDB permissions
- Verify connection string
- Ensure database name follows naming rules

### Migration fails
- Ensure both databases exist
- Check for conflicting data
- Review error messages in logs

### Users not loading
- Verify database is selected
- Check database is initialized
- Ensure tables exist

## License

MIT

## Author

Built with React JSX, Vite, Node.js, Express, and NeonDB PostgreSQL

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

For issues or questions:
- Check the troubleshooting section
- Review activity logs
- Contact support at your NeonDB dashboard
