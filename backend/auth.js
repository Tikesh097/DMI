import crypto from 'crypto'

// Simple API key authentication (in production, use JWT or OAuth)
const API_KEYS = new Map()

// Generate a new API key
export const generateApiKey = (userId) => {
  const apiKey = crypto.randomBytes(32).toString('hex')
  API_KEYS.set(apiKey, {
    userId,
    createdAt: new Date(),
    lastUsed: new Date()
  })
  return apiKey
}

// Validate API key
export const validateApiKey = (apiKey) => {
  if (!apiKey) return null

  const keyData = API_KEYS.get(apiKey)
  if (!keyData) return null

  // Update last used timestamp
  keyData.lastUsed = new Date()
  return keyData.userId
}

// Middleware to check authentication
export const authMiddleware = (req, res, next) => {
  // Skip auth for health check
  if (req.path === '/api/health') {
    return next()
  }

  const apiKey = req.headers['x-api-key']

  if (!apiKey) {
    return res.status(401).json({
      error: 'Authentication required',
      message: 'Please provide an API key in the X-API-Key header'
    })
  }

  const userId = validateApiKey(apiKey)

  if (!userId) {
    return res.status(401).json({
      error: 'Invalid API key',
      message: 'The provided API key is invalid or expired'
    })
  }

  // Attach userId to request
  req.userId = userId
  next()
}

// Optional: Simple role-based access control
export const ROLES = {
  ADMIN: 'admin',
  USER: 'user'
}

// For demo purposes, hardcode a default API key
// In production, store these securely in a database
export const DEFAULT_API_KEY = generateApiKey('admin')

console.log('🔑 Default API Key:', DEFAULT_API_KEY)
console.log('ℹ️  Use this key in the X-API-Key header for API requests')

export default {
  generateApiKey,
  validateApiKey,
  authMiddleware,
  DEFAULT_API_KEY
}
