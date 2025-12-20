// Vercel serverless function wrapper for Express app
const app = require('../server.js');

// Export handler for Vercel serverless functions
// Wrap Express app to ensure CORS headers are always set for OPTIONS requests
module.exports = (req, res) => {
  // Set CORS headers immediately for all requests
  const origin = req.headers.origin;
  
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  } else {
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
  
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-API-Key, X-Staff-Identifier, Authorization, Accept');
  res.setHeader('Access-Control-Max-Age', '86400');
  res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Type');
  
  // Handle OPTIONS preflight requests immediately
  if (req.method === 'OPTIONS') {
    console.log('✅ Vercel: OPTIONS preflight handled in wrapper');
    return res.status(200).send('');
  }
  
  // For all other requests, pass to Express app
  return app(req, res);
};

