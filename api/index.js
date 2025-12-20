// Vercel serverless function wrapper for Express app
const app = require('../server.js');

// Export handler for Vercel serverless functions
// Simply pass the Express app - it will handle CORS via its middleware
module.exports = app;

