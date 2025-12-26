const express = require('express');
const cors = require('cors');
const fetch = require('node-fetch');
const nodemailer = require('nodemailer');
const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const XLSX = require('xlsx');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const app = express();

// Backup scheduler
let backupScheduler = null;
let lastBackupCheck = null;

// Enable CORS for Shopify store - MUST BE BEFORE OTHER MIDDLEWARE
// Use cors package for reliable CORS handling
app.use(cors({
  origin: true, // Allow all origins (reflects the request origin back)
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: ['Content-Type', 'X-API-Key', 'X-Staff-Identifier', 'Authorization', 'Accept'],
  exposedHeaders: ['Content-Length', 'Content-Type'],
  maxAge: 86400,
  preflightContinue: false,
  optionsSuccessStatus: 200
}));

app.use(express.json());

// Your Shopify credentials (from environment variables)
const SHOPIFY_SHOP = process.env.SHOPIFY_SHOP;
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API_SECRET = process.env.API_SECRET;
const JWT_SECRET = process.env.JWT_SECRET || API_SECRET || 'your-secret-key-change-in-production';

// Email configuration (SMTP)
const SMTP_HOST = process.env.SMTP_HOST || 'smtp.gmail.com';
const SMTP_PORT = process.env.SMTP_PORT || 587;
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASS = process.env.SMTP_PASS;
const SMTP_FROM = process.env.SMTP_FROM || SMTP_USER;
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || SMTP_USER;

// Email transporter
const transporter = nodemailer.createTransport({
  host: SMTP_HOST,
  port: SMTP_PORT,
  secure: false, // true for 465, false for other ports
  auth: SMTP_USER && SMTP_PASS ? {
    user: SMTP_USER,
    pass: SMTP_PASS
  } : undefined
});

// Pricing rules storage (in-memory, can be persisted to file/database)
let pricingRules = {
  "Apple": {
    "iPhone 15": {
      "128GB": { "base": 500 },
      "256GB": { "base": 600 },
      "512GB": { "base": 700 }
    },
    "iPhone 14": {
      "128GB": { "base": 400 },
      "256GB": { "base": 500 },
      "512GB": { "base": 600 }
    },
    "iPhone 13": {
      "128GB": { "base": 350 },
      "256GB": { "base": 450 },
      "512GB": { "base": 550 }
    },
    "iPhone 12": {
      "128GB": { "base": 300 },
      "256GB": { "base": 400 }
    }
  },
  "Samsung": {
    "Galaxy S24": {
      "128GB": { "base": 450 },
      "256GB": { "base": 550 },
      "512GB": { "base": 650 }
    },
    "Galaxy S23": {
      "128GB": { "base": 400 },
      "256GB": { "base": 500 }
    }
  }
};

// Condition multipliers
const conditionMultipliers = {
  "Excellent": 1.0,
  "Good": 0.8,
  "Fair": 0.6,
  "Faulty": 0.3
};

// MongoDB connection
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://abrahambarrios970_db_user:c3CZOXj85ikzHYLM@cluster0.ueqwc8p.mongodb.net/?appName=Cluster0';
const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME || 'trade_in_system';
let mongoClient = null;
let db = null;

// Initialize MongoDB connection
async function initMongoDB() {
  try {
    if (!MONGODB_URI) {
      console.error('❌ MONGODB_URI environment variable not set!');
      return false;
    }
    
    console.log('🔄 Attempting to connect to MongoDB...');
    mongoClient = new MongoClient(MONGODB_URI);
    await mongoClient.connect();
    db = mongoClient.db(MONGODB_DB_NAME);
    console.log('✅ Connected to MongoDB');
    
    // Create indexes for better performance (ignore errors if they already exist)
    try {
      await db.collection('submissions').createIndex({ id: 1 }, { unique: true });
      await db.collection('submissions').createIndex({ status: 1 });
      await db.collection('submissions').createIndex({ createdAt: -1 });
      await db.collection('submissions').createIndex({ customerId: 1 });
      
      // Customer collection indexes
      await db.collection('customers').createIndex({ email: 1 }, { unique: true });
      await db.collection('customers').createIndex({ createdAt: -1 });
      
      // Session collection indexes
      await db.collection('sessions').createIndex({ sessionId: 1 }, { unique: true });
      await db.collection('sessions').createIndex({ customerId: 1 });
      await db.collection('sessions').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
      
      // Password reset tokens
      await db.collection('password_reset_tokens').createIndex({ token: 1 }, { unique: true });
      await db.collection('password_reset_tokens').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    } catch (indexError) {
      // Indexes might already exist, that's okay
      console.log('Index creation skipped (may already exist)');
    }
    
    return true;
  } catch (error) {
    console.error('❌ MongoDB connection error:', error.message);
    console.error('Full error:', error);
    console.warn('⚠️ MongoDB connection failed - data may not persist');
    return false;
  }
}

// Trade-in submissions storage (in-memory fallback, MongoDB for production)
let tradeInSubmissions = [];
let submissionIdCounter = 1;
let useMongoDB = false;

// Load submissions from MongoDB or file
async function loadSubmissions() {
  // Ensure MongoDB connection first
  await ensureMongoConnection();
  
  if (db) {
    try {
      const submissions = await db.collection('submissions').find({}).sort({ id: 1 }).toArray();
      tradeInSubmissions = submissions;
      submissionIdCounter = submissions.length > 0 ? Math.max(...submissions.map(s => s.id)) + 1 : 1;
      console.log(`✅ Loaded ${tradeInSubmissions.length} submissions from MongoDB`);
      useMongoDB = true;
      return;
    } catch (error) {
      console.error('Error loading from MongoDB:', error);
    }
  }
  
  // Fallback to file (skip on Vercel)
  if (process.env.VERCEL) {
    console.log('No MongoDB connection, starting fresh (Vercel - no file storage)');
    tradeInSubmissions = [];
    submissionIdCounter = 1;
    return;
  }
  
  try {
    const filePath = path.join(__dirname, 'trade-in-submissions.json');
    const data = await fs.readFile(filePath, 'utf8');
    const parsed = JSON.parse(data);
    tradeInSubmissions = parsed.submissions || [];
    submissionIdCounter = parsed.counter || (tradeInSubmissions.length > 0 ? Math.max(...tradeInSubmissions.map(s => s.id)) + 1 : 1);
    console.log(`📁 Loaded ${tradeInSubmissions.length} submissions from file`);
  } catch (error) {
    console.log('No submissions file found, starting fresh');
    tradeInSubmissions = [];
    submissionIdCounter = 1;
  }
}

// Ensure MongoDB connection (for serverless functions)
async function ensureMongoConnection() {
  if (db) {
    // Test connection is still alive
    try {
      await db.admin().ping();
      return true;
    } catch (pingError) {
      console.log('⚠️ MongoDB connection lost, reconnecting...');
      db = null;
      mongoClient = null;
    }
  }
  
  try {
    await initMongoDB();
    return db !== null;
  } catch (error) {
    console.error('Failed to connect to MongoDB:', error);
    return false;
  }
}

// Save submissions to MongoDB or file
async function saveSubmissions() {
  // Ensure MongoDB connection
  await ensureMongoConnection();
  
  if (db) {
    try {
      // Save all submissions to MongoDB
      for (const submission of tradeInSubmissions) {
        await db.collection('submissions').replaceOne(
          { id: submission.id },
          submission,
          { upsert: true }
        );
      }
      console.log(`✅ Saved ${tradeInSubmissions.length} submissions to MongoDB`);
      return;
    } catch (error) {
      console.error('Error saving to MongoDB:', error);
    }
  }
  
  // Fallback to file (skip on Vercel - read-only filesystem)
  if (process.env.VERCEL) {
    console.warn('⚠️ Vercel detected: Skipping file save (read-only filesystem). MongoDB required.');
    return;
  }
  
  try {
    const filePath = path.join(__dirname, 'trade-in-submissions.json');
    await fs.writeFile(
      filePath,
      JSON.stringify({
        submissions: tradeInSubmissions,
        counter: submissionIdCounter,
        lastUpdated: new Date().toISOString()
      }, null, 2),
      'utf8'
    );
    console.log(`📁 Saved ${tradeInSubmissions.length} submissions to file`);
  } catch (error) {
    console.error('Error saving submissions:', error);
  }
}

// Initialize MongoDB and load submissions
(async () => {
  await initMongoDB();
  await loadSubmissions();
})();

// Load pricing rules from MongoDB or file
async function loadPricingRules() {
  // Ensure MongoDB connection first
  await ensureMongoConnection();
  
  if (db) {
    try {
      const doc = await db.collection('pricing').findOne({ type: 'rules' });
      if (doc) {
        if (doc.rules) {
          pricingRules = doc.rules;
        }
        if (doc.multipliers) {
          conditionMultipliers = doc.multipliers;
        }
        console.log('✅ Pricing rules and multipliers loaded from MongoDB');
        return;
      }
    } catch (error) {
      console.error('Error loading pricing rules from MongoDB:', error);
    }
  }
  
  // Fallback to file (skip on Vercel)
  if (process.env.VERCEL) {
    console.log('No MongoDB connection, using default pricing rules (Vercel - no file storage)');
    return;
  }
  
  try {
    const data = await fs.readFile(path.join(__dirname, 'pricing-rules.json'), 'utf8');
    const parsed = JSON.parse(data);
    if (parsed.rules) {
      pricingRules = parsed.rules;
    }
    if (parsed.multipliers) {
      conditionMultipliers = parsed.multipliers;
    }
    console.log('📁 Pricing rules loaded from file');
  } catch (error) {
    console.log('No pricing rules file found, using defaults');
  }
}

// Save pricing rules to MongoDB or file
async function savePricingRules() {
  // Ensure MongoDB connection
  await ensureMongoConnection();
  
  if (db) {
    try {
      const result = await db.collection('pricing').replaceOne(
        { type: 'rules' },
        { 
          type: 'rules', 
          rules: pricingRules, 
          multipliers: conditionMultipliers,
          updatedAt: new Date().toISOString() 
        },
        { upsert: true }
      );
      console.log('✅ Pricing rules saved to MongoDB', {
        matched: result.matchedCount,
        modified: result.modifiedCount,
        upserted: result.upsertedCount,
        brands: Object.keys(pricingRules).length
      });
      return;
    } catch (error) {
      console.error('Error saving pricing rules to MongoDB:', error);
      throw error; // Re-throw to handle in caller
    }
  }
  
  // Fallback to file (skip on Vercel)
  if (process.env.VERCEL) {
    console.warn('⚠️ Vercel detected: Skipping file save (read-only filesystem). MongoDB required.');
    if (!db) {
      throw new Error('MongoDB connection required on Vercel');
    }
    return;
  }
  
  try {
    await fs.writeFile(
      path.join(__dirname, 'pricing-rules.json'),
      JSON.stringify({ rules: pricingRules, multipliers: conditionMultipliers }, null, 2),
      'utf8'
    );
    console.log('📁 Pricing rules saved to file');
  } catch (error) {
    console.error('Error saving pricing rules:', error);
    throw error;
  }
}

// Initialize pricing rules (on server start)
(async () => {
  try {
    await loadPricingRules();
  } catch (error) {
    console.error('Pricing rules initialization error:', error);
  }
})();

// Wishlist save endpoint (updated to use MongoDB and JWT)
app.post('/api/wishlist', async (req, res) => {
  try {
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Check for JWT token (custom auth) first
    let customerId = null;
    const authHeader = req.headers['authorization'];
    
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.split(' ')[1];
      try {
        const decoded = jwt.verify(token, JWT_SECRET);
        customerId = decoded.customerId;
      } catch (jwtError) {
        // JWT invalid - check for legacy API key auth (for backward compatibility)
        console.warn('JWT token invalid, checking legacy auth:', jwtError.message);
      }
    }

    // Legacy support: Check for API key auth (backward compatibility)
    if (!customerId) {
      const apiKeyHeader = req.headers['x-api-key'];
      if (apiKeyHeader === API_SECRET) {
        // Legacy mode: use customer_id from body (Shopify customer ID)
        const { customer_id } = req.body;
        if (customer_id) {
          // Try to find MongoDB customer by shopifyCustomerId
          const customer = await db.collection('customers').findOne({ 
            shopifyCustomerId: customer_id.toString() 
          });
          if (customer) {
            customerId = customer._id.toString();
          }
        }
      }
    }
    
    if (!customerId) {
      return res.status(401).json({ error: 'Unauthorized - Please log in' });
    }

    const { wishlist } = req.body;

    if (!wishlist || !Array.isArray(wishlist)) {
      return res.status(400).json({ error: 'Missing or invalid wishlist array' });
    }

    // Save to MongoDB
    const wishlistData = {
      customerId: customerId,
      productIds: wishlist.map(id => String(id)), // Ensure all IDs are strings
      updatedAt: new Date().toISOString()
    };

    // Upsert wishlist (create if doesn't exist, update if exists)
    await db.collection('wishlists').updateOne(
      { customerId: customerId },
      { 
        $set: wishlistData,
        $setOnInsert: { createdAt: new Date().toISOString() }
      },
      { upsert: true }
    );

    res.json({ 
      success: true, 
      message: 'Wishlist saved successfully' 
    });

  } catch (error) {
    console.error('Error saving wishlist:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get wishlist endpoint (updated to use MongoDB and JWT)
app.get('/api/wishlist/get', async (req, res) => {
  try {
    await ensureMongoConnection();
    
    if (!db) {
      return res.json({ success: true, wishlist: [] }); // Return empty instead of error
    }

    // Check for JWT token (custom auth) first
    let customerId = null;
    const authHeader = req.headers['authorization'];
    
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.split(' ')[1];
      try {
        const decoded = jwt.verify(token, JWT_SECRET);
        customerId = decoded.customerId;
      } catch (jwtError) {
        // JWT invalid - check for legacy API key auth
        console.warn('JWT token invalid, checking legacy auth:', jwtError.message);
      }
    }

    // Legacy support: Check for API key auth and customer_id query param
    if (!customerId) {
      const apiKeyHeader = req.headers['x-api-key'];
      if (apiKeyHeader === API_SECRET) {
        const queryCustomerId = req.query.customer_id;
        if (queryCustomerId) {
          // Try to find MongoDB customer by shopifyCustomerId
          const customer = await db.collection('customers').findOne({ 
            shopifyCustomerId: queryCustomerId.toString() 
          });
          if (customer) {
            customerId = customer._id.toString();
          }
        }
      }
    }
    
    if (!customerId) {
      // No authentication - return empty wishlist (guest mode)
      return res.json({ success: true, wishlist: [] });
    }

    // Fetch wishlist from MongoDB
    const wishlistDoc = await db.collection('wishlists').findOne({ 
      customerId: customerId 
    });

    if (wishlistDoc && wishlistDoc.productIds) {
        res.json({ 
          success: true, 
        wishlist: Array.isArray(wishlistDoc.productIds) ? wishlistDoc.productIds : []
        });
    } else {
      // No wishlist found - return empty array
      res.json({ success: true, wishlist: [] });
    }

  } catch (error) {
    console.error('Error fetching wishlist:', error);
    // Return empty wishlist instead of 500 error
    res.json({ 
      success: true, 
      wishlist: [],
      error: 'Server error (using empty wishlist)'
    });
  }
});

// ============================================
// HELPER FUNCTIONS
// ============================================

// Generate SEO-friendly slug from product data
function generateProductSlug(brand, model, storage, color = null) {
  // Combine brand, model, storage (and optionally color) into a slug
  const parts = [
    brand,
    model,
    storage
  ];
  
  // Add color if specified and not "Default"
  if (color && color.toLowerCase() !== 'default' && color.trim() !== '') {
    parts.push(color);
  }
  
  // Join and create slug: lowercase, replace spaces/special chars with hyphens, remove duplicates
  const slug = parts
    .filter(p => p && p.trim() !== '')
    .map(p => p.trim().toLowerCase())
    .join('-')
    .replace(/[^a-z0-9-]/g, '-') // Replace non-alphanumeric with hyphens
    .replace(/-+/g, '-') // Replace multiple hyphens with single hyphen
    .replace(/^-|-$/g, ''); // Remove leading/trailing hyphens
  
  return slug;
}

// ============================================
// TRADE-IN PRICING ENDPOINTS
// ============================================

// OLD ENDPOINT - DISABLED: Now using MongoDB endpoint below (line 1165)
// This endpoint fetched from Shopify but has been replaced by the database-driven endpoint
/*
app.get('/api/products/trade-in', async (req, res) => {
  try {
    const { deviceType } = req.query; // Optional: filter by device type

    // GraphQL query to fetch products with "trade-in" tag
    const query = `
      query getTradeInProducts($first: Int!, $after: String) {
        products(first: $first, after: $after, query: "tag:trade-in") {
          pageInfo {
            hasNextPage
            endCursor
          }
          edges {
            node {
              id
              title
              handle
              vendor
              tags
              featuredImage {
                id
                url
                altText
                width
                height
              }
              images(first: 5) {
                edges {
                  node {
                    id
                    url
                    altText
                    width
                    height
                  }
                }
              }
              variants(first: 50) {
                edges {
                  node {
                    id
                    title
                    price
                    availableForSale
                    selectedOptions {
                      name
                      value
                    }
                    image {
                      id
                      url
                      altText
                    }
                  }
                }
              }
            }
          }
        }
      }
    `;

    let allProducts = [];
    let hasNextPage = true;
    let cursor = null;

    // Paginate through all products
    while (hasNextPage) {
      const response = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        },
        body: JSON.stringify({
          query: query,
          variables: { first: 50, after: cursor }
        })
      });

      const data = await response.json();

      if (data.errors) {
        console.error('GraphQL errors fetching products:', data.errors);
        
        // Check for access denied errors
        const accessDeniedError = data.errors.find(err => 
          err.extensions?.code === 'ACCESS_DENIED' || 
          err.message?.includes('Access denied')
        );
        
        if (accessDeniedError) {
          return res.status(403).json({
            success: false,
            error: 'Shopify API permission denied',
            message: 'Your Shopify access token is missing the required "read_products" scope.',
            details: data.errors,
            fix: {
              step1: 'Go to Shopify Admin → Settings → Apps and sales channels → Develop apps',
              step2: 'Find your app (or create new) → Configuration tab',
              step3: 'Enable "read_products" scope in Admin API integration scopes',
              step4: 'Save and update SHOPIFY_ACCESS_TOKEN in your environment variables',
              documentation: 'See SHOPIFY_API_PERMISSIONS_FIX.md for detailed instructions'
            }
          });
        }
        
        return res.status(500).json({
          success: false,
          error: 'Failed to fetch products from Shopify',
          details: data.errors
        });
      }

      const products = data.data?.products?.edges || [];
      allProducts = allProducts.concat(products.map(edge => ({
        id: edge.node.id.replace('gid://shopify/Product/', ''),
        gid: edge.node.id,
        title: edge.node.title,
        handle: edge.node.handle,
        vendor: edge.node.vendor,
        tags: Array.isArray(edge.node.tags) ? edge.node.tags : (edge.node.tags ? [edge.node.tags] : []),
        featuredImage: edge.node.featuredImage ? {
          url: edge.node.featuredImage.url,
          altText: edge.node.featuredImage.altText || edge.node.title,
          width: edge.node.featuredImage.width,
          height: edge.node.featuredImage.height
        } : null,
        images: edge.node.images?.edges?.map(img => ({
          url: img.node.url,
          altText: img.node.altText || edge.node.title,
          width: img.node.width,
          height: img.node.height
        })) || [],
        variants: edge.node.variants.edges.map(v => ({
          id: v.node.id.replace('gid://shopify/ProductVariant/', ''),
          gid: v.node.id,
          title: v.node.title,
          price: parseFloat(v.node.price),
          availableForSale: v.node.availableForSale,
          image: v.node.image ? {
            url: v.node.image.url,
            altText: v.node.image.altText || v.node.title
          } : null,
          options: v.node.selectedOptions.reduce((acc, opt) => {
            acc[opt.name.toLowerCase()] = opt.value;
            return acc;
          }, {})
        }))
      })));

      hasNextPage = data.data?.products?.pageInfo?.hasNextPage || false;
      cursor = data.data?.products?.pageInfo?.endCursor;
    }

    // Filter by device type if specified (check tags or vendor)
    let filteredProducts = allProducts;
    if (deviceType && deviceType.trim() !== '') {
      const deviceTypeLower = deviceType.toLowerCase();
      console.log(`🔍 Filtering products for device type: "${deviceType}" (${deviceTypeLower})`);
      console.log(`📦 Total products before filtering: ${allProducts.length}`);
      
      filteredProducts = allProducts.filter(product => {
        // Ensure tags is an array
        if (!product.tags || !Array.isArray(product.tags)) {
          console.log(`⚠️ Product "${product.title}" has invalid tags:`, product.tags);
          return false;
        }
        
        // Convert tags to lowercase array
        const tags = product.tags.map(t => String(t).toLowerCase().trim());
        
        // Check for exact match (not substring)
        const hasExactMatch = tags.includes(deviceTypeLower) || 
                             tags.includes(`trade-in-${deviceTypeLower}`);
        
        if (hasExactMatch) {
          console.log(`✅ Product "${product.title}" matches - tags:`, tags);
        }
        
        return hasExactMatch;
      });
      
      console.log(`📊 Products after filtering: ${filteredProducts.length}`);
      
      // Log if no products found for this device type
      if (filteredProducts.length === 0) {
        console.log(`ℹ️ No products found with "${deviceType}" tag. Products need to be tagged with "${deviceType}" or "trade-in-${deviceType}" to appear on this page.`);
        // Log all available tags for debugging
        const allTags = new Set();
        allProducts.forEach(p => {
          if (p.tags && Array.isArray(p.tags)) {
            p.tags.forEach(t => allTags.add(String(t).toLowerCase()));
          }
        });
        console.log(`📋 Available tags in all products:`, Array.from(allTags).sort());
      } else {
        // Log which products matched
        console.log(`✅ Matched products:`, filteredProducts.map(p => ({
          title: p.title,
          tags: p.tags
        })));
      }
    }

    res.json({
      success: true,
      products: filteredProducts,
      count: filteredProducts.length,
      deviceType: deviceType || 'all',
      filtered: deviceType ? filteredProducts.length < allProducts.length : false
    });

  } catch (error) {
    console.error('Error fetching trade-in products:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message
    });
  }
});
*/

// Calculate valuation (supports both old and new systems)
app.post('/api/pricing/calculate', async (req, res) => {
  try {
    const { 
      // New system (variant-based)
      productId, 
      variantId, 
      // Old system (backward compatibility)
      brand, 
      model, 
      storage, 
      // Common
      condition 
    } = req.body;

    // Log the request for debugging
    console.log('Pricing calculation request:', { productId, variantId, brand, model, storage, condition });

    if (!condition) {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required field: condition' 
      });
    }

    let basePrice = null;
    let useConditionPrice = false; // Flag to use direct condition price vs multiplier

    // NEW SYSTEM: Check if database product or Shopify product
    if (productId && variantId) {
      // Check if it's a database product (gid://database)
      if (variantId.includes('gid://database') || productId.includes('database')) {
        try {
          await ensureMongoConnection();
          if (!db) {
            return res.status(500).json({
              success: false,
              error: 'Database connection failed'
            });
          }

          // Extract product data from variantId or fetch from database
          // VariantId format: gid://database/Variant/{productId}_{storage}_{color}
          // Example: gid://database/Variant/507f1f77bcf86cd799439011_256GB_default
          // Or without prefix: 507f1f77bcf86cd799439011_256GB_default
          console.log('🔍 Parsing database variantId:', variantId);
          
          let product = null;
          
          // Try to extract MongoDB ObjectId from variantId
          // Format: gid://database/Variant/{ObjectId}_{storage}_{color}
          // Or: {ObjectId}_{storage}_{color}
          let dbProductId = null;
          let storageFromVariant = null;
          
          if (variantId.includes('gid://database/Variant/')) {
            const variantIdMatch = variantId.match(/gid:\/\/database\/Variant\/([^_]+)_(.+)/);
            if (variantIdMatch) {
              dbProductId = variantIdMatch[1];
              storageFromVariant = variantIdMatch[2].split('_')[0]; // Get storage (before color if exists)
            }
          } else {
            // Try without prefix: {ObjectId}_{storage}_{color}
            const parts = variantId.split('_');
            if (parts.length >= 2) {
              dbProductId = parts[0];
              storageFromVariant = parts[1];
            }
          }
          
          if (dbProductId) {
            console.log('📦 Extracted from variantId:', { dbProductId, storageFromVariant, storage });
            
            try {
              // Try to find by MongoDB ObjectId
              const searchStorage = storage || storageFromVariant;
              product = await db.collection('trade_in_products').findOne({ 
                _id: new ObjectId(dbProductId),
                ...(searchStorage ? { storage: searchStorage } : {})
              });
              
              if (product) {
                console.log('✅ Found product by ObjectId:', product.brand, product.model, product.storage);
              }
            } catch (e) {
              console.log('⚠️ ObjectId parsing failed, trying alternative lookup:', e.message);
            }
          }
          
          // If not found by ID, try to find by brand/model/storage/color
          if (!product && brand && model && storage) {
            console.log('🔍 Trying to find by brand/model/storage:', { brand, model, storage, color: req.body.color });
            product = await db.collection('trade_in_products').findOne({
              brand: brand.trim(),
              model: model.trim(),
              storage: storage.trim(),
              color: req.body.color ? req.body.color.trim() : null
            });
            
            if (product) {
              console.log('✅ Found product by brand/model/storage:', product.brand, product.model, product.storage);
            }
          }
          
          // If still not found, try without color
          if (!product && brand && model && storage) {
            product = await db.collection('trade_in_products').findOne({
              brand: brand.trim(),
              model: model.trim(),
              storage: storage.trim()
            });
            
            if (product) {
              console.log('✅ Found product by brand/model/storage (without color):', product.brand, product.model, product.storage);
            }
          }

          if (!product) {
            return res.status(404).json({
              success: false,
              error: 'Product not found in database'
            });
          }

          // Check if condition-specific price exists
          const conditionPrice = product.prices?.[condition];
          
          if (conditionPrice === null || conditionPrice === undefined) {
            return res.status(404).json({
              success: false,
              error: `Price not available for condition: ${condition}`,
              availableConditions: Object.keys(product.prices || {}).filter(c => product.prices[c] !== null)
            });
          }

          // Use condition-specific price directly (no multiplier needed)
          basePrice = conditionPrice;
          useConditionPrice = true;
          console.log(`✅ Found condition price from database: £${basePrice} for ${product.brand} ${product.model} ${product.storage} (${condition})`);

        } catch (error) {
          console.error('Error fetching product from database:', error);
          return res.status(500).json({
            success: false,
            error: 'Failed to fetch product price from database',
            message: error.message
          });
        }
      } else {
        // Shopify product (legacy support)
        try {
          // Fetch variant price from Shopify
          const variantQuery = `
            query getVariant($id: ID!) {
              productVariant(id: $id) {
                id
                price
                product {
                  id
                  title
                }
              }
            }
          `;

          const variantGid = variantId.startsWith('gid://') ? variantId : `gid://shopify/ProductVariant/${variantId}`;
          
          const response = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
            },
            body: JSON.stringify({
              query: variantQuery,
              variables: { id: variantGid }
            })
          });

          const data = await response.json();

          if (data.errors) {
            console.error('GraphQL errors fetching variant:', data.errors);
            return res.status(404).json({
              success: false,
              error: 'Variant not found in Shopify',
              details: data.errors
            });
          }

          const variant = data.data?.productVariant;
          if (!variant) {
            return res.status(404).json({
              success: false,
              error: 'Variant not found'
            });
          }

          basePrice = parseFloat(variant.price);
          console.log(`✅ Found variant price from Shopify: £${basePrice} for ${variant.product.title}`);

        } catch (error) {
          console.error('Error fetching variant from Shopify:', error);
          return res.status(500).json({
            success: false,
            error: 'Failed to fetch variant price from Shopify',
            message: error.message
          });
        }
      }
    }
    // OLD SYSTEM: Use pricing rules (backward compatibility)
    else if (brand && model && storage) {
      // Get base price - try exact match first
      basePrice = pricingRules[brand]?.[model]?.[storage]?.base;
      
      // If not found, try case-insensitive match
      if (!basePrice) {
        const brandKeys = Object.keys(pricingRules);
        const matchedBrand = brandKeys.find(b => b.toLowerCase() === brand.toLowerCase());
        
        if (matchedBrand) {
          const modelKeys = Object.keys(pricingRules[matchedBrand] || {});
          const matchedModel = modelKeys.find(m => m.toLowerCase() === model.toLowerCase());
          
          if (matchedModel) {
            const storageKeys = Object.keys(pricingRules[matchedBrand][matchedModel] || {});
            const matchedStorage = storageKeys.find(s => s.toLowerCase() === storage.toLowerCase());
            
            if (matchedStorage) {
              basePrice = pricingRules[matchedBrand][matchedModel][matchedStorage]?.base;
              console.log(`Matched with case-insensitive: ${matchedBrand} ${matchedModel} ${matchedStorage}`);
            }
          }
        }
      }
      
      if (!basePrice) {
        console.log('Pricing not found. Available brands:', Object.keys(pricingRules));
        console.log('Requested:', { brand, model, storage });
        
        // Show available models for the brand if brand exists
        let availableModels = [];
        const brandKeys = Object.keys(pricingRules);
        const matchedBrand = brandKeys.find(b => b.toLowerCase() === brand.toLowerCase());
        if (matchedBrand) {
          availableModels = Object.keys(pricingRules[matchedBrand] || {});
        }
        
        return res.status(404).json({ 
          success: false,
          error: 'Pricing not found for this device configuration',
          requested: { brand, model, storage },
          availableBrands: Object.keys(pricingRules),
          availableModels: availableModels.length > 0 ? availableModels : undefined
        });
      }
    } else {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required fields. Provide either (productId + variantId) OR (brand + model + storage)' 
      });
    }

    // At this point, basePrice should be set (either from new or legacy system)
    if (!basePrice) {
      return res.status(500).json({
        success: false,
        error: 'Failed to determine base price'
      });
    }

    // If using condition-specific price from database, no multiplier needed
    let finalPrice = basePrice;
    let multiplier = 1.0;

    if (!useConditionPrice) {
      // Use condition multiplier (for Shopify products or legacy system)
      const conditionMultiplier = conditionMultipliers[condition];
      if (!conditionMultiplier) {
        return res.status(400).json({ 
          success: false,
          error: 'Invalid condition',
          requestedCondition: condition,
          availableConditions: Object.keys(conditionMultipliers)
        });
      }

      multiplier = conditionMultiplier;
      finalPrice = Math.round(basePrice * multiplier * 100) / 100; // Round to 2 decimals
    } else {
      // Already using condition-specific price, just round
      finalPrice = Math.round(basePrice * 100) / 100;
    }

    res.json({
      success: true,
      basePrice,
      conditionMultiplier: multiplier,
      finalPrice,
      currency: 'GBP',
      formattedPrice: `£${finalPrice.toFixed(2)}`,
      // Include system type for debugging
      system: useConditionPrice ? 'database-condition-specific' : (productId && variantId ? 'shopify-variant' : 'legacy')
    });

  } catch (error) {
    console.error('Error calculating price:', error);
    res.status(500).json({ 
      success: false,
      error: 'Internal server error',
      message: error.message 
    });
  }
});

// Get available brands/models/storage from pricing rules (for trade-in form)
app.get('/api/pricing/available', async (req, res) => {
  try {
    // Ensure MongoDB connection and reload rules
    await ensureMongoConnection();
    await loadPricingRules();

    const { deviceType } = req.query; // Optional: filter by device type

    // Extract available brands, models, and storage from pricing rules
    const availableData = {
      brands: [],
      modelsByBrand: {},
      storageByBrandModel: {}
    };

    // Get all brands
    availableData.brands = Object.keys(pricingRules);

    // Get models for each brand
    for (const brand of availableData.brands) {
      if (pricingRules[brand]) {
        availableData.modelsByBrand[brand] = Object.keys(pricingRules[brand]);
        
        // Get storage for each model
        for (const model of availableData.modelsByBrand[brand]) {
          if (pricingRules[brand][model]) {
            availableData.storageByBrandModel[`${brand}_${model}`] = Object.keys(pricingRules[brand][model]);
          }
        }
      }
    }

    res.json({
      success: true,
      ...availableData
    });

  } catch (error) {
    console.error('Error fetching available pricing data:', error);
    res.status(500).json({ 
      success: false,
      error: 'Internal server error' 
    });
  }
});

// Get all pricing rules (admin)
app.get('/api/pricing/rules', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    res.json({
      success: true,
      pricingRules,
      conditionMultipliers
    });

  } catch (error) {
    console.error('Error fetching pricing rules:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update pricing rules (admin)
app.post('/api/pricing/rules', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Ensure MongoDB connection
    await ensureMongoConnection();

    const { pricingRules: newRules, conditionMultipliers: newMultipliers } = req.body;

    console.log('Received pricing update:', {
      hasNewRules: !!newRules,
      hasNewMultipliers: !!newMultipliers,
      rulesCount: newRules ? Object.keys(newRules).length : 0
    });

    if (newRules) {
      pricingRules = newRules;
      console.log('Updated pricingRules:', Object.keys(pricingRules).length, 'brands');
    }
    if (newMultipliers) {
      Object.assign(conditionMultipliers, newMultipliers);
      console.log('Updated conditionMultipliers:', Object.keys(conditionMultipliers).length, 'conditions');
    }

    try {
      await savePricingRules();
      console.log('Pricing rules saved successfully');
    } catch (saveError) {
      console.error('Error during save:', saveError);
      return res.status(500).json({ 
        success: false,
        error: 'Failed to save pricing rules',
        details: saveError.message 
      });
    }

    // Reload to verify save (but don't fail if reload fails)
    try {
      await loadPricingRules();
      console.log('Pricing rules reloaded successfully');
    } catch (reloadError) {
      console.warn('Warning: Could not reload pricing rules after save:', reloadError);
      // Continue anyway - the save was successful
    }

    res.json({
      success: true,
      message: 'Pricing rules updated successfully',
      pricingRules: pricingRules,
      conditionMultipliers: conditionMultipliers
    });

  } catch (error) {
    console.error('Error updating pricing rules:', error);
    res.status(500).json({ 
      error: 'Internal server error',
      details: error.message 
    });
  }
});

// Export pricing rules to Excel format (CSV)
app.get('/api/pricing/export', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Convert to CSV format
    let csv = 'Brand,Model,Storage,Base Price\n';
    
    for (const [brand, models] of Object.entries(pricingRules)) {
      for (const [model, storages] of Object.entries(models)) {
        for (const [storage, data] of Object.entries(storages)) {
          csv += `${brand},${model},${storage},${data.base}\n`;
        }
      }
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=pricing-rules.csv');
    res.send(csv);

  } catch (error) {
    console.error('Error exporting pricing rules:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// DATABASE-DRIVEN PRODUCT MANAGEMENT ENDPOINTS
// ============================================

// Helper function to get default image URL based on device type
function getDefaultImageUrl(deviceType) {
  const defaultImages = {
    phone: 'https://cdn.shopify.com/s/files/1/1002/3944/2245/files/new_phone_brand.png?v=1764998845',
    tablet: 'https://cdn.shopify.com/s/files/1/1002/3944/2245/files/new_tablet_brand.png?v=1764998844',
    laptop: 'https://cdn.shopify.com/s/files/1/1002/3944/2245/files/new_laptop_brand.png?v=1764998845',
    gaming: 'https://cdn.shopify.com/s/files/1/1002/3944/2245/files/new_gaming_brand.png?v=1764998845',
    watch: 'https://cdn.shopify.com/s/files/1/1002/3944/2245/files/new_watch_brand.png?v=1764998844'
  };
  
  const normalizedType = (deviceType || 'phone').toLowerCase();
  return defaultImages[normalizedType] || defaultImages.phone;
}

// Get trade-in products from MongoDB (replaces Shopify API)
app.get('/api/products/trade-in', async (req, res) => {
  try {
    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({
        success: false,
        error: 'Database connection failed'
      });
    }

    const { deviceType } = req.query;

    // Build query
    const query = {};
    if (deviceType && deviceType.trim() !== '') {
      query.deviceType = deviceType.toLowerCase();
    }

    // Fetch products from MongoDB, sorted by sortOrder first, then brand/model/storage
    let products = await db.collection('trade_in_products').find(query).sort({ 
      sortOrder: 1, 
      brand: 1, 
      model: 1, 
      storage: 1 
    }).toArray();
    
    // Auto-generate slugs for products that don't have one
    const updatePromises = [];
    for (const product of products) {
      if (!product.slug || product.slug === '') {
        const slug = generateProductSlug(product.brand, product.model, product.storage, product.color);
        // Check for duplicates
        const existing = products.find(p => p.slug === slug && p._id.toString() !== product._id.toString());
        const finalSlug = existing ? slug + '-' + product._id.toString().substring(0, 8) : slug;
        
        updatePromises.push(
          db.collection('trade_in_products').updateOne(
            { _id: product._id },
            { $set: { slug: finalSlug } }
          )
        );
        product.slug = finalSlug; // Update in memory for immediate use
      }
    }
    
    // Update all products with missing slugs in parallel
    if (updatePromises.length > 0) {
      await Promise.all(updatePromises);
      console.log(`✅ Auto-generated ${updatePromises.length} slugs for products`);
    }
    
    console.log(`📦 Fetched ${products.length} products from MongoDB for deviceType: ${deviceType || 'all'}`);
    if (products.length > 0) {
      console.log(`✅ Sample product:`, {
        brand: products[0].brand,
        model: products[0].model,
        storage: products[0].storage,
        deviceType: products[0].deviceType,
        hasImage: !!products[0].imageUrl,
        hasPrices: !!products[0].prices,
        hasSlug: !!products[0].slug
      });
    }

    // Transform to match frontend expected format
    const transformedProducts = products.reduce((acc, product) => {
      // Group by brand and model
      const key = `${product.brand}_${product.model}`;
      
      if (!acc[key]) {
        // Use product image or default image based on device type
        const imageUrl = product.imageUrl || getDefaultImageUrl(product.deviceType);
        const productImage = {
          url: imageUrl,
          altText: `${product.brand} ${product.model}`,
          width: 800,
          height: 800
        };
        
        acc[key] = {
          id: product._id.toString(),
          gid: `gid://database/Product/${product._id}`,
          title: product.model,
          handle: `${product.brand}-${product.model}`.toLowerCase().replace(/\s+/g, '-'),
          vendor: product.brand,
          tags: [product.deviceType || 'phone', 'trade-in'],
          featuredImage: productImage,
          images: [productImage],
          variants: [],
          sortOrder: product.sortOrder !== undefined ? product.sortOrder : 999999 // Preserve sortOrder for sorting
        };
      } else {
        // If product group exists but doesn't have an image yet, use this product's image or default
        if (!acc[key].featuredImage || !product.imageUrl) {
          const imageUrl = product.imageUrl || getDefaultImageUrl(product.deviceType);
          const productImage = {
            url: imageUrl,
            altText: `${product.brand} ${product.model}`,
            width: 800,
            height: 800
          };
          acc[key].featuredImage = productImage;
          acc[key].images = [productImage];
        }
      }

      // Add variant (one per storage size, with all colors in options)
      // Use full gid format for both id and gid to ensure consistency
      // Support both new colors array and old single color field
      const productColors = product.colors || (product.color ? [product.color] : []);
      const firstColor = productColors.length > 0 ? productColors[0] : 'default';
      const variantGid = `gid://database/Variant/${product._id}_${product.storage}_${firstColor}`;
      const variant = {
        id: variantGid, // Use full gid format for id as well
        gid: variantGid,
        title: product.storage,
        price: product.prices?.Excellent || product.basePrice || 0, // Use Excellent as base or fallback
        availableForSale: true,
        image: {
          url: product.imageUrl || getDefaultImageUrl(product.deviceType),
          altText: `${product.brand} ${product.model} ${product.storage}`
        },
        options: {
          storage: product.storage,
          colors: productColors.length > 0 ? productColors : ['Default'], // Store all colors as array
          color: firstColor // Keep for backward compatibility
        },
        // Store full product data for pricing calculation
        _productData: product
      };

      acc[key].variants.push(variant);

      return acc;
    }, {});

    const productArray = Object.values(transformedProducts);
    
    // Sort by sortOrder to maintain the order set in admin
    productArray.sort((a, b) => {
      const aOrder = a.sortOrder !== undefined ? a.sortOrder : 999999;
      const bOrder = b.sortOrder !== undefined ? b.sortOrder : 999999;
      return aOrder - bOrder;
    });
    
    console.log(`✅ Transformed to ${productArray.length} grouped products (by brand/model), sorted by sortOrder`);
    if (productArray.length > 0) {
      console.log(`📱 Sample transformed product:`, {
        title: productArray[0].title,
        vendor: productArray[0].vendor,
        variantsCount: productArray[0].variants?.length || 0,
        hasImage: !!productArray[0].featuredImage,
        imageUrl: productArray[0].featuredImage?.url || 'No image'
      });
    }
    
    // Count products with images
    const productsWithImages = productArray.filter(p => p.featuredImage).length;
    console.log(`🖼️ Products with images: ${productsWithImages}/${productArray.length}`);

    res.json({
      success: true,
      products: productArray,
      count: productArray.length,
      deviceType: deviceType || 'all'
    });

  } catch (error) {
    console.error('Error fetching products from database:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: error.message
    });
  }
});

// Get all products (admin)
app.get('/api/products/admin', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Initialize sortOrder for products that don't have it (use sequential numbers)
    const productsWithoutSortOrder = await db.collection('trade_in_products')
      .find({ sortOrder: { $exists: false } })
      .toArray();
    
    if (productsWithoutSortOrder.length > 0) {
      // Get max sortOrder or use 0 if none exists
      const maxSortResult = await db.collection('trade_in_products')
        .find({ sortOrder: { $exists: true } }, { sort: { sortOrder: -1 }, limit: 1 })
        .toArray();
      let nextSortOrder = maxSortResult.length > 0 ? maxSortResult[0].sortOrder + 1 : 1;
      
      // Update all products without sortOrder
      for (const product of productsWithoutSortOrder) {
        await db.collection('trade_in_products').updateOne(
          { _id: product._id },
          { $set: { sortOrder: nextSortOrder } }
        );
        nextSortOrder++;
      }
      console.log(`✅ Admin: Initialized sortOrder for ${productsWithoutSortOrder.length} products`);
    }
    
    let products = await db.collection('trade_in_products').find({}).sort({ 
      sortOrder: 1, 
      brand: 1, 
      model: 1, 
      storage: 1 
    }).toArray();
    
    // Auto-generate slugs for products that don't have one
    const updatePromises = [];
    for (const product of products) {
      if (!product.slug || product.slug === '') {
        const slug = generateProductSlug(product.brand, product.model, product.storage, product.color);
        // Check for duplicates
        const existing = products.find(p => p.slug === slug && p._id.toString() !== product._id.toString());
        const finalSlug = existing ? slug + '-' + product._id.toString().substring(0, 8) : slug;
        
        updatePromises.push(
          db.collection('trade_in_products').updateOne(
            { _id: product._id },
            { $set: { slug: finalSlug } }
          )
        );
        product.slug = finalSlug; // Update in memory for immediate use
      }
    }
    
    // Update all products with missing slugs in parallel
    if (updatePromises.length > 0) {
      await Promise.all(updatePromises);
      console.log(`✅ Admin: Auto-generated ${updatePromises.length} slugs for products`);
    }
    
    // Log sample product to verify imageUrl is present
    if (products.length > 0) {
      console.log(`📦 Admin: Fetched ${products.length} products`);
      console.log(`🖼️ Sample product imageUrl:`, {
        brand: products[0].brand,
        model: products[0].model,
        storage: products[0].storage,
        imageUrl: products[0].imageUrl || 'NULL/EMPTY',
        hasImageUrl: !!products[0].imageUrl,
        hasSlug: !!products[0].slug
      });
    }

    res.json({
      success: true,
      products: products,
      count: products.length
    });

  } catch (error) {
    console.error('Error fetching products for admin:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get product by slug (for SEO-friendly URLs)
app.get('/api/products/by-slug/:slug', async (req, res) => {
  try {
    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { slug } = req.params;
    
    // Find product by slug
    const product = await db.collection('trade_in_products').findOne({ slug: slug });
    
    if (!product) {
      return res.status(404).json({ error: 'Product not found' });
    }

    // Fetch ALL variants for this product model (same brand + model)
    // This ensures all storage/color options are available
    const allVariants = await db.collection('trade_in_products').find({
      brand: product.brand,
      model: product.model,
      deviceType: product.deviceType
    }).toArray();

    console.log(`📦 Found ${allVariants.length} variants for ${product.brand} ${product.model}`);

    // Transform to match frontend expected format
    const imageUrl = product.imageUrl || getDefaultImageUrl(product.deviceType);
    const productImage = {
      url: imageUrl,
      altText: product.brand + ' ' + product.model,
      width: 800,
      height: 800
    };

    // Create variants array from all products with same brand/model
    const variants = allVariants.map(p => {
      const variantGid = 'gid://database/Variant/' + p._id + '_' + p.storage + '_' + (p.color || 'default');
      return {
        id: variantGid,
        gid: variantGid,
        title: p.storage + (p.color ? ' - ' + p.color : ''),
        price: p.prices?.Excellent || 0,
        availableForSale: true,
        image: {
          url: p.imageUrl || imageUrl,
          altText: p.brand + ' ' + p.model + ' ' + p.storage,
          width: 800,
          height: 800
        },
        options: {
          storage: p.storage,
          color: p.color || 'Default'
        },
        _productData: p // Include full product data with slug
      };
    });

    const transformedProduct = {
      id: product._id.toString(),
      gid: 'gid://database/Product/' + product._id,
      title: product.model,
      handle: (product.brand + '-' + product.model).toLowerCase().replace(/\s+/g, '-'),
      vendor: product.brand,
      tags: [product.deviceType || 'phone', 'trade-in'],
      featuredImage: productImage,
      images: [productImage],
      variants: variants, // Include ALL variants, not just the one matching the slug
      slug: product.slug,
      prices: product.prices || {}
    };

    console.log(`✅ Returning product with ${variants.length} variants`);

    res.json({
      success: true,
      product: transformedProduct
    });
  } catch (error) {
    console.error('Error fetching product by slug:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Generate slugs for existing products (migration endpoint)
app.post('/api/products/generate-slugs', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Get all products without slugs
    const products = await db.collection('trade_in_products').find({
      $or: [
        { slug: { $exists: false } },
        { slug: null },
        { slug: '' }
      ]
    }).toArray();

    let updated = 0;
    let skipped = 0;

    for (const product of products) {
      // Generate slug
      const slug = generateProductSlug(product.brand, product.model, product.storage, product.color);
      
      // Check for duplicates
      const existing = await db.collection('trade_in_products').findOne({ 
        slug: slug,
        _id: { $ne: product._id }
      });
      
      if (existing) {
        // If duplicate, append product ID to make it unique
        const uniqueSlug = slug + '-' + product._id.toString().substring(0, 8);
        await db.collection('trade_in_products').updateOne(
          { _id: product._id },
          { $set: { slug: uniqueSlug } }
        );
        updated++;
      } else {
        await db.collection('trade_in_products').updateOne(
          { _id: product._id },
          { $set: { slug: slug } }
        );
        updated++;
      }
    }

    res.json({
      success: true,
      message: 'Generated slugs for ' + updated + ' products',
      updated: updated,
      skipped: skipped
    });
  } catch (error) {
    console.error('Error generating slugs:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Create or update product (admin)
app.post('/api/products/admin', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id, brand, model, storage, color, colors, deviceType, imageUrl, prices } = req.body;

    if (!brand || !model || !storage || !deviceType) {
      return res.status(400).json({ error: 'Missing required fields: brand, model, storage, deviceType' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    // Check permissions: if updating, need pricingEdit; if creating, need pricingCreate
    const requiredPermission = id ? 'pricingEdit' : 'pricingCreate';
    if (!await hasPermission(staffIdentifier, requiredPermission)) {
      return res.status(403).json({ 
        error: `Permission denied. You need "${requiredPermission}" permission to ${id ? 'edit' : 'create'} products.` 
      });
    }
    
    // Handle colors field: support both old single color and new colors array/comma-separated
    let colorsArray = [];
    if (colors) {
      // If colors is a string (comma-separated), parse it
      if (typeof colors === 'string') {
        colorsArray = colors.split(',').map(c => c.trim()).filter(c => c && c !== '');
      } else if (Array.isArray(colors)) {
        colorsArray = colors.map(c => typeof c === 'string' ? c.trim() : c).filter(c => c && c !== '');
      }
    } else if (color) {
      // Backward compatibility: if only color is provided, use it as single color
      colorsArray = [color.trim()];
    }
    
    // Generate SEO-friendly slug (use first color for slug, or null if no colors)
    const firstColor = colorsArray.length > 0 ? colorsArray[0] : null;
    const slug = generateProductSlug(brand, model, storage, firstColor);
    
    const productData = {
      brand: brand.trim(),
      model: model.trim(),
      storage: storage.trim(),
      colors: colorsArray.length > 0 ? colorsArray : null, // Store as array
      color: firstColor, // Keep for backward compatibility
      deviceType: deviceType.toLowerCase(),
      imageUrl: imageUrl || null,
      prices: prices || {}, // { Excellent: 500, Good: 400, Fair: 300, Faulty: null }
      slug: slug, // SEO-friendly URL slug
      updatedAt: new Date().toISOString(),
      lastEditedBy: staffIdentifier
    };

    if (id) {
      // Update existing - get old data for audit
      const oldProduct = await db.collection('trade_in_products').findOne({ _id: new ObjectId(id) });
      
      // Preserve sortOrder from old product if it exists
      if (oldProduct && oldProduct.sortOrder !== undefined) {
        productData.sortOrder = oldProduct.sortOrder;
      }
      
      const result = await db.collection('trade_in_products').updateOne(
        { _id: new ObjectId(id) },
        { $set: productData }
      );
      
      // Log audit trail
      if (oldProduct && result.modifiedCount > 0) {
        const changes = [];
        if (oldProduct.brand !== productData.brand) changes.push({ field: 'brand', old: oldProduct.brand, new: productData.brand });
        if (oldProduct.model !== productData.model) changes.push({ field: 'model', old: oldProduct.model, new: productData.model });
        if (oldProduct.storage !== productData.storage) changes.push({ field: 'storage', old: oldProduct.storage, new: productData.storage });
        if (oldProduct.deviceType !== productData.deviceType) changes.push({ field: 'deviceType', old: oldProduct.deviceType, new: productData.deviceType });
        if (oldProduct.imageUrl !== productData.imageUrl) changes.push({ field: 'imageUrl', old: oldProduct.imageUrl || 'null', new: productData.imageUrl || 'null' });
        
        // Compare prices
        const conditions = ['Excellent', 'Good', 'Fair', 'Faulty'];
        conditions.forEach(condition => {
          const oldPrice = oldProduct.prices?.[condition] || null;
          const newPrice = productData.prices?.[condition] || null;
          if (oldPrice !== newPrice) {
            changes.push({ field: `price_${condition}`, old: oldPrice ? `£${oldPrice.toFixed(2)}` : 'null', new: newPrice ? `£${newPrice.toFixed(2)}` : 'null' });
          }
        });
        
        if (changes.length > 0) {
          await logAudit({
            action: 'update_product',
            resourceType: 'product',
            resourceId: id,
            staffIdentifier: staffIdentifier,
            changes: changes
          });
        }
      }
      
      res.json({ success: true, updated: result.modifiedCount > 0, id });
    } else {
      // Create new - set sortOrder to end of list (get max sortOrder + 1)
      const maxSortOrder = await db.collection('trade_in_products')
        .find({}, { sort: { sortOrder: -1 }, limit: 1 })
        .toArray();
      const nextSortOrder = maxSortOrder.length > 0 && maxSortOrder[0].sortOrder !== undefined 
        ? maxSortOrder[0].sortOrder + 1 
        : Date.now(); // Use timestamp if no sortOrder exists
      productData.sortOrder = nextSortOrder;
      
      productData.createdAt = new Date().toISOString();
      productData.createdBy = staffIdentifier;
      const result = await db.collection('trade_in_products').insertOne(productData);
      
      // Log audit trail
      await logAudit({
        action: 'create_product',
        resourceType: 'product',
        resourceId: result.insertedId.toString(),
        staffIdentifier: staffIdentifier,
        changes: [{
          field: 'status',
          old: null,
          new: 'created',
          description: `Created product: ${productData.brand} ${productData.model} ${productData.storage}`
        }]
      });
      
      res.json({ success: true, id: result.insertedId.toString() });
    }

  } catch (error) {
    console.error('Error saving product:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Update product sort order (admin) - MUST come before /:id route
app.put('/api/products/admin/sort-order', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { productIds } = req.body; // Array of product IDs in the new order
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';

    if (!Array.isArray(productIds) || productIds.length === 0) {
      return res.status(400).json({ error: 'productIds must be a non-empty array' });
    }

    // Check permission
    if (!await hasPermission(staffIdentifier, 'pricingEdit')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "pricingEdit" permission to reorder products.' 
      });
    }

    // Update sortOrder for each product based on its position in the array
    const updatePromises = productIds.map((productId, index) => {
      return db.collection('trade_in_products').updateOne(
        { _id: new ObjectId(productId) },
        { $set: { sortOrder: index + 1 } }
      );
    });

    await Promise.all(updatePromises);

    // Log audit trail
    await logAudit({
      action: 'reorder_products',
      resourceType: 'products',
      resourceId: 'multiple',
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'sortOrder',
        old: 'previous order',
        new: `reordered ${productIds.length} products`,
        description: `Reordered ${productIds.length} products`
      }]
    });

    res.json({ success: true, message: `Updated sort order for ${productIds.length} products` });

  } catch (error) {
    console.error('Error updating product sort order:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Update product (admin)
app.put('/api/products/admin/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id } = req.params;
    const { brand, model, storage, color, colors, deviceType, imageUrl, prices } = req.body;
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';

    // Check permission
    if (!await hasPermission(staffIdentifier, 'pricingEdit')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "pricingEdit" permission to edit products.' 
      });
    }

    if (!brand || !model || !storage || !deviceType) {
      return res.status(400).json({ error: 'Missing required fields: brand, model, storage, deviceType' });
    }

    // Get old product for audit
    const oldProduct = await db.collection('trade_in_products').findOne({ _id: new ObjectId(id) });
    if (!oldProduct) {
      return res.status(404).json({ error: 'Product not found' });
    }

    // Handle colors field: support both old single color and new colors array/comma-separated
    let colorsArray = [];
    if (colors) {
      // If colors is a string (comma-separated), parse it
      if (typeof colors === 'string') {
        colorsArray = colors.split(',').map(c => c.trim()).filter(c => c && c !== '');
      } else if (Array.isArray(colors)) {
        colorsArray = colors.map(c => typeof c === 'string' ? c.trim() : c).filter(c => c && c !== '');
      }
    } else if (color) {
      // Backward compatibility: if only color is provided, use it as single color
      colorsArray = [color.trim()];
    }
    
    // Generate SEO-friendly slug (use first color for slug, or null if no colors)
    const firstColor = colorsArray.length > 0 ? colorsArray[0] : null;
    const slug = generateProductSlug(brand, model, storage, firstColor);
    
    // Preserve sortOrder when updating (don't change it)
    const productData = {
      brand: brand.trim(),
      model: model.trim(),
      storage: storage.trim(),
      colors: colorsArray.length > 0 ? colorsArray : null, // Store as array
      color: firstColor, // Keep for backward compatibility
      deviceType: deviceType.toLowerCase(),
      imageUrl: imageUrl || null,
      prices: prices || {},
      slug: slug, // SEO-friendly URL slug
      updatedAt: new Date().toISOString(),
      lastEditedBy: staffIdentifier
      // Note: sortOrder is NOT included here - it will be preserved from oldProduct
    };

    // Preserve sortOrder from old product if it exists
    if (oldProduct.sortOrder !== undefined) {
      productData.sortOrder = oldProduct.sortOrder;
    }
    
    const result = await db.collection('trade_in_products').updateOne(
      { _id: new ObjectId(id) },
      { $set: productData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }

    // Log audit trail
    if (result.modifiedCount > 0) {
      const changes = [];
      if (oldProduct.brand !== productData.brand) changes.push({ field: 'brand', old: oldProduct.brand, new: productData.brand });
      if (oldProduct.model !== productData.model) changes.push({ field: 'model', old: oldProduct.model, new: productData.model });
      if (oldProduct.storage !== productData.storage) changes.push({ field: 'storage', old: oldProduct.storage, new: productData.storage });
      const oldColors = oldProduct.colors ? (Array.isArray(oldProduct.colors) ? oldProduct.colors.join(', ') : oldProduct.colors) : (oldProduct.color || '');
      const newColors = productData.colors ? (Array.isArray(productData.colors) ? productData.colors.join(', ') : productData.colors) : (productData.color || '');
      if (oldColors !== newColors) changes.push({ field: 'colors', old: oldColors || 'null', new: newColors || 'null' });
      if (oldProduct.deviceType !== productData.deviceType) changes.push({ field: 'deviceType', old: oldProduct.deviceType, new: productData.deviceType });
      if (oldProduct.imageUrl !== productData.imageUrl) changes.push({ field: 'imageUrl', old: oldProduct.imageUrl || 'null', new: productData.imageUrl || 'null' });
      
      // Compare prices
      const conditions = ['Excellent', 'Good', 'Fair', 'Faulty'];
      conditions.forEach(condition => {
        const oldPrice = oldProduct.prices?.[condition] || null;
        const newPrice = productData.prices?.[condition] || null;
        if (oldPrice !== newPrice) {
          changes.push({ field: `price_${condition}`, old: oldPrice ? `£${oldPrice.toFixed(2)}` : 'null', new: newPrice ? `£${newPrice.toFixed(2)}` : 'null' });
        }
      });
      
      if (changes.length > 0) {
        await logAudit({
          action: 'update_product',
          resourceType: 'product',
          resourceId: id,
          staffIdentifier: staffIdentifier,
          changes: changes
        });
      }
    }

    res.json({ success: true, updated: result.modifiedCount > 0, id });

  } catch (error) {
    console.error('Error updating product:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Delete product (admin)
app.delete('/api/products/admin/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id } = req.params;
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(staffIdentifier, 'pricingDelete')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "pricingDelete" permission to delete products.' 
      });
    }
    
    // Get product before deletion for audit log
    const product = await db.collection('trade_in_products').findOne({ _id: new ObjectId(id) });
    
    const result = await db.collection('trade_in_products').deleteOne({ _id: new ObjectId(id) });

    // Log audit trail
    if (product && result.deletedCount > 0) {
      await logAudit({
        action: 'delete_product',
        resourceType: 'product',
        resourceId: id,
        staffIdentifier: staffIdentifier,
        changes: [{
          field: 'status',
          old: 'exists',
          new: 'deleted',
          description: `Deleted product: ${product.brand} ${product.model} ${product.storage}`
        }]
      });
    }

    res.json({ success: true, deleted: result.deletedCount > 0 });

  } catch (error) {
    console.error('Error deleting product:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// AUDIT LOGGING SYSTEM
// ============================================

// Helper function to log audit trail
async function logAudit({ action, resourceType, resourceId, staffIdentifier, changes, metadata = {} }) {
  try {
    await ensureMongoConnection();
    if (!db) return;

    const auditLog = {
      action: action, // 'create_product', 'update_product', 'delete_product', 'update_submission', 'update_status', etc.
      resourceType: resourceType, // 'product', 'submission', 'pricing'
      resourceId: resourceId, // Product ID, Submission ID, etc.
      staffIdentifier: staffIdentifier || 'Unknown', // Staff email or identifier
      changes: changes || [], // Array of { field, old, new, description }
      metadata: metadata, // Additional context
      timestamp: new Date().toISOString(),
      createdAt: new Date()
    };

    await db.collection('audit_logs').insertOne(auditLog);
    console.log('📝 Audit log created:', { action, resourceType, resourceId, staffIdentifier });
  } catch (error) {
    console.error('Error logging audit:', error);
    // Don't throw - audit logging failure shouldn't break the main operation
  }
}

// Get audit logs (admin only)
app.get('/api/audit-logs', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Check permission
    const staffEmail = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffEmail, 'auditView')) {
      return res.status(403).json({ error: 'Permission denied. You need "auditView" permission to view audit logs.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { resourceType, resourceId, staffIdentifier, limit = 100 } = req.query;
    
    const query = {};
    if (resourceType) query.resourceType = resourceType;
    if (resourceId) query.resourceId = resourceId;
    if (staffIdentifier) query.staffIdentifier = staffIdentifier;

    const logs = await db.collection('audit_logs')
      .find(query)
      .sort({ timestamp: -1 })
      .limit(parseInt(limit))
      .toArray();

    res.json({
      success: true,
      logs: logs,
      count: logs.length
    });

  } catch (error) {
    console.error('Error fetching audit logs:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// STAFF MANAGEMENT SYSTEM
// ============================================

// Helper function to get staff member with permissions
async function getStaffMember(staffEmail) {
  if (!staffEmail || staffEmail === 'Unknown') {
    return null;
  }

  try {
    await ensureMongoConnection();
    if (!db) {
      return null;
    }

    const staff = await db.collection('staff_members').findOne({ 
      email: staffEmail.trim().toLowerCase(),
      active: true 
    });

    return staff;
  } catch (error) {
    console.error('Error getting staff member:', error);
    return null;
  }
}

// Helper function to check if staff has specific permission
async function hasPermission(staffEmail, permission) {
  try {
    if (!staffEmail || !permission) {
      return false;
    }

    const staff = await getStaffMember(staffEmail);
    if (!staff) {
      return false;
    }

    // Admin and manager roles have all permissions
    if (staff.role === 'admin' || staff.role === 'manager') {
      return true;
    }

    // Check specific permission
    const permissions = staff.permissions || {};
    
    // Support legacy permissions for backward compatibility
    if (permission === 'pricingView' && (permissions.pricingView || permissions.pricing)) return true;
    if (permission === 'pricingEdit' && (permissions.pricingEdit || permissions.pricing)) return true;
    if (permission === 'pricingCreate' && permissions.pricingCreate) return true;
    if (permission === 'pricingDelete' && permissions.pricingDelete) return true;
    if (permission === 'pricingBulk' && permissions.pricingBulk) return true;
    if (permission === 'pricingImport' && permissions.pricingImport) return true;
    
    if (permission === 'tradeInView' && (permissions.tradeInView || permissions.tradeIn)) return true;
    if (permission === 'tradeInEdit' && (permissions.tradeInEdit || permissions.tradeIn)) return true;
    if (permission === 'tradeInStatus' && permissions.tradeInStatus) return true;
    if (permission === 'tradeInCredit' && permissions.tradeInCredit) return true;
    if (permission === 'tradeInPayment' && permissions.tradeInPayment) return true;
    
    if (permission === 'auditView' && permissions.auditView) return true;
    if (permission === 'staffView' && permissions.staffView) return true;
    if (permission === 'staffEdit' && permissions.staffEdit) return true;
    
    if (permission === 'backupView' && permissions.backupView) return true;
    if (permission === 'backupCreate' && permissions.backupCreate) return true;
    if (permission === 'backupRestore' && permissions.backupRestore) return true;
    if (permission === 'backupDelete' && permissions.backupDelete) return true;
    if (permission === 'backupConfig' && permissions.backupConfig) return true;

    return permissions[permission] === true;
  } catch (error) {
    console.error('Error in hasPermission:', error);
    return false; // Fail closed - deny permission on error
  }
}

// Helper function to verify admin access
async function verifyAdminAccess(staffEmail) {
  const staff = await getStaffMember(staffEmail);
  if (!staff) {
    return false;
  }

  // Check if any admins exist (bootstrap mode)
  const adminCount = await db.collection('staff_members').countDocuments({ 
    role: { $in: ['admin', 'manager'] },
    active: true 
  });

  // If no admins exist, allow first-time setup
  if (adminCount === 0) {
    console.log('⚠️ No admins found - allowing bootstrap access');
    return true;
  }

  // Only admin and manager roles can access admin pages
  return staff.role === 'admin' || staff.role === 'manager';
}

// Verify staff admin access (for Audit and Staff Admin pages - admin/manager only)
app.get('/api/staff/verify-admin', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const staffEmail = req.headers['x-staff-identifier'] || req.query.email;
    if (!staffEmail) {
      return res.status(400).json({ error: 'Staff email required' });
    }

    const staff = await db.collection('staff_members').findOne({ 
      email: staffEmail.trim().toLowerCase(),
      active: true 
    });

    if (!staff) {
      return res.json({
        success: true,
        isAdmin: false,
        message: 'Staff member not found or inactive'
      });
    }

    // Check if staff has admin or manager role (for Audit/Staff Admin pages)
    const isAdmin = staff.role === 'admin' || staff.role === 'manager';
    
    res.json({
      success: true,
      isAdmin: isAdmin,
      role: staff.role,
      permissions: staff.permissions
    });

  } catch (error) {
    console.error('Error verifying admin access:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Verify staff access for Pricing/Trade-In pages (allows staff, manager, admin)
app.get('/api/staff/verify-access', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const staffEmail = req.headers['x-staff-identifier'] || req.query.email;
    if (!staffEmail) {
      return res.status(400).json({ error: 'Staff email required' });
    }

    const staff = await db.collection('staff_members').findOne({ 
      email: staffEmail.trim().toLowerCase(),
      active: true 
    });

    if (!staff) {
      return res.json({
        success: true,
        hasAccess: false,
        message: 'Staff member not found or inactive'
      });
    }

    // All active staff (staff, manager, admin) can access Pricing/Trade-In pages
    const hasAccess = true; // Any active staff member can access
    
    res.json({
      success: true,
      hasAccess: hasAccess,
      role: staff.role,
      permissions: staff.permissions
    });

  } catch (error) {
    console.error('Error verifying staff access:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Send verification code to email
app.post('/api/staff/send-verification-code', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { email } = req.body;
    if (!email || !email.trim()) {
      return res.status(400).json({ error: 'Email is required' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Check if any admins exist (bootstrap mode)
    const adminCount = await db.collection('staff_members').countDocuments({ 
      role: { $in: ['admin', 'manager'] },
      active: true 
    });

    // Check if email exists in staff members
    const staff = await db.collection('staff_members').findOne({ 
      email: email.trim().toLowerCase(),
      active: true 
    });

    // If not in bootstrap mode and email doesn't exist, deny
    if (!staff && adminCount > 0) {
      return res.status(404).json({ 
        success: false,
        error: 'Email not found in staff database. Please contact administrator.' 
      });
    }

    // In bootstrap mode, allow sending code even if email doesn't exist
    const isBootstrapMode = adminCount === 0;

    // Generate 6-digit verification code
    const verificationCode = Math.floor(100000 + Math.random() * 900000).toString();
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000); // 10 minutes from now

    // Store verification code in database
    await db.collection('verification_codes').updateOne(
      { email: email.trim().toLowerCase() },
      {
        $set: {
          code: verificationCode,
          expiresAt: expiresAt,
          createdAt: new Date(),
          attempts: 0
        }
      },
      { upsert: true }
    );

    // Send verification code via email
    try {
      const displayName = staff ? (staff.name || email) : email;
      const emailSubject = isBootstrapMode 
        ? 'Admin Setup - Verification Code' 
        : 'Admin Access Verification Code';
      const emailBody = isBootstrapMode
        ? `
          <h2>Admin Setup - Verification Code</h2>
          <p>Hello,</p>
          <p>You are setting up the first admin account. Use the verification code below to complete the setup:</p>
          <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center;">
            <p style="font-size: 32px; font-weight: bold; letter-spacing: 4px; color: #1a73e8; margin: 0;">${verificationCode}</p>
          </div>
          <p><strong>This code will expire in 10 minutes.</strong></p>
          <p>After verification, you will be able to create your admin account.</p>
        `
        : `
          <h2>Admin Access Verification</h2>
          <p>Hello ${displayName},</p>
          <p>You requested access to the admin panel. Use the verification code below to complete your login:</p>
          <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center;">
            <p style="font-size: 32px; font-weight: bold; letter-spacing: 4px; color: #1a73e8; margin: 0;">${verificationCode}</p>
          </div>
          <p><strong>This code will expire in 10 minutes.</strong></p>
          <p>If you didn't request this code, please ignore this email or contact your administrator.</p>
        `;

      await transporter.sendMail({
        from: SMTP_FROM,
        to: email.trim(),
        subject: emailSubject,
        html: emailBody
      });
    } catch (emailError) {
      console.error('Error sending verification email:', emailError);
      return res.status(500).json({ 
        success: false,
        error: 'Failed to send verification email. Please check email configuration.' 
      });
    }

    res.json({
      success: true,
      message: 'Verification code sent to your email',
      expiresIn: 600, // 10 minutes in seconds
      isBootstrapMode: isBootstrapMode
    });

  } catch (error) {
    console.error('Error sending verification code:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Verify code and grant access
app.post('/api/staff/verify-code', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { email, code } = req.body;
    if (!email || !email.trim()) {
      return res.status(400).json({ error: 'Email is required' });
    }
    if (!code || !code.trim()) {
      return res.status(400).json({ error: 'Verification code is required' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Get verification code from database
    const verificationRecord = await db.collection('verification_codes').findOne({
      email: email.trim().toLowerCase()
    });

    if (!verificationRecord) {
      return res.status(400).json({
        success: false,
        error: 'No verification code found. Please request a new code.'
      });
    }

    // Check if code has expired
    if (new Date() > new Date(verificationRecord.expiresAt)) {
      await db.collection('verification_codes').deleteOne({ email: email.trim().toLowerCase() });
      return res.status(400).json({
        success: false,
        error: 'Verification code has expired. Please request a new code.'
      });
    }

    // Check attempts (max 5 attempts)
    if (verificationRecord.attempts >= 5) {
      await db.collection('verification_codes').deleteOne({ email: email.trim().toLowerCase() });
      return res.status(400).json({
        success: false,
        error: 'Too many failed attempts. Please request a new code.'
      });
    }

    // Verify code
    if (verificationRecord.code !== code.trim()) {
      // Increment attempts
      await db.collection('verification_codes').updateOne(
        { email: email.trim().toLowerCase() },
        { $inc: { attempts: 1 } }
      );
      return res.status(400).json({
        success: false,
        error: 'Invalid verification code. Please try again.',
        attemptsRemaining: 5 - (verificationRecord.attempts + 1)
      });
    }

    // Check if any admins exist (bootstrap mode)
    const adminCount = await db.collection('staff_members').countDocuments({ 
      role: { $in: ['admin', 'manager'] },
      active: true 
    });
    const isBootstrapMode = adminCount === 0;

    // Code is valid - get staff info
    const staff = await db.collection('staff_members').findOne({ 
      email: email.trim().toLowerCase(),
      active: true 
    });

    // In bootstrap mode, if staff doesn't exist, allow creating first admin
    if (!staff && isBootstrapMode) {
      // Delete used verification code
      await db.collection('verification_codes').deleteOne({ email: email.trim().toLowerCase() });
      
      // Return success with bootstrap flag - frontend will create the admin
      return res.json({
        success: true,
        message: 'Email verified successfully. You can now create your admin account.',
        isBootstrapMode: true,
        email: email.trim().toLowerCase(),
        needsAccountCreation: true
      });
    }

    if (!staff) {
      return res.status(404).json({
        success: false,
        error: 'Staff member not found'
      });
    }

    // Delete used verification code
    await db.collection('verification_codes').deleteOne({ email: email.trim().toLowerCase() });

    // Return success with staff info
    res.json({
      success: true,
      message: 'Email verified successfully',
      staff: {
        email: staff.email,
        name: staff.name,
        role: staff.role,
        permissions: staff.permissions
      },
      isAdmin: staff.role === 'admin' || staff.role === 'manager',
      hasAccess: true, // All active staff can access pricing/trade-in
      isBootstrapMode: false
    });

  } catch (error) {
    console.error('Error verifying code:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get all staff members (admin only)
app.get('/api/staff', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Check permission
    const staffEmail = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffEmail, 'staffView')) {
      return res.status(403).json({ error: 'Permission denied. You need "staffView" permission to view staff members.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const staff = await db.collection('staff_members')
      .find({})
      .sort({ createdAt: -1 })
      .toArray();

    res.json({
      success: true,
      staff: staff,
      count: staff.length
    });

  } catch (error) {
    console.error('Error fetching staff:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Add new staff member (admin only)
app.post('/api/staff', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const adminIdentifier = req.headers['x-staff-identifier'] || req.body.adminIdentifier || 'Unknown';
    
    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Check if any admins exist (bootstrap mode)
    const adminCount = await db.collection('staff_members').countDocuments({ 
      role: { $in: ['admin', 'manager'] },
      active: true 
    });

    // If admins exist, check permission
    if (adminCount > 0) {
      if (!await hasPermission(adminIdentifier, 'staffEdit')) {
        return res.status(403).json({ error: 'Permission denied. You need "staffEdit" permission to add staff members.' });
      }
    }

    const { email, name, role, permissions, active = true } = req.body;

    if (!email || !email.trim()) {
      return res.status(400).json({ error: 'Email is required' });
    }

    // Check if staff member already exists
    const existing = await db.collection('staff_members').findOne({ email: email.trim().toLowerCase() });
    if (existing) {
      return res.status(400).json({ error: 'Staff member with this email already exists' });
    }

    // If this is the first admin being created, ensure role is admin
    const finalRole = (adminCount === 0 && role !== 'staff') ? 'admin' : (role || 'staff');

    const staffMember = {
      email: email.trim().toLowerCase(),
      name: name || email.trim(),
      role: finalRole,
      permissions: permissions || {
        pricing: true,
        tradeIn: true,
        readOnly: false
      },
      active: active !== false,
      createdAt: new Date().toISOString(),
      createdBy: adminIdentifier,
      updatedAt: new Date().toISOString()
    };

    const result = await db.collection('staff_members').insertOne(staffMember);

    // Log audit trail (skip if bootstrap mode)
    if (adminCount > 0) {
      await logAudit({
        action: 'add_staff',
        resourceType: 'staff',
        resourceId: result.insertedId.toString(),
        staffIdentifier: adminIdentifier,
        changes: [{
          field: 'status',
          old: null,
          new: 'added',
          description: `Added staff member: ${staffMember.email} (${staffMember.role})`
        }]
      });
    }

    res.json({
      success: true,
      staff: {
        _id: result.insertedId.toString(),
        ...staffMember
      },
      isFirstAdmin: adminCount === 0
    });

  } catch (error) {
    console.error('Error adding staff:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Update staff member (admin only)
app.put('/api/staff/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const adminIdentifier = req.headers['x-staff-identifier'] || req.body.adminIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(adminIdentifier, 'staffEdit')) {
      return res.status(403).json({ error: 'Permission denied. You need "staffEdit" permission to update staff members.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id } = req.params;
    const { name, role, permissions, active } = req.body;

    // Get old staff member for audit
    const oldStaff = await db.collection('staff_members').findOne({ _id: new ObjectId(id) });
    if (!oldStaff) {
      return res.status(404).json({ error: 'Staff member not found' });
    }

    const updateData = {
      updatedAt: new Date().toISOString(),
      updatedBy: adminIdentifier
    };

    if (name !== undefined) updateData.name = name;
    if (role !== undefined) updateData.role = role;
    if (permissions !== undefined) updateData.permissions = permissions;
    if (active !== undefined) updateData.active = active;

    const result = await db.collection('staff_members').updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Staff member not found' });
    }

    // Log audit trail
    const changes = [];
    if (name !== undefined && name !== oldStaff.name) changes.push({ field: 'name', old: oldStaff.name, new: name });
    if (role !== undefined && role !== oldStaff.role) changes.push({ field: 'role', old: oldStaff.role, new: role });
    if (active !== undefined && active !== oldStaff.active) changes.push({ field: 'active', old: oldStaff.active, new: active });
    
    if (changes.length > 0) {
      await logAudit({
        action: 'update_staff',
        resourceType: 'staff',
        resourceId: id,
        staffIdentifier: adminIdentifier,
        changes: changes
      });
    }

    res.json({
      success: true,
      updated: result.modifiedCount > 0
    });

  } catch (error) {
    console.error('Error updating staff:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Delete staff member (admin only)
app.delete('/api/staff/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const adminIdentifier = req.headers['x-staff-identifier'] || req.body.adminIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(adminIdentifier, 'staffEdit')) {
      return res.status(403).json({ error: 'Permission denied. You need "staffEdit" permission to delete staff members.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id } = req.params;

    // Get staff member before deletion for audit
    const staff = await db.collection('staff_members').findOne({ _id: new ObjectId(id) });
    if (!staff) {
      return res.status(404).json({ error: 'Staff member not found' });
    }

    const result = await db.collection('staff_members').deleteOne({ _id: new ObjectId(id) });

    // Log audit trail
    if (result.deletedCount > 0) {
      await logAudit({
        action: 'delete_staff',
        resourceType: 'staff',
        resourceId: id,
        staffIdentifier: adminIdentifier,
        changes: [{
          field: 'status',
          old: 'active',
          new: 'deleted',
          description: `Deleted staff member: ${staff.email}`
        }]
      });
    }

    res.json({
      success: true,
      deleted: result.deletedCount > 0
    });

  } catch (error) {
    console.error('Error deleting staff:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Validate Excel file before import (admin) - returns validation results without importing
app.post('/api/products/validate-excel', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { fileData, fileName } = req.body;

    if (!fileData) {
      return res.status(400).json({ error: 'No file data provided' });
    }

    // Decode base64
    const buffer = Buffer.from(fileData, 'base64');
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    // Reuse the same parsing logic from import-excel
    const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1');
    let mainHeaderRowIndex = -1;
    let subHeaderRowIndex = -1;
    let mainHeaderRow = null;
    let subHeaderRow = null;
    
    // Find headers (same logic as import)
    for (let row = 0; row <= Math.min(4, range.e.r); row++) {
      const rowData = [];
      for (let col = range.s.c; col <= range.e.c; col++) {
        const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
        const cell = worksheet[cellAddress];
        const value = cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '';
        rowData.push(value);
      }
      
      const hasDevice = rowData.some(cell => /^device$/i.test(cell));
      const hasBrand = rowData.some(cell => /^brand$/i.test(cell));
      const hasModel = rowData.some(cell => /^model$/i.test(cell));
      const hasExcellent = rowData.some(cell => /^excellent$/i.test(cell));
      const hasGood = rowData.some(cell => /^good$/i.test(cell));
      const hasFair = rowData.some(cell => /^fair$/i.test(cell));
      const hasFaulty = rowData.some(cell => /^faulty$/i.test(cell));
      
      if (hasDevice && hasBrand && hasModel && (hasExcellent || hasGood || hasFair || hasFaulty)) {
        mainHeaderRowIndex = row;
        mainHeaderRow = rowData;
        
        if (row + 1 <= range.e.r) {
          const subRowData = [];
          for (let col = range.s.c; col <= range.e.c; col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row + 1, c: col });
            const cell = worksheet[cellAddress];
            subRowData.push(cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '');
          }
          subHeaderRowIndex = row + 1;
          subHeaderRow = subRowData;
        }
        break;
      }
    }
    
    if (mainHeaderRowIndex === -1) {
      mainHeaderRowIndex = 0;
      subHeaderRowIndex = 1;
      const firstRow = [];
      const secondRow = [];
      for (let col = range.s.c; col <= range.e.c; col++) {
        const cellAddress1 = XLSX.utils.encode_cell({ r: 0, c: col });
        const cellAddress2 = XLSX.utils.encode_cell({ r: 1, c: col });
        const cell1 = worksheet[cellAddress1];
        const cell2 = worksheet[cellAddress2];
        firstRow.push(cell1 ? (cell1.v !== undefined ? String(cell1.v).trim() : '') : '');
        secondRow.push(cell2 ? (cell2.v !== undefined ? String(cell2.v).trim() : '') : '');
      }
      mainHeaderRow = firstRow;
      subHeaderRow = secondRow;
    }
    
    // Build column mapping (same as import)
    const columnMapping = [];
    const columnConditionByIndex = {};
    let currentCondition = null;
    
    const conditionStarts = {};
    for (let col = range.s.c; col <= range.e.c; col++) {
      const colIdx = col - range.s.c;
      const mainHeader = mainHeaderRow[colIdx] || '';
      const mainUpper = mainHeader.toUpperCase().trim();
      
      if (mainUpper === 'EXCELLENT' && !conditionStarts['Excellent']) {
        conditionStarts['Excellent'] = colIdx;
      } else if (mainUpper === 'GOOD' && !conditionStarts['Good']) {
        conditionStarts['Good'] = colIdx;
      } else if (mainUpper === 'FAIR' && !conditionStarts['Fair']) {
        conditionStarts['Fair'] = colIdx;
      } else if (mainUpper === 'FAULTY' && !conditionStarts['Faulty']) {
        conditionStarts['Faulty'] = colIdx;
      }
    }
    
    const conditionEnds = {};
    const sortedConditions = Object.entries(conditionStarts).sort((a, b) => a[1] - b[1]);
    for (let i = 0; i < sortedConditions.length; i++) {
      const [condition, startIdx] = sortedConditions[i];
      if (i < sortedConditions.length - 1) {
        conditionEnds[condition] = sortedConditions[i + 1][1];
      } else {
        let endIdx = range.e.c - range.s.c + 1;
        for (let col = range.s.c; col <= range.e.c; col++) {
          const colIdx = col - range.s.c;
          const mainHeader = mainHeaderRow[colIdx] || '';
          const mainUpper = mainHeader.toUpperCase().trim();
          if (mainUpper.includes('IMAGE') || mainUpper.includes('URL') || mainUpper.includes('IMG')) {
            endIdx = colIdx;
            break;
          }
        }
        conditionEnds[condition] = endIdx;
      }
    }
    
    for (let col = range.s.c; col <= range.e.c; col++) {
      const colIdx = col - range.s.c;
      const mainHeader = mainHeaderRow[colIdx] || '';
      const subHeader = subHeaderRow && subHeaderRow[colIdx] ? subHeaderRow[colIdx] : '';
      
      const mainUpper = mainHeader.toUpperCase().trim();
      let columnName;
      
      if (mainUpper === 'EXCELLENT' || mainUpper === 'GOOD' || mainUpper === 'FAIR' || mainUpper === 'FAULTY') {
        if (mainUpper === 'EXCELLENT') currentCondition = 'Excellent';
        else if (mainUpper === 'GOOD') currentCondition = 'Good';
        else if (mainUpper === 'FAIR') currentCondition = 'Fair';
        else if (mainUpper === 'FAULTY') currentCondition = 'Faulty';
        
        columnName = subHeader || mainHeader || `__EMPTY_${colIdx}`;
        if (currentCondition) {
          columnConditionByIndex[colIdx] = currentCondition;
        }
      } else {
        let belongsToCondition = null;
        for (const [condition, startIdx] of Object.entries(conditionStarts)) {
          const endIdx = conditionEnds[condition] || (range.e.c - range.s.c + 1);
          if (colIdx >= startIdx && colIdx < endIdx) {
            belongsToCondition = condition;
            break;
          }
        }
        
        if (belongsToCondition) {
          if (subHeader) {
            columnName = subHeader;
            columnConditionByIndex[colIdx] = belongsToCondition;
          } else {
            columnName = mainHeader || `__EMPTY_${colIdx}`;
            if (mainHeader && mainHeader.trim() !== '') {
              columnConditionByIndex[colIdx] = belongsToCondition;
            }
          }
          currentCondition = belongsToCondition;
        } else {
          columnName = mainHeader || subHeader || `__EMPTY_${colIdx}`;
          if (mainUpper && mainUpper !== '' && !mainUpper.includes('EMPTY') && !mainUpper.includes('IMAGE') && !mainUpper.includes('URL')) {
            currentCondition = null;
          }
        }
      }
      
      columnMapping.push(columnName);
    }
    
    // Read data rows
    const dataStartRow = subHeaderRowIndex !== -1 ? subHeaderRowIndex + 1 : mainHeaderRowIndex + 1;
    const allRows = [];
    for (let row = dataStartRow; row <= range.e.r; row++) {
      const rowData = {};
      const rowDataByIndex = {};
      for (let col = range.s.c; col <= range.e.c; col++) {
        const colIdx = col - range.s.c;
        const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
        const cell = worksheet[cellAddress];
        const value = cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '';
        const headerName = columnMapping[colIdx] || `__EMPTY_${colIdx}`;
        
        rowData[headerName] = value;
        rowDataByIndex[colIdx] = value;
      }
      rowData._byIndex = rowDataByIndex;
      if (Object.values(rowData).some(v => v && v !== '' && v !== rowDataByIndex)) {
        allRows.push(rowData);
      }
    }
    
    const data = allRows;
    
    // Helper function to find column value
    const getColumnValue = (row, possibleNames) => {
      for (const name of possibleNames) {
        if (row[name] !== undefined && row[name] !== null && row[name] !== '') {
          return row[name];
        }
        const rowKeys = Object.keys(row);
        const matchedKey = rowKeys.find(key => key.toLowerCase().trim() === name.toLowerCase().trim());
        if (matchedKey && row[matchedKey] !== undefined && row[matchedKey] !== null && row[matchedKey] !== '') {
          return row[matchedKey];
        }
      }
      return null;
    };
    
    // Validation logic
    const errors = [];
    const previewData = [];
    const storageOptions = ['64GB', '128GB', '256GB', '512GB', '1TB', '2TB'];
    
    // URL validation regex
    const urlRegex = /^(https?:\/\/|www\.)[^\s/$.?#].[^\s]*$/i;
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const excelRowNumber = dataStartRow + i + 1; // Excel row number (1-based, including headers)
      const rowErrors = [];
      
      // Get basic info
      const brand = getColumnValue(row, ['Brand', 'brand', 'Brand Name', 'BRAND', 'BrandName']);
      const model = getColumnValue(row, ['Model', 'model', 'Product Model', 'MODEL', 'ModelName', 'Product']);
      const deviceType = (getColumnValue(row, ['Device Type', 'deviceType', 'DeviceType', 'Device', 'device', 'DEVICE', 'Type', 'type']) || 'phone').toLowerCase();
      const color = getColumnValue(row, ['Color', 'color', 'Colour', 'colour', 'COLOR']) || null;
      
      // Find image URL column
      let imageUrl = null;
      const imageColumnNames = ['Image URL', 'imageUrl', 'ImageUrl', 'Image', 'image', 'Image Link', 'imageLink', 'image_url', 'IMAGE_URL'];
      
      for (const colName of imageColumnNames) {
        const val = getColumnValue(row, [colName]);
        if (val && val.trim() !== '') {
          imageUrl = val.trim();
          break;
        }
      }
      
      if (!imageUrl) {
        const allCols = Object.keys(row);
        for (const col of allCols) {
          const colLower = col.toLowerCase();
          if ((colLower.includes('image') || colLower.includes('img') || (colLower.includes('url') && !colLower.includes('excellent') && !colLower.includes('good') && !colLower.includes('fair') && !colLower.includes('faulty'))) && 
              row[col] && row[col] !== null && row[col] !== undefined && row[col].toString().trim() !== '') {
            imageUrl = row[col].toString().trim();
            break;
          }
        }
      }
      
      // Validate required fields
      if (!brand || !model) {
        const missing = [];
        if (!brand) missing.push('Brand');
        if (!model) missing.push('Model');
        rowErrors.push({
          column: missing.join(', '),
          error: `Missing required fields: ${missing.join(', ')}`
        });
      }
      
      // Validate image URL format if provided
      if (imageUrl && !urlRegex.test(imageUrl)) {
        rowErrors.push({
          column: 'Image URL',
          error: 'Is not a valid http url'
        });
      }
      
      // Validate price columns (check all storage options and conditions)
      const allColumns = Object.keys(row);
      const columnNameToIndex = {};
      columnMapping.forEach((colName, idx) => {
        if (!columnNameToIndex[colName]) {
          columnNameToIndex[colName] = [];
        }
        columnNameToIndex[colName].push(idx);
      });
      
      for (let storageIdx = 0; storageIdx < storageOptions.length; storageIdx++) {
        const storage = storageOptions[storageIdx];
        const storageNormalized = storage.toUpperCase().replace(/\s+/g, '');
        
        const matchingIndices = [];
        columnMapping.forEach((colName, idx) => {
          const colUpper = colName.toUpperCase().replace(/\s+/g, '');
          if (colUpper === storageNormalized || 
              colUpper.includes(storageNormalized) || 
              storageNormalized.includes(colUpper)) {
            matchingIndices.push(idx);
          }
        });
        
        for (const colIdx of matchingIndices) {
          const condition = columnConditionByIndex[colIdx];
          const colName = columnMapping[colIdx];
          
          const cellValue = row._byIndex && row._byIndex[colIdx] !== undefined 
            ? row._byIndex[colIdx] 
            : row[colName];
          
          if (condition && cellValue !== undefined && cellValue !== null && cellValue !== '') {
            const val = parseFloat(cellValue);
            if (isNaN(val) || val < 0) {
              rowErrors.push({
                column: `${condition} - ${colName}`,
                error: 'Is not a number'
              });
            }
          }
        }
      }
      
      // Add row errors to main errors array
      rowErrors.forEach(err => {
        errors.push({
          row: excelRowNumber,
          column: err.column,
          error: err.error
        });
      });
      
      // Add to preview data (include all fields for display)
      previewData.push({
        row: excelRowNumber,
        brand: brand || '',
        model: model || '',
        deviceType: deviceType || '',
        color: color || '',
        imageUrl: imageUrl || '',
        hasErrors: rowErrors.length > 0,
        errors: rowErrors
      });
    }
    
    res.json({
      success: true,
      fileName: fileName || 'import.xlsx',
      totalRows: data.length,
      errorCount: errors.length,
      validRows: data.length - errors.length,
      errors: errors,
      previewData: previewData
    });

  } catch (error) {
    console.error('Error validating Excel:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Import Excel file (admin)
app.post('/api/products/import-excel', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Note: In production, you'd use multer or similar to handle file upload
    // For now, expecting base64 encoded Excel file in request body
    const { fileData, fileName } = req.body;

    if (!fileData) {
      return res.status(400).json({ error: 'No file data provided' });
    }

    // Decode base64
    const buffer = Buffer.from(fileData, 'base64');
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    
    // Get all rows as arrays to find the header rows
    // Excel structure: Row 1 = "UPDATED: date", Row 2 = Main headers (DEVICE, BRAND, MODEL, EXCELLENT, GOOD, FAIR, FAULTY, image_url)
    // Row 3 = Sub-headers (64GB, 128GB, etc. under each condition), Row 4+ = Data
    const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1');
    let mainHeaderRowIndex = -1; // Row 2 (index 1)
    let subHeaderRowIndex = -1;  // Row 3 (index 2)
    let mainHeaderRow = null;
    let subHeaderRow = null;
    
    // Try first 5 rows to find headers
    for (let row = 0; row <= Math.min(4, range.e.r); row++) {
      const rowData = [];
      for (let col = range.s.c; col <= range.e.c; col++) {
        const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
        const cell = worksheet[cellAddress];
        const value = cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '';
        rowData.push(value);
      }
      
      // Check if this is the main header row (has DEVICE, BRAND, MODEL, EXCELLENT, GOOD, FAIR, FAULTY)
      const hasDevice = rowData.some(cell => /^device$/i.test(cell));
      const hasBrand = rowData.some(cell => /^brand$/i.test(cell));
      const hasModel = rowData.some(cell => /^model$/i.test(cell));
      const hasExcellent = rowData.some(cell => /^excellent$/i.test(cell));
      const hasGood = rowData.some(cell => /^good$/i.test(cell));
      const hasFair = rowData.some(cell => /^fair$/i.test(cell));
      const hasFaulty = rowData.some(cell => /^faulty$/i.test(cell));
      
      if (hasDevice && hasBrand && hasModel && (hasExcellent || hasGood || hasFair || hasFaulty)) {
        mainHeaderRowIndex = row;
        mainHeaderRow = rowData;
        console.log(`📋 Found main header row at index ${row}:`, rowData);
        
        // Next row should be sub-headers (storage sizes)
        if (row + 1 <= range.e.r) {
          const subRowData = [];
          for (let col = range.s.c; col <= range.e.c; col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: row + 1, c: col });
            const cell = worksheet[cellAddress];
            subRowData.push(cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '');
          }
          subHeaderRowIndex = row + 1;
          subHeaderRow = subRowData;
          console.log(`📋 Found sub-header row at index ${row + 1}:`, subRowData);
        }
        break;
      }
    }
    
    // If no header found, use row 1 as main header and row 2 as sub-header
    if (mainHeaderRowIndex === -1) {
      console.log('⚠️ No main header row found, using row 1 and 2');
      mainHeaderRowIndex = 0;
      subHeaderRowIndex = 1;
      const firstRow = [];
      const secondRow = [];
      for (let col = range.s.c; col <= range.e.c; col++) {
        const cellAddress1 = XLSX.utils.encode_cell({ r: 0, c: col });
        const cellAddress2 = XLSX.utils.encode_cell({ r: 1, c: col });
        const cell1 = worksheet[cellAddress1];
        const cell2 = worksheet[cellAddress2];
        firstRow.push(cell1 ? (cell1.v !== undefined ? String(cell1.v).trim() : '') : '');
        secondRow.push(cell2 ? (cell2.v !== undefined ? String(cell2.v).trim() : '') : '');
      }
      mainHeaderRow = firstRow;
      subHeaderRow = secondRow;
    }
    
    // Build column mapping: combine main header with sub-header for storage columns
    // For condition columns (EXCELLENT, GOOD, FAIR), use sub-header (64GB, 128GB, etc.)
    // For other columns (DEVICE, BRAND, MODEL, FAULTY, image_url), use main header
    // Track which condition each column index belongs to (since storage names repeat)
    // Note: Excel merged cells mean condition headers only appear in first column of span
    const columnMapping = [];
    const columnConditionByIndex = {}; // Maps column index to condition (Excellent, Good, Fair, Faulty)
    let currentCondition = null; // Track current condition as we iterate
    
    // First pass: identify condition headers and their start positions
    const conditionStarts = {}; // Maps condition name to column index where it starts
    for (let col = range.s.c; col <= range.e.c; col++) {
      const colIdx = col - range.s.c;
      const mainHeader = mainHeaderRow[colIdx] || '';
      const mainUpper = mainHeader.toUpperCase().trim();
      
      if (mainUpper === 'EXCELLENT' && !conditionStarts['Excellent']) {
        conditionStarts['Excellent'] = colIdx;
      } else if (mainUpper === 'GOOD' && !conditionStarts['Good']) {
        conditionStarts['Good'] = colIdx;
      } else if (mainUpper === 'FAIR' && !conditionStarts['Fair']) {
        conditionStarts['Fair'] = colIdx;
      } else if (mainUpper === 'FAULTY' && !conditionStarts['Faulty']) {
        conditionStarts['Faulty'] = colIdx;
      }
    }
    
    // Second pass: build column mapping and assign conditions
    // Determine where each condition section ends (start of next condition or non-condition column)
    const conditionEnds = {};
    const sortedConditions = Object.entries(conditionStarts).sort((a, b) => a[1] - b[1]);
    for (let i = 0; i < sortedConditions.length; i++) {
      const [condition, startIdx] = sortedConditions[i];
      if (i < sortedConditions.length - 1) {
        // End before next condition starts
        conditionEnds[condition] = sortedConditions[i + 1][1];
      } else {
        // Last condition - end before image_url or end of columns
        let endIdx = range.e.c - range.s.c + 1;
        for (let col = range.s.c; col <= range.e.c; col++) {
          const colIdx = col - range.s.c;
          const mainHeader = mainHeaderRow[colIdx] || '';
          const mainUpper = mainHeader.toUpperCase().trim();
          if (mainUpper.includes('IMAGE') || mainUpper.includes('URL') || mainUpper.includes('IMG')) {
            endIdx = colIdx;
            break;
          }
        }
        conditionEnds[condition] = endIdx;
      }
    }
    
    for (let col = range.s.c; col <= range.e.c; col++) {
      const colIdx = col - range.s.c;
      const mainHeader = mainHeaderRow[colIdx] || '';
      const subHeader = subHeaderRow && subHeaderRow[colIdx] ? subHeaderRow[colIdx] : '';
      
      const mainUpper = mainHeader.toUpperCase().trim();
      let columnName;
      
      // Check if this is a condition header
      if (mainUpper === 'EXCELLENT' || mainUpper === 'GOOD' || mainUpper === 'FAIR' || mainUpper === 'FAULTY') {
        // This is a condition header - update current condition
        if (mainUpper === 'EXCELLENT') currentCondition = 'Excellent';
        else if (mainUpper === 'GOOD') currentCondition = 'Good';
        else if (mainUpper === 'FAIR') currentCondition = 'Fair';
        else if (mainUpper === 'FAULTY') currentCondition = 'Faulty';
        
        // For condition headers, use sub-header if available, otherwise use main header
        columnName = subHeader || mainHeader || `__EMPTY_${colIdx}`;
        if (currentCondition) {
          columnConditionByIndex[colIdx] = currentCondition;
        }
      } else {
        // Check if this column belongs to a condition section
        let belongsToCondition = null;
        for (const [condition, startIdx] of Object.entries(conditionStarts)) {
          const endIdx = conditionEnds[condition] || (range.e.c - range.s.c + 1);
          if (colIdx >= startIdx && colIdx < endIdx) {
            belongsToCondition = condition;
            break;
          }
        }
        
        if (belongsToCondition) {
          // This column belongs to a condition section
          if (subHeader) {
            // This is a storage column under a condition section
            columnName = subHeader;
            columnConditionByIndex[colIdx] = belongsToCondition;
          } else {
            // This might be an empty cell within a condition section (merged cell continuation)
            // Use main header if available, otherwise mark as empty
            columnName = mainHeader || `__EMPTY_${colIdx}`;
            // Still assign the condition if we have a main header that's not a condition name
            if (mainHeader && mainHeader.trim() !== '') {
              columnConditionByIndex[colIdx] = belongsToCondition;
            }
          }
          currentCondition = belongsToCondition; // Keep track for next iteration
        } else {
          // Regular column (DEVICE, BRAND, MODEL, image_url, etc.)
          columnName = mainHeader || subHeader || `__EMPTY_${colIdx}`;
          // Reset current condition when we hit a non-condition column
          if (mainUpper && mainUpper !== '' && !mainUpper.includes('EMPTY') && !mainUpper.includes('IMAGE') && !mainUpper.includes('URL')) {
            currentCondition = null;
          }
        }
      }
      
      columnMapping.push(columnName);
    }
    
    console.log('📋 Column mapping:', columnMapping);
    console.log('📋 Column condition by index:', columnConditionByIndex);
    
    // Read data starting from row after sub-header row
    // IMPORTANT: Store data by column index, not by column name, because multiple columns
    // can have the same name (e.g., "128GB" appears under EXCELLENT, GOOD, and FAIR)
    const dataStartRow = subHeaderRowIndex !== -1 ? subHeaderRowIndex + 1 : mainHeaderRowIndex + 1;
    const allRows = [];
    for (let row = dataStartRow; row <= range.e.r; row++) {
      const rowData = {};
      const rowDataByIndex = {}; // Store values by column index for accurate retrieval
      for (let col = range.s.c; col <= range.e.c; col++) {
        const colIdx = col - range.s.c;
        const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
        const cell = worksheet[cellAddress];
        const value = cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '';
        const headerName = columnMapping[colIdx] || `__EMPTY_${colIdx}`;
        
        // Store by header name (for non-storage columns like DEVICE, BRAND, MODEL)
        rowData[headerName] = value;
        // Store by column index (for storage columns that have duplicate names)
        rowDataByIndex[colIdx] = value;
      }
      // Attach the index-based data to the row object
      rowData._byIndex = rowDataByIndex;
      // Only add non-empty rows
      if (Object.values(rowData).some(v => v && v !== '' && v !== rowDataByIndex)) {
        allRows.push(rowData);
      }
    }
    
    const data = allRows;
    
    // Log first row to see structure
    if (data.length > 0) {
      console.log('📋 First data row keys:', Object.keys(data[0]));
      console.log('📋 First data row sample:', JSON.stringify(data[0], null, 2));
      console.log('📋 Column condition by index:', columnConditionByIndex);
    }

    // Helper function to find column value (case-insensitive, tries multiple variations)
    const getColumnValue = (row, possibleNames) => {
      for (const name of possibleNames) {
        // Try exact match first
        if (row[name] !== undefined && row[name] !== null && row[name] !== '') {
          return row[name];
        }
        // Try case-insensitive match
        const rowKeys = Object.keys(row);
        const matchedKey = rowKeys.find(key => key.toLowerCase().trim() === name.toLowerCase().trim());
        if (matchedKey && row[matchedKey] !== undefined && row[matchedKey] !== null && row[matchedKey] !== '') {
          return row[matchedKey];
        }
      }
      return null;
    };

    // Log available columns from first row for debugging
    if (data.length > 0) {
      console.log('📋 Available columns in Excel:', Object.keys(data[0]));
      // Check for image URL column
      const firstRow = data[0];
      const imageCols = Object.keys(firstRow).filter(col => {
        const colLower = col.toLowerCase();
        return colLower.includes('image') || colLower.includes('img') || colLower.includes('url');
      });
      if (imageCols.length > 0) {
        console.log('🖼️ Found potential image URL columns:', imageCols);
      } else {
        console.log('⚠️ No image URL columns found in Excel');
      }
    }

    const results = {
      new: 0,
      updated: 0,
      errors: [],
      changes: []
    };

    // Storage capacities to look for
    const storageOptions = ['64GB', '128GB', '256GB', '512GB', '1TB', '2TB'];
    
    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      try {
        // Get basic info
        const brand = getColumnValue(row, ['Brand', 'brand', 'Brand Name', 'BRAND', 'BrandName']);
        const model = getColumnValue(row, ['Model', 'model', 'Product Model', 'MODEL', 'ModelName', 'Product']);
        const deviceType = (getColumnValue(row, ['Device Type', 'deviceType', 'DeviceType', 'Device', 'device', 'DEVICE', 'Type', 'type']) || 'phone').toLowerCase();
        const color = getColumnValue(row, ['Color', 'color', 'Colour', 'colour', 'COLOR']) || null;
        
        // Find image URL column - check all possible variations and positions
        let imageUrl = null;
        const imageColumnNames = ['Image URL', 'imageUrl', 'ImageUrl', 'Image', 'image', 'Image Link', 'imageLink', 'image_url', 'IMAGE_URL', 'imageUrl', 'ImageUrl'];
        
        // First try direct column name match
        for (const colName of imageColumnNames) {
          const val = getColumnValue(row, [colName]);
          if (val && val.trim() !== '') {
            imageUrl = val.trim();
            break;
          }
        }
        
        // If not found, search through all columns for image-related names
        if (!imageUrl) {
          const allCols = Object.keys(row);
          for (const col of allCols) {
            const colLower = col.toLowerCase();
            // Look for image-related columns (but exclude condition columns)
            if ((colLower.includes('image') || colLower.includes('img') || (colLower.includes('url') && !colLower.includes('excellent') && !colLower.includes('good') && !colLower.includes('fair') && !colLower.includes('faulty'))) && 
                row[col] && row[col] !== null && row[col] !== undefined && row[col].toString().trim() !== '') {
              const val = row[col].toString().trim();
              // Accept any non-empty value (not just URLs starting with http)
              // This allows for relative URLs, file paths, or any image identifier
              if (val && val.length > 0) {
                imageUrl = val;
                console.log(`📷 Found image URL in column "${col}": ${imageUrl.substring(0, 80)}`);
                break;
              }
            }
          }
        }
        
        // Log if image URL was found or not
        if (imageUrl) {
          console.log(`✅ Found image URL for ${brand} ${model}: ${imageUrl.substring(0, Math.min(80, imageUrl.length))}${imageUrl.length > 80 ? '...' : ''}`);
        } else {
          console.log(`⚠️ No image URL found for ${brand} ${model} - available columns: ${Object.keys(row).join(', ')}`);
        }

        if (!brand || !model) {
          const missing = [];
          if (!brand) missing.push('Brand');
          if (!model) missing.push('Model');
          results.errors.push({ 
            row: i + 2, 
            error: `Missing required fields: ${missing.join(', ')}. Available columns: ${Object.keys(row).join(', ')}` 
          });
          continue;
        }

        // Extract prices for each storage capacity and condition
        // After column mapping, we need to match storage columns by their position and condition
        // Use columnConditionByIndex to find which condition each column index belongs to
        
        // Get all column names from the row (these are the mapped column names like "64GB", "128GB", etc.)
        const allColumns = Object.keys(row);
        
        // Build a map of column name -> column index in the original mapping
        const columnNameToIndex = {};
        columnMapping.forEach((colName, idx) => {
          if (!columnNameToIndex[colName]) {
            columnNameToIndex[colName] = [];
          }
          columnNameToIndex[colName].push(idx);
        });
        
        // For each storage option, extract prices from each condition section
        for (let storageIdx = 0; storageIdx < storageOptions.length; storageIdx++) {
          const storage = storageOptions[storageIdx];
          const prices = {
            Excellent: null,
            Good: null,
            Fair: null,
            Faulty: null
          };
          
          // Normalize storage for matching
          const storageNormalized = storage.toUpperCase().replace(/\s+/g, '');
          
          // Find all column indices that match this storage size
          const matchingIndices = [];
          columnMapping.forEach((colName, idx) => {
            const colUpper = colName.toUpperCase().replace(/\s+/g, '');
            if (colUpper === storageNormalized || 
                colUpper.includes(storageNormalized) || 
                storageNormalized.includes(colUpper)) {
              matchingIndices.push(idx);
            }
          });
          
          // For each matching column index, check its condition and extract price
          // CRITICAL: Read value by column index, not by column name, because the same
          // storage name (e.g., "128GB") appears multiple times under different conditions
          for (const colIdx of matchingIndices) {
            const condition = columnConditionByIndex[colIdx];
            const colName = columnMapping[colIdx];
            
            // Read value directly from column index to avoid overwriting issues
            const cellValue = row._byIndex && row._byIndex[colIdx] !== undefined 
              ? row._byIndex[colIdx] 
              : row[colName];
            
            // Debug logging for first row, first storage
            if (storage === '64GB' && i === 0) {
              console.log(`🔍 Column ${colIdx} (${colName}): condition=${condition}, value="${cellValue}"`);
            }
            
            if (condition && cellValue !== undefined && cellValue !== null && cellValue !== '') {
              const val = parseFloat(cellValue);
              if (!isNaN(val) && val > 0) {
                // Map condition to price object key (only set if not already set)
                if (condition === 'Excellent' && prices.Excellent === null) {
                  prices.Excellent = val;
                  if (storage === '64GB' && i === 0) console.log(`✅ Set Excellent price: £${val}`);
                }
                else if (condition === 'Good' && prices.Good === null) {
                  prices.Good = val;
                  if (storage === '64GB' && i === 0) console.log(`✅ Set Good price: £${val}`);
                }
                else if (condition === 'Fair' && prices.Fair === null) {
                  prices.Fair = val;
                  if (storage === '64GB' && i === 0) console.log(`✅ Set Fair price: £${val}`);
                }
                else if (condition === 'Faulty' && prices.Faulty === null) {
                  prices.Faulty = val;
                  if (storage === '64GB' && i === 0) console.log(`✅ Set Faulty price: £${val}`);
                }
              }
            }
          }
          
          // Debug: Log extracted prices for first row
          if (i === 0) {
            console.log(`📊 ${brand} ${model} ${storage} prices:`, prices);
          }
          
          // Fallback: if columnConditionByIndex wasn't populated, try sequential position
          if (Object.keys(columnConditionByIndex).length === 0) {
            // Try to find condition headers in column names
            let excellentStart = -1, goodStart = -1, fairStart = -1, faultyStart = -1;
            columnMapping.forEach((colName, idx) => {
              const colUpper = colName.toUpperCase().trim();
              if (colUpper === 'EXCELLENT' && excellentStart === -1) excellentStart = idx;
              if (colUpper === 'GOOD' && goodStart === -1) goodStart = idx;
              if (colUpper === 'FAIR' && fairStart === -1) fairStart = idx;
              if (colUpper === 'FAULTY' && faultyStart === -1) faultyStart = idx;
            });
            
            // Try sequential position for each condition
            if (excellentStart !== -1 && excellentStart + 1 + storageIdx < columnMapping.length) {
              const colName = columnMapping[excellentStart + 1 + storageIdx];
              const val = parseFloat(row[colName]);
              if (!isNaN(val) && val > 0) prices.Excellent = val;
            }
            if (goodStart !== -1 && goodStart + 1 + storageIdx < columnMapping.length) {
              const colName = columnMapping[goodStart + 1 + storageIdx];
              const val = parseFloat(row[colName]);
              if (!isNaN(val) && val > 0) prices.Good = val;
            }
            if (fairStart !== -1 && fairStart + 1 + storageIdx < columnMapping.length) {
              const colName = columnMapping[fairStart + 1 + storageIdx];
              const val = parseFloat(row[colName]);
              if (!isNaN(val) && val > 0) prices.Fair = val;
            }
            if (faultyStart !== -1 && faultyStart + 1 < columnMapping.length) {
              // FAULTY usually has just one price column
              const colName = columnMapping[faultyStart + 1];
              const val = parseFloat(row[colName]);
              if (!isNaN(val) && val > 0) prices.Faulty = val;
            }
          }
          
          // Only create product if at least one price exists
          const hasAnyPrice = prices.Excellent !== null || prices.Good !== null || prices.Fair !== null || prices.Faulty !== null;
          
          if (!hasAnyPrice) {
            continue; // Skip this storage if no prices
          }
          
          // Check if product exists
          const existing = await db.collection('trade_in_products').findOne({
            brand: brand.trim(),
            model: model.trim(),
            storage: storage,
            color: color ? color.trim() : null,
            deviceType: deviceType
          });

          // Generate SEO-friendly slug
          const slug = generateProductSlug(brand.trim(), model.trim(), storage, color ? color.trim() : null);
          
          const productData = {
            brand: brand.trim(),
            model: model.trim(),
            storage: storage,
            color: color ? color.trim() : null,
            deviceType: deviceType,
            imageUrl: imageUrl ? imageUrl.trim() : null, // Ensure imageUrl is trimmed
            prices: prices,
            slug: slug, // SEO-friendly URL slug
            updatedAt: new Date().toISOString()
          };
          
          // Log imageUrl for debugging
          if (imageUrl) {
            console.log(`💾 Saving product with imageUrl: ${brand} ${model} ${storage} - ${imageUrl.substring(0, 60)}...`);
          } else {
            console.log(`⚠️ No imageUrl for ${brand} ${model} ${storage}`);
          }

          if (existing) {
            // Track changes
            const changes = [];
            if (existing.prices?.Excellent !== prices.Excellent) {
              changes.push({ field: 'Excellent', old: existing.prices?.Excellent, new: prices.Excellent });
            }
            if (existing.prices?.Good !== prices.Good) {
              changes.push({ field: 'Good', old: existing.prices?.Good, new: prices.Good });
            }
            if (existing.prices?.Fair !== prices.Fair) {
              changes.push({ field: 'Fair', old: existing.prices?.Fair, new: prices.Fair });
            }
            if (existing.prices?.Faulty !== prices.Faulty) {
              changes.push({ field: 'Faulty', old: existing.prices?.Faulty, new: prices.Faulty });
            }
            if (existing.imageUrl !== imageUrl) {
              changes.push({ field: 'imageUrl', old: existing.imageUrl, new: imageUrl });
            }

            await db.collection('trade_in_products').updateOne(
              { _id: existing._id },
              { $set: productData }
            );

            results.updated++;
            if (changes.length > 0) {
              results.changes.push({
                product: `${brand} ${model} ${storage}${color ? ` ${color}` : ''}`,
                changes: changes
              });
            }
          } else {
            productData.createdAt = new Date().toISOString();
            await db.collection('trade_in_products').insertOne(productData);
            results.new++;
            results.changes.push({
              product: `${brand} ${model} ${storage}${color ? ` ${color}` : ''}`,
              changes: [{ field: 'status', old: null, new: 'Created' }]
            });
          }
        }

      } catch (error) {
        results.errors.push({ row: i + 2, error: error.message });
      }
    }

    res.json({
      success: true,
      summary: {
        total: data.length,
        new: results.new,
        updated: results.updated,
        errors: results.errors.length
      },
      changes: results.changes,
      errors: results.errors
    });

  } catch (error) {
    console.error('Error importing Excel:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Export products to Excel (admin)
app.get('/api/products/export-excel', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const products = await db.collection('trade_in_products').find({}).sort({ brand: 1, model: 1, storage: 1 }).toArray();

    // Convert to Excel format
    const excelData = products.map(p => ({
      'Brand': p.brand,
      'Model': p.model,
      'Storage': p.storage,
      'Color': p.color || '',
      'Device Type': p.deviceType,
      'Image URL': p.imageUrl || '',
      'Excellent': p.prices?.Excellent || '',
      'Good': p.prices?.Good || '',
      'Fair': p.prices?.Fair || '',
      'Faulty': p.prices?.Faulty || ''
    }));

    const worksheet = XLSX.utils.json_to_sheet(excelData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Products');

    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=trade-in-products-${new Date().toISOString().split('T')[0]}.xlsx`);
    res.send(buffer);

  } catch (error) {
    console.error('Error exporting Excel:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// AUTHENTICATION MIDDLEWARE
// ============================================

// Middleware to verify JWT token
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1]; // Bearer TOKEN
  
  if (!token) {
    return res.status(401).json({ error: 'Access token required' });
  }
  
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({ error: 'Invalid or expired token' });
    }
    req.user = user;
    next();
  });
}

// Helper function to generate session ID
function generateSessionId() {
  return require('crypto').randomBytes(32).toString('hex');
}

// Helper function to create JWT token
function createToken(customerId, email) {
  return jwt.sign(
    { customerId, email },
    JWT_SECRET,
    { expiresIn: '30m' } // Token expires in 30 minutes
  );
}

// Helper function to create or find Shopify customer
async function createOrFindShopifyCustomer(firstName, lastName, email, phone, postcode) {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      console.warn('Shopify credentials not configured - skipping Shopify customer creation');
      return null;
    }

    // First, try to find existing customer by email
    const searchResponse = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/customers/search.json?query=email:${encodeURIComponent(email)}`,
      {
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        }
      }
    );

    if (searchResponse.ok) {
      const searchData = await searchResponse.json();
      if (searchData.customers && searchData.customers.length > 0) {
        // Customer exists, return their ID
        const existingCustomer = searchData.customers[0];
        console.log(`Found existing Shopify customer: ${existingCustomer.id}`);
        return existingCustomer.id.toString();
      }
    }

    // Customer doesn't exist, create new one
    const customerData = {
      customer: {
        first_name: firstName,
        last_name: lastName,
        email: email,
        phone: phone || null,
        addresses: postcode ? [{
          address1: '',
          city: '',
          province: '',
          country: 'United Kingdom',
          zip: postcode
        }] : []
      }
    };

    const createResponse = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/customers.json`,
      {
        method: 'POST',
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(customerData)
      }
    );

    if (createResponse.ok) {
      const createData = await createResponse.json();
      if (createData.customer) {
        console.log(`Created new Shopify customer: ${createData.customer.id}`);
        return createData.customer.id.toString();
      }
    } else {
      const errorData = await createResponse.json().catch(() => ({}));
      console.error('Error creating Shopify customer:', errorData);
    }

    return null;
  } catch (error) {
    console.error('Error in createOrFindShopifyCustomer:', error);
    return null; // Don't fail registration if Shopify creation fails
  }
}

// Helper function to update Shopify customer
async function updateShopifyCustomer(shopifyCustomerId, firstName, lastName, phone, postcode) {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN || !shopifyCustomerId) {
      return false;
    }

    const customerData = {
      customer: {
        first_name: firstName,
        last_name: lastName,
        phone: phone || null
      }
    };

    // Update address if postcode provided
    if (postcode) {
      // First get existing customer to preserve addresses
      const getResponse = await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2024-01/customers/${shopifyCustomerId}.json`,
        {
          headers: {
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
            'Content-Type': 'application/json'
          }
        }
      );

      if (getResponse.ok) {
        const getData = await getResponse.json();
        const existingAddresses = getData.customer?.addresses || [];
        
        // Update or add address with postcode
        const addressIndex = existingAddresses.findIndex(addr => addr.zip === postcode);
        if (addressIndex >= 0) {
          existingAddresses[addressIndex].zip = postcode;
        } else {
          existingAddresses.push({
            address1: '',
            city: '',
            province: '',
            country: 'United Kingdom',
            zip: postcode
          });
        }
        customerData.customer.addresses = existingAddresses;
      }
    }

    const updateResponse = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/customers/${shopifyCustomerId}.json`,
      {
        method: 'PUT',
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(customerData)
      }
    );

    return updateResponse.ok;
  } catch (error) {
    console.error('Error updating Shopify customer:', error);
    return false;
  }
}

// ============================================
// AUTHENTICATION ENDPOINTS
// ============================================

// Register new customer
app.post('/api/auth/register', async (req, res) => {
  try {
    const { firstName, lastName, email, password, phone, postcode } = req.body;
    
    if (!firstName || !lastName || !email || !password) {
      return res.status(400).json({ 
        error: 'First name, last name, email, and password are required' 
      });
    }
    
    if (password.length < 6) {
      return res.status(400).json({ 
        error: 'Password must be at least 6 characters long' 
      });
    }
    
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    // Check if customer already exists
    const existingCustomer = await db.collection('customers').findOne({ email: email.toLowerCase().trim() });
    if (existingCustomer) {
      return res.status(409).json({ 
        error: 'An account with this email already exists' 
      });
    }
    
    // Hash password
    const saltRounds = 10;
    const passwordHash = await bcrypt.hash(password, saltRounds);
    
    // Create Shopify customer (in background, don't fail if it fails)
    let shopifyCustomerId = null;
    try {
      shopifyCustomerId = await createOrFindShopifyCustomer(
        firstName.trim(),
        lastName.trim(),
        email.toLowerCase().trim(),
        phone || '',
        postcode || ''
      );
    } catch (shopifyError) {
      console.error('Error creating Shopify customer (non-fatal):', shopifyError);
      // Continue with registration even if Shopify creation fails
    }
    
    // Create customer in MongoDB
    const customer = {
      firstName: firstName.trim(),
      lastName: lastName.trim(),
      email: email.toLowerCase().trim(),
      passwordHash,
      phone: phone || '',
      postcode: postcode || '',
      shopifyCustomerId: shopifyCustomerId || null, // Store Shopify customer ID
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      emailVerified: false,
      isActive: true
    };
    
    const result = await db.collection('customers').insertOne(customer);
    const customerId = result.insertedId.toString();
    
    // Create session
    const sessionId = generateSessionId();
    const expiresAt = new Date();
    expiresAt.setMinutes(expiresAt.getMinutes() + 30); // 30 minutes
    
    await db.collection('sessions').insertOne({
      sessionId,
      customerId,
      email: customer.email,
      createdAt: new Date().toISOString(),
      expiresAt: expiresAt.toISOString()
    });
    
    // Generate JWT token
    const token = createToken(customerId, customer.email);
    
    // Return customer data (without password)
    res.json({
      success: true,
      token,
      sessionId,
      customer: {
        id: customerId,
        firstName: customer.firstName,
        lastName: customer.lastName,
        email: customer.email,
        phone: customer.phone,
        postcode: customer.postcode
      }
    });
    
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Registration failed. Please try again.' });
  }
});

// Login customer
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    
    if (!email || !password) {
      return res.status(400).json({ 
        error: 'Email and password are required' 
      });
    }
    
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    // Find customer by email
    const customer = await db.collection('customers').findOne({ 
      email: email.toLowerCase().trim() 
    });
    
    if (!customer) {
      return res.status(401).json({ 
        error: 'Invalid email or password' 
      });
    }
    
    // Check if account is active
    if (customer.isActive === false) {
      return res.status(403).json({ 
        error: 'Account is deactivated. Please contact support.' 
      });
    }
    
    // Verify password
    const passwordMatch = await bcrypt.compare(password, customer.passwordHash);
    if (!passwordMatch) {
      return res.status(401).json({ 
        error: 'Invalid email or password' 
      });
    }
    
    // Sync with Shopify if shopifyCustomerId is missing
    if (!customer.shopifyCustomerId) {
      try {
        const shopifyCustomerId = await createOrFindShopifyCustomer(
          customer.firstName,
          customer.lastName,
          customer.email,
          customer.phone,
          customer.postcode
        );
        
        if (shopifyCustomerId) {
          // Update MongoDB customer with Shopify ID
          await db.collection('customers').updateOne(
            { _id: customer._id },
            { $set: { shopifyCustomerId: shopifyCustomerId } }
          );
          customer.shopifyCustomerId = shopifyCustomerId;
        }
      } catch (shopifyError) {
        console.error('Error syncing Shopify customer (non-fatal):', shopifyError);
        // Continue with login even if Shopify sync fails
      }
    }
    
    // Create session
    const sessionId = generateSessionId();
    const expiresAt = new Date();
    expiresAt.setMinutes(expiresAt.getMinutes() + 30); // 30 minutes
    
    await db.collection('sessions').insertOne({
      sessionId,
      customerId: customer._id.toString(),
      email: customer.email,
      createdAt: new Date().toISOString(),
      expiresAt: expiresAt.toISOString()
    });
    
    // Generate JWT token
    const token = createToken(customer._id.toString(), customer.email);
    
    // Return customer data (without password)
    res.json({
      success: true,
      token,
      sessionId,
      customer: {
        id: customer._id.toString(),
        firstName: customer.firstName,
        lastName: customer.lastName,
        email: customer.email,
        phone: customer.phone,
        postcode: customer.postcode,
        shopifyCustomerId: customer.shopifyCustomerId || null
      }
    });
    
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Login failed. Please try again.' });
  }
});

// Get current user (requires authentication)
app.get('/api/auth/me', authenticateToken, async (req, res) => {
  try {
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    const customer = await db.collection('customers').findOne({ 
      _id: new ObjectId(req.user.customerId) 
    });
    
    if (!customer) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    
    // Return customer data (without password)
    res.json({
      success: true,
      customer: {
        id: customer._id.toString(),
        firstName: customer.firstName,
        lastName: customer.lastName,
        email: customer.email,
        phone: customer.phone,
        postcode: customer.postcode,
        shopifyCustomerId: customer.shopifyCustomerId || null,
        createdAt: customer.createdAt
      }
    });
    
  } catch (error) {
    console.error('Get user error:', error);
    res.status(500).json({ error: 'Failed to fetch user data' });
  }
});

// Logout customer
app.post('/api/auth/logout', authenticateToken, async (req, res) => {
  try {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    // Invalidate session (optional - you can also just rely on token expiration)
    // For now, we'll just return success
    // In production, you might want to maintain a blacklist of tokens
    
    res.json({
      success: true,
      message: 'Logged out successfully'
    });
    
  } catch (error) {
    console.error('Logout error:', error);
    res.status(500).json({ error: 'Logout failed' });
  }
});

// Forgot password - send reset email
app.post('/api/auth/forgot-password', async (req, res) => {
  try {
    const { email } = req.body;
    
    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }
    
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    const customer = await db.collection('customers').findOne({ 
      email: email.toLowerCase().trim() 
    });
    
    // Don't reveal if email exists or not (security best practice)
    // Always return success message
    if (customer) {
      // Generate reset token
      const resetToken = require('crypto').randomBytes(32).toString('hex');
      const expiresAt = new Date();
      expiresAt.setHours(expiresAt.getHours() + 1); // Token expires in 1 hour
      
      await db.collection('password_reset_tokens').insertOne({
        token: resetToken,
        customerId: customer._id.toString(),
        email: customer.email,
        createdAt: new Date().toISOString(),
        expiresAt: expiresAt.toISOString(),
        used: false
      });
      
      // Send reset email
      const resetUrl = `${req.headers.origin || 'https://tech-corner-9576.myshopify.com'}/account/reset-password?token=${resetToken}`;
      
      try {
        await transporter.sendMail({
          from: SMTP_FROM,
          to: customer.email,
          subject: 'Password Reset Request',
          html: `
            <h2>Password Reset Request</h2>
            <p>You requested to reset your password. Click the link below to reset it:</p>
            <p><a href="${resetUrl}" style="background: #10b981; color: white; padding: 12px 24px; text-decoration: none; border-radius: 8px; display: inline-block;">Reset Password</a></p>
            <p>This link will expire in 1 hour.</p>
            <p>If you didn't request this, please ignore this email.</p>
          `
        });
      } catch (emailError) {
        console.error('Email send error:', emailError);
        // Still return success to not reveal if email exists
      }
    }
    
    res.json({
      success: true,
      message: 'If an account exists with this email, a password reset link has been sent.'
    });
    
  } catch (error) {
    console.error('Forgot password error:', error);
    res.status(500).json({ error: 'Failed to process request' });
  }
});

// Reset password
app.post('/api/auth/reset-password', async (req, res) => {
  try {
    const { token, password } = req.body;
    
    if (!token || !password) {
      return res.status(400).json({ 
        error: 'Token and password are required' 
      });
    }
    
    if (password.length < 6) {
      return res.status(400).json({ 
        error: 'Password must be at least 6 characters long' 
      });
    }
    
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    // Find reset token
    const resetToken = await db.collection('password_reset_tokens').findOne({
      token: token,
      used: false,
      expiresAt: { $gt: new Date().toISOString() }
    });
    
    if (!resetToken) {
      return res.status(400).json({ 
        error: 'Invalid or expired reset token' 
      });
    }
    
    // Hash new password
    const saltRounds = 10;
    const passwordHash = await bcrypt.hash(password, saltRounds);
    
    // Update customer password
    await db.collection('customers').updateOne(
      { _id: new ObjectId(resetToken.customerId) },
      { 
        $set: { 
          passwordHash,
          updatedAt: new Date().toISOString()
        }
      }
    );
    
    // Mark token as used
    await db.collection('password_reset_tokens').updateOne(
      { token: token },
      { $set: { used: true } }
    );
    
    res.json({
      success: true,
      message: 'Password reset successfully'
    });
    
  } catch (error) {
    console.error('Reset password error:', error);
    res.status(500).json({ error: 'Failed to reset password' });
  }
});

// ============================================
// CUSTOMER ACCOUNT ENDPOINTS
// ============================================

// Get customer's trade-in submissions
app.get('/api/customer/trade-ins', authenticateToken, async (req, res) => {
  try {
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    const customerId = req.user.customerId;
    
    // Get submissions from MongoDB
    const submissions = await db.collection('submissions')
      .find({ customerId: customerId })
      .sort({ createdAt: -1 })
      .toArray();
    
    res.json({
      success: true,
      submissions: submissions,
      count: submissions.length
    });
    
  } catch (error) {
    console.error('Error fetching customer trade-ins:', error);
    res.status(500).json({ error: 'Failed to fetch trade-ins' });
  }
});

// Get single trade-in submission (customer's own)
app.get('/api/customer/trade-ins/:id', authenticateToken, async (req, res) => {
  try {
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    const customerId = req.user.customerId;
    const submissionId = parseInt(req.params.id);
    
    // Get submission from MongoDB
    const submission = await db.collection('submissions').findOne({
      id: submissionId,
      customerId: customerId
    });
    
    if (!submission) {
      return res.status(404).json({ error: 'Trade-in submission not found' });
    }
    
    res.json({
      success: true,
      submission: submission
    });
    
  } catch (error) {
    console.error('Error fetching trade-in:', error);
    res.status(500).json({ error: 'Failed to fetch trade-in' });
  }
});

// Update customer profile
app.put('/api/customer/profile', authenticateToken, async (req, res) => {
  try {
    const { firstName, lastName, phone, postcode } = req.body;
    const customerId = req.user.customerId;
    
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    const updateData = {
      updatedAt: new Date().toISOString()
    };
    
    if (firstName) updateData.firstName = firstName.trim();
    if (lastName) updateData.lastName = lastName.trim();
    if (phone !== undefined) updateData.phone = phone || '';
    if (postcode !== undefined) updateData.postcode = postcode || '';
    
    // Get customer first to check for shopifyCustomerId
    const customer = await db.collection('customers').findOne({ 
      _id: new ObjectId(customerId) 
    });
    
    if (!customer) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    
    // Update MongoDB
    const result = await db.collection('customers').updateOne(
      { _id: new ObjectId(customerId) },
      { $set: updateData }
    );
    
    // Sync with Shopify if shopifyCustomerId exists
    if (customer.shopifyCustomerId) {
      try {
        await updateShopifyCustomer(
          customer.shopifyCustomerId,
          updateData.firstName || customer.firstName,
          updateData.lastName || customer.lastName,
          updateData.phone !== undefined ? updateData.phone : customer.phone,
          updateData.postcode !== undefined ? updateData.postcode : customer.postcode
        );
      } catch (shopifyError) {
        console.error('Error syncing with Shopify (non-fatal):', shopifyError);
        // Continue even if Shopify update fails
      }
    }
    
    // Get updated customer
    const updatedCustomer = await db.collection('customers').findOne({ 
      _id: new ObjectId(customerId) 
    });
    
    res.json({
      success: true,
      customer: {
        id: updatedCustomer._id.toString(),
        firstName: updatedCustomer.firstName,
        lastName: updatedCustomer.lastName,
        email: updatedCustomer.email,
        phone: updatedCustomer.phone,
        postcode: updatedCustomer.postcode
      }
    });
    
  } catch (error) {
    console.error('Error updating profile:', error);
    res.status(500).json({ error: 'Failed to update profile' });
  }
});

// Change password
app.put('/api/customer/change-password', authenticateToken, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    const customerId = req.user.customerId;
    
    if (!currentPassword || !newPassword) {
      return res.status(400).json({ 
        error: 'Current password and new password are required' 
      });
    }
    
    if (newPassword.length < 6) {
      return res.status(400).json({ 
        error: 'New password must be at least 6 characters long' 
      });
    }
    
    await ensureMongoConnection();
    
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }
    
    // Get customer
    const customer = await db.collection('customers').findOne({ 
      _id: new ObjectId(customerId) 
    });
    
    if (!customer) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    
    // Verify current password
    const passwordMatch = await bcrypt.compare(currentPassword, customer.passwordHash);
    if (!passwordMatch) {
      return res.status(401).json({ 
        error: 'Current password is incorrect' 
      });
    }
    
    // Hash new password
    const saltRounds = 10;
    const passwordHash = await bcrypt.hash(newPassword, saltRounds);
    
    // Update password
    await db.collection('customers').updateOne(
      { _id: new ObjectId(customerId) },
      { 
        $set: { 
          passwordHash,
          updatedAt: new Date().toISOString()
        }
      }
    );
    
    res.json({
      success: true,
      message: 'Password changed successfully'
    });
    
  } catch (error) {
    console.error('Error changing password:', error);
    res.status(500).json({ error: 'Failed to change password' });
  }
});

// ============================================
// TRADE-IN SUBMISSION ENDPOINTS
// ============================================

// Email price quote
app.post('/api/trade-in/email-price', async (req, res) => {
  try {
    const { to, subject, itemName, brand, model, storage, condition, price, priceFormatted, deviceType, pageUrl } = req.body;
    
    if (!to || !itemName || !price) {
      return res.status(400).json({ error: 'Missing required fields (to, itemName, price)' });
    }
    
    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      return res.status(400).json({ error: 'Invalid email address' });
    }
    
    // Create email content
    const emailSubject = subject || `Trade-in Quote: ${itemName}`;
    const escapedItemName = itemName.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const escapedCondition = (condition || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const escapedStorage = (storage || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const priceDisplay = priceFormatted || '£' + parseFloat(price).toFixed(2);
    const emailUrl = pageUrl || 'https://tech-corner-9576.myshopify.com/pages/sell-your-device';
    
    const emailHtml = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 2rem;">
        <h2 style="color: #10b981; margin-bottom: 1rem;">Trade-in Quote</h2>
        <p>Thank you for your interest in trading in your device with us!</p>
        
        <div style="background: #f5f5f5; padding: 1.5rem; border-radius: 8px; margin: 2rem 0;">
          <h3 style="margin-top: 0; color: #333;">${escapedItemName}</h3>
          <p><strong>Condition:</strong> ${escapedCondition}</p>
          <p><strong>Storage:</strong> ${escapedStorage}</p>
          <p style="font-size: 1.5rem; color: #10b981; font-weight: 700; margin: 1rem 0;">
            We'll pay you: ${priceDisplay}
          </p>
        </div>
        
        <p>This quote is valid for 30 days. To proceed with your trade-in, please visit:</p>
        <p><a href="${emailUrl}" style="color: #10b981; text-decoration: underline;">Complete Your Trade-in</a></p>
        
        <p style="margin-top: 2rem; color: #666; font-size: 0.9rem;">
          If you have any questions, please don't hesitate to contact us.
        </p>
      </div>
    `;
    
    const emailText = `
Trade-in Quote

Thank you for your interest in trading in your device with us!

Device: ${itemName}
Condition: ${condition}
Storage: ${storage}

We'll pay you: ${priceFormatted || '£' + parseFloat(price).toFixed(2)}

This quote is valid for 30 days. To proceed with your trade-in, please visit:
${pageUrl || 'https://tech-corner-9576.myshopify.com/pages/sell-your-device'}

If you have any questions, please don't hesitate to contact us.
    `;
    
    // Send email
    await transporter.sendMail({
      from: `"Tech Corner" <${SMTP_USER}>`,
      to: to,
      subject: emailSubject,
      text: emailText,
      html: emailHtml
    });
    
    res.json({ 
      success: true, 
      message: 'Price quote email sent successfully' 
    });
    
  } catch (error) {
    console.error('Error sending price email:', error);
    res.status(500).json({ error: 'Failed to send email' });
  }
});

// Submit trade-in request
app.post('/api/trade-in/submit', async (req, res) => {
  try {
    const {
      name,
      email,
      phone,
      postcode,
      notes,
      brand,
      model,
      storage,
      condition,
      finalPrice,
      deviceType,
      pageUrl,
      isCustomDevice,
      paymentMethod,
      paymentDetails,
      items, // New: array of items for batch submission
      confirmations // New: confirmations from checkboxes
    } = req.body;

    // Check if this is a batch submission (items array) or single item
    const isBatchSubmission = items && Array.isArray(items) && items.length > 0;

    if (isBatchSubmission) {
      // Batch submission: validate items array
      if (!name || !email) {
        return res.status(400).json({ 
          error: 'Missing required fields: name, email' 
        });
      }

      // Validate each item in the array
      for (let i = 0; i < items.length; i++) {
        const item = items[i];
        if (!item.brand || !item.model || !item.condition) {
          return res.status(400).json({ 
            error: `Missing required fields in item ${i + 1}: brand, model, condition` 
          });
        }
      }
    } else {
      // Single item submission: validate required fields
      if (!name || !email || !brand || !model || !condition) {
        return res.status(400).json({ 
          error: 'Missing required fields' 
        });
      }
    }

    // Validate payment details based on payment method
    const selectedPaymentMethod = paymentMethod || 'store_credit';
    if (selectedPaymentMethod === 'bank_transfer') {
      if (!paymentDetails?.firstName || !paymentDetails?.lastName || !paymentDetails?.sortCode || !paymentDetails?.accountNumber) {
        return res.status(400).json({ 
          error: 'Bank account details are required for bank transfer' 
        });
      }
    } else if (selectedPaymentMethod === 'paypal') {
      if (!paymentDetails?.paypalEmail) {
        return res.status(400).json({ 
          error: 'PayPal email is required for PayPal payment' 
        });
      }
    }

    // Get customer ID from token if authenticated (optional - guest submissions allowed)
    let customerId = null;
    let shopifyCustomerId = null;
    try {
      const authHeader = req.headers['authorization'];
      const token = authHeader && authHeader.split(' ')[1];
      if (token) {
        const decoded = jwt.verify(token, JWT_SECRET);
        customerId = decoded.customerId;
        
        // Get customer to get shopifyCustomerId
        if (customerId && db) {
          const customer = await db.collection('customers').findOne({ 
            _id: new ObjectId(customerId) 
          });
          shopifyCustomerId = customer?.shopifyCustomerId || null;
        }
      }
    } catch (tokenError) {
      // Token invalid or missing - allow guest submission
      console.log('Guest submission (no valid token)');
    }

    // Create submission
    let submission;
    
    if (isBatchSubmission) {
      // Batch submission: create one submission with items array
      const totalPrice = items.reduce((sum, item) => {
        return sum + (parseFloat(item.price || 0) * (item.quantity || 1));
      }, 0);

      // Prepare items array with all details
      const submissionItems = items.map(item => ({
        brand: item.brand,
        model: item.model,
        storage: item.storage || 'Unknown',
        color: item.color || null,
        condition: item.condition,
        price: parseFloat(item.price || 0),
        quantity: item.quantity || 1,
        deviceType: item.deviceType || 'phone',
        productId: item.productId || null,
        productGid: item.productGid || null,
        variantId: item.variantId || null,
        variantGid: item.variantGid || null,
        productTitle: item.productTitle || null,
        imageUrl: item.imageUrl || null,
        returnPack: item.returnPack !== false
      }));

      submission = {
        id: submissionIdCounter++,
        customerId: customerId || null,
        shopifyCustomerId: shopifyCustomerId || null,
        name,
        email,
        phone: phone || '',
        postcode: postcode || '',
        notes: notes || '',
        items: submissionItems, // Array of items
        itemCount: items.length,
        finalPrice: totalPrice,
        paymentMethod: selectedPaymentMethod,
        paymentDetails: paymentDetails || {},
        confirmations: confirmations || {},
        pageUrl: pageUrl || '',
        status: 'pending',
        paymentStatus: 'pending',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        giftCardCode: null,
        giftCardId: null,
        paymentReference: null,
        paymentDate: null
      };
    } else {
      // Single item submission (backward compatibility)
      const price = isCustomDevice ? 0 : (finalPrice || 0);
      
      submission = {
        id: submissionIdCounter++,
        customerId: customerId || null,
        shopifyCustomerId: shopifyCustomerId || null,
        name,
        email,
        phone: phone || '',
        postcode: postcode || '',
        notes: notes || '',
        brand,
        model,
        storage: storage || 'Unknown',
        condition,
        finalPrice: parseFloat(price),
        deviceType: deviceType || 'phone',
        isCustomDevice: isCustomDevice || false,
        paymentMethod: selectedPaymentMethod,
        paymentDetails: paymentDetails || {},
        confirmations: confirmations || {},
        pageUrl: pageUrl || '',
        status: 'pending',
        paymentStatus: 'pending',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        giftCardCode: null,
        giftCardId: null,
        paymentReference: null,
        paymentDate: null
      };
    }

    tradeInSubmissions.push(submission);
    
    // Ensure MongoDB connection and save
    await ensureMongoConnection();
    
    if (db) {
      try {
        await db.collection('submissions').replaceOne(
          { id: submission.id },
          submission,
          { upsert: true }
        );
        console.log(`✅ Saved submission #${submission.id} to MongoDB`);
      } catch (error) {
        console.error('Error saving to MongoDB:', error);
        // Still try saveSubmissions for consistency
      }
    } else {
      console.error('❌ MongoDB not connected - submission may be lost!');
    }
    
    // Also call saveSubmissions for consistency (will skip file on Vercel)
    await saveSubmissions();

    // Send confirmation email to customer
    try {
      let emailHtml = `
        <h2>Thank you for your trade-in request!</h2>
        <p>Hello ${name},</p>
        <p>We've received your trade-in request:</p>
      `;

      if (isBatchSubmission) {
        emailHtml += `<p><strong>Items (${submission.itemCount}):</strong></p><ul>`;
        submission.items.forEach((item, index) => {
          emailHtml += `
            <li>
              <strong>Item ${index + 1}:</strong> ${item.brand} ${item.model} ${item.storage} 
              (${item.condition}) × ${item.quantity} - £${(item.price * item.quantity).toFixed(2)}
            </li>
          `;
        });
        emailHtml += `</ul><p><strong>Total Estimated Value:</strong> £${submission.finalPrice.toFixed(2)}</p>`;
      } else {
        emailHtml += `
          <ul>
            <li><strong>Device:</strong> ${submission.brand} ${submission.model} ${submission.storage}</li>
            <li><strong>Condition:</strong> ${submission.condition}</li>
            <li><strong>Estimated Value:</strong> £${submission.finalPrice.toFixed(2)}</li>
          </ul>
        `;
      }

      emailHtml += `
        <p>Our team will review your request and get back to you shortly.</p>
        <p>Submission ID: #${submission.id}</p>
      `;

      await transporter.sendMail({
        from: SMTP_FROM,
        to: email,
        subject: 'Trade-In Request Received',
        html: emailHtml
      });
    } catch (emailError) {
      console.error('Error sending confirmation email:', emailError);
      // Don't fail the request if email fails
    }

    // Send notification email to admin
    try {
      let adminHtml = `
        <h2>New Trade-In Request</h2>
        <p><strong>Customer:</strong> ${name} (${email})</p>
        <p><strong>Phone:</strong> ${phone || 'N/A'}</p>
      `;

      if (isBatchSubmission) {
        adminHtml += `<p><strong>Items (${submission.itemCount}):</strong></p><ul>`;
        submission.items.forEach((item, index) => {
          adminHtml += `
            <li>
              <strong>Item ${index + 1}:</strong> ${item.brand} ${item.model} ${item.storage} 
              (${item.condition}) × ${item.quantity} - £${(item.price * item.quantity).toFixed(2)}
            </li>
          `;
        });
        adminHtml += `</ul><p><strong>Total Estimated Value:</strong> £${submission.finalPrice.toFixed(2)}</p>`;
      } else {
        adminHtml += `
          <p><strong>Device:</strong> ${submission.brand} ${submission.model} ${submission.storage}</p>
          <p><strong>Condition:</strong> ${submission.condition}</p>
          <p><strong>Estimated Value:</strong> £${submission.finalPrice.toFixed(2)}</p>
        `;
      }

      adminHtml += `
        <p><strong>Notes:</strong> ${notes || 'None'}</p>
        <p><strong>Submission ID:</strong> #${submission.id}</p>
      `;

      await transporter.sendMail({
        from: SMTP_FROM,
        to: ADMIN_EMAIL,
        subject: `New Trade-In Request #${submission.id}`,
        html: adminHtml
      });
    } catch (emailError) {
      console.error('Error sending admin notification email:', emailError);
    }

    res.json({
      success: true,
      id: submission.id,
      submissionId: submission.id,
      message: 'Trade-in request submitted successfully'
    });

  } catch (error) {
    console.error('Error submitting trade-in:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get admin dashboard statistics
app.get('/api/admin/dashboard', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { startDate, endDate } = req.query;
    
    // Build date filter if provided
    const dateFilter = {};
    if (startDate || endDate) {
      dateFilter.createdAt = {};
      if (startDate) {
        dateFilter.createdAt.$gte = new Date(startDate);
      }
      if (endDate) {
        dateFilter.createdAt.$lte = new Date(endDate);
      }
    }

    // Get product counts by device type
    const deviceTypes = ['phone', 'tablet', 'laptop', 'gaming', 'watch'];
    const productCounts = {};
    for (const deviceType of deviceTypes) {
      const count = await db.collection('trade_in_products').countDocuments({ deviceType: deviceType });
      productCounts[deviceType] = count;
    }

    // Get total products
    const totalProducts = await db.collection('trade_in_products').countDocuments({});

    // Get staff count
    const staffCount = await db.collection('staff_members').countDocuments({});

    // Get submission statistics
    const allSubmissionsQuery = dateFilter.createdAt ? { createdAt: dateFilter.createdAt } : {};
    const totalSubmissions = await db.collection('submissions').countDocuments(allSubmissionsQuery);
    
    // Get submissions by status
    const pendingSubmissions = await db.collection('submissions').countDocuments({ ...allSubmissionsQuery, status: 'pending' });
    const acceptedSubmissions = await db.collection('submissions').countDocuments({ ...allSubmissionsQuery, status: 'accepted' });
    const completedSubmissions = await db.collection('submissions').countDocuments({ ...allSubmissionsQuery, status: 'completed' });
    const rejectedSubmissions = await db.collection('submissions').countDocuments({ ...allSubmissionsQuery, status: 'rejected' });

    // Get submissions that need payment (accepted but not completed)
    const needsPayment = await db.collection('submissions').countDocuments({ 
      ...allSubmissionsQuery,
      status: { $in: ['accepted', 'pending'] },
      paymentStatus: { $ne: 'paid' }
    });

    // Calculate total money spent (sum of all paid amounts)
    const paidSubmissions = await db.collection('submissions').find({
      ...allSubmissionsQuery,
      paymentStatus: 'paid',
      finalPrice: { $exists: true, $ne: null }
    }).toArray();
    
    const totalMoneySpent = paidSubmissions.reduce((sum, sub) => {
      const price = parseFloat(sub.finalPrice) || 0;
      return sum + price;
    }, 0);

    // Calculate total money pending (sum of accepted/pending submissions)
    const pendingPaymentSubmissions = await db.collection('submissions').find({
      ...allSubmissionsQuery,
      status: { $in: ['accepted', 'pending'] },
      paymentStatus: { $ne: 'paid' },
      finalPrice: { $exists: true, $ne: null }
    }).toArray();
    
    const totalMoneyPending = pendingPaymentSubmissions.reduce((sum, sub) => {
      const price = parseFloat(sub.finalPrice) || 0;
      return sum + price;
    }, 0);

    // Get recent activity (last 7 days)
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const recentSubmissions = await db.collection('submissions').countDocuments({
      createdAt: { $gte: sevenDaysAgo }
    });

    // Get store credit (gift card) statistics
    const storeCreditSubmissions = await db.collection('submissions').find({
      ...allSubmissionsQuery,
      giftCardCode: { $exists: true, $ne: null }
    }).toArray();
    
    const totalStoreCreditsIssued = storeCreditSubmissions.length;
    const totalStoreCreditValue = storeCreditSubmissions.reduce((sum, sub) => {
      const price = parseFloat(sub.finalPrice) || 0;
      return sum + price;
    }, 0);

    res.json({
      success: true,
      stats: {
        products: {
          total: totalProducts,
          byType: productCounts
        },
        staff: {
          total: staffCount
        },
        submissions: {
          total: totalSubmissions,
          pending: pendingSubmissions,
          accepted: acceptedSubmissions,
          completed: completedSubmissions,
          rejected: rejectedSubmissions,
          needsPayment: needsPayment,
          recent: recentSubmissions
        },
        payments: {
          totalSpent: totalMoneySpent,
          totalPending: totalMoneyPending
        },
        storeCredits: {
          totalIssued: totalStoreCreditsIssued,
          totalValue: totalStoreCreditValue
        }
      }
    });
  } catch (error) {
    console.error('Error fetching dashboard statistics:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// List all trade-in submissions (admin)
app.get('/api/trade-in/list', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Ensure MongoDB connection
    await ensureMongoConnection();

    const { status, limit = 100, offset = 0 } = req.query;

    let submissions = [];
    let total = 0;

    // Try MongoDB first
    if (db) {
      try {
        const query = status ? { status: status } : {};
        total = await db.collection('submissions').countDocuments(query);
        submissions = await db.collection('submissions')
          .find(query)
          .sort({ createdAt: -1 })
          .skip(parseInt(offset))
          .limit(parseInt(limit))
          .toArray();
        
        res.json({
          success: true,
          submissions: submissions,
          total: total,
          limit: parseInt(limit),
          offset: parseInt(offset)
        });
        return;
      } catch (error) {
        console.error('Error fetching from MongoDB:', error);
      }
    }

    // Fallback to in-memory
    submissions = [...tradeInSubmissions];
    if (status) {
      submissions = submissions.filter(s => s.status === status);
    }
    submissions.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    total = submissions.length;
    const paginated = submissions.slice(parseInt(offset), parseInt(offset) + parseInt(limit));

    res.json({
      success: true,
      submissions: paginated,
      total: total,
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (error) {
    console.error('Error fetching trade-in submissions:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get single submission (admin)
app.get('/api/trade-in/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const id = parseInt(req.params.id);
    const submission = tradeInSubmissions.find(s => s.id === id);

    if (!submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    res.json({
      success: true,
      submission
    });

  } catch (error) {
    console.error('Error fetching submission:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update submission (edit)
app.put('/api/trade-in/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const submissionId = parseInt(req.params.id);
    const updateData = req.body;
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(staffIdentifier, 'tradeInEdit')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "tradeInEdit" permission to edit submissions.' 
      });
    }

    // Ensure MongoDB connection
    await ensureMongoConnection();

    let submission = null;
    if (db) {
      submission = await db.collection('submissions').findOne({ id: submissionId });
    } else {
      submission = tradeInSubmissions.find(s => s.id === submissionId);
    }

    if (!submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    // Track changes for audit
    const changes = [];
    if (updateData.name && updateData.name !== submission.name) changes.push({ field: 'name', old: submission.name, new: updateData.name });
    if (updateData.email && updateData.email !== submission.email) changes.push({ field: 'email', old: submission.email, new: updateData.email });
    if (updateData.phone !== undefined && updateData.phone !== submission.phone) changes.push({ field: 'phone', old: submission.phone || 'null', new: updateData.phone || 'null' });
    if (updateData.postcode !== undefined && updateData.postcode !== submission.postcode) changes.push({ field: 'postcode', old: submission.postcode || 'null', new: updateData.postcode || 'null' });
    if (updateData.brand && updateData.brand !== submission.brand) changes.push({ field: 'brand', old: submission.brand, new: updateData.brand });
    if (updateData.model && updateData.model !== submission.model) changes.push({ field: 'model', old: submission.model, new: updateData.model });
    if (updateData.storage !== undefined && updateData.storage !== submission.storage) changes.push({ field: 'storage', old: submission.storage || 'null', new: updateData.storage || 'null' });
    if (updateData.condition && updateData.condition !== submission.condition) changes.push({ field: 'condition', old: submission.condition, new: updateData.condition });
    if (updateData.finalPrice !== undefined && parseFloat(updateData.finalPrice) !== submission.finalPrice) changes.push({ field: 'finalPrice', old: `£${submission.finalPrice.toFixed(2)}`, new: `£${parseFloat(updateData.finalPrice).toFixed(2)}` });
    if (updateData.status && updateData.status !== submission.status) changes.push({ field: 'status', old: submission.status, new: updateData.status });
    if (updateData.notes !== undefined && updateData.notes !== submission.notes) changes.push({ field: 'notes', old: submission.notes || 'null', new: updateData.notes || 'null' });

    // Update submission fields
    const updatedSubmission = {
      ...submission,
      name: updateData.name || submission.name,
      email: updateData.email || submission.email,
      phone: updateData.phone !== undefined ? updateData.phone : submission.phone,
      postcode: updateData.postcode !== undefined ? updateData.postcode : submission.postcode,
      brand: updateData.brand || submission.brand,
      model: updateData.model || submission.model,
      storage: updateData.storage !== undefined ? updateData.storage : submission.storage,
      condition: updateData.condition || submission.condition,
      finalPrice: updateData.finalPrice !== undefined ? parseFloat(updateData.finalPrice) : submission.finalPrice,
      status: updateData.status || submission.status,
      notes: updateData.notes !== undefined ? updateData.notes : submission.notes,
      updatedAt: new Date().toISOString(),
      lastEditedBy: staffIdentifier
    };

    // Save to MongoDB
    if (db) {
      await db.collection('submissions').updateOne(
        { id: submissionId },
        { $set: updatedSubmission }
      );
    }

    // Update in-memory cache
    const index = tradeInSubmissions.findIndex(s => s.id === submissionId);
    if (index !== -1) {
      tradeInSubmissions[index] = updatedSubmission;
    }

    // Save to file (if not on Vercel)
    if (process.env.VERCEL !== '1') {
      await saveSubmissions();
    }

    // Log audit trail
    if (changes.length > 0) {
      await logAudit({
        action: 'update_submission',
        resourceType: 'submission',
        resourceId: submissionId.toString(),
        staffIdentifier: staffIdentifier,
        changes: changes
      });
    }

    console.log(`✅ Updated submission #${submissionId}`);

    res.json({
      success: true,
      submission: updatedSubmission
    });

  } catch (error) {
    console.error('Error updating submission:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Update submission status (admin)
app.post('/api/trade-in/:id/update-status', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const id = parseInt(req.params.id);
    const { status, notes } = req.body;
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(staffIdentifier, 'tradeInStatus')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "tradeInStatus" permission to update submission status.' 
      });
    }

    if (!['pending', 'accepted', 'rejected', 'completed'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    let submission = null;

    // Try MongoDB first
    if (db) {
      try {
        submission = await db.collection('submissions').findOne({ id: id });
        if (!submission) {
          return res.status(404).json({ error: 'Submission not found' });
        }
        
        submission.status = status;
        submission.updatedAt = new Date().toISOString();
        if (notes) {
          submission.adminNotes = notes;
        }
        
        await db.collection('submissions').replaceOne({ id: id }, submission);
        
        // Also update in-memory for consistency
        const index = tradeInSubmissions.findIndex(s => s.id === id);
        if (index !== -1) {
          tradeInSubmissions[index] = submission;
        } else {
          tradeInSubmissions.push(submission);
        }
      } catch (error) {
        console.error('Error updating in MongoDB:', error);
        // Fall through to in-memory update
      }
    }
    
    // Fallback to in-memory
    if (!submission) {
      submission = tradeInSubmissions.find(s => s.id === id);
      if (!submission) {
        return res.status(404).json({ error: 'Submission not found' });
      }
      
      const oldStatus = submission.status;
      submission.status = status;
      submission.updatedAt = new Date().toISOString();
      submission.lastEditedBy = staffIdentifier;
      if (notes) {
        submission.adminNotes = notes;
      }
      
      await saveSubmissions();
      
      // Log audit trail
      const changes = [{ field: 'status', old: oldStatus, new: status }];
      if (notes) {
        changes.push({ field: 'adminNotes', old: submission.adminNotes || 'null', new: notes });
      }
      await logAudit({
        action: 'update_status',
        resourceType: 'submission',
        resourceId: id.toString(),
        staffIdentifier: staffIdentifier,
        changes: changes
      });
    }

    // Send email to customer about status change
    try {
      const statusMessages = {
        accepted: 'Your trade-in request has been accepted!',
        rejected: 'Your trade-in request has been rejected.',
        completed: 'Your trade-in has been completed and credit has been issued.'
      };

      await transporter.sendMail({
        from: SMTP_FROM,
        to: submission.email,
        subject: `Trade-In Request #${submission.id} - ${status.charAt(0).toUpperCase() + status.slice(1)}`,
        html: `
          <h2>${statusMessages[status] || 'Your trade-in request status has been updated'}</h2>
          <p>Hello ${submission.name},</p>
          <p>Your trade-in request (#${submission.id}) status has been updated to: <strong>${status}</strong></p>
          ${notes ? `<p><strong>Notes:</strong> ${notes}</p>` : ''}
          ${submission.giftCardCode ? `<p><strong>Gift Card Code:</strong> ${submission.giftCardCode}</p>` : ''}
        `
      });
    } catch (emailError) {
      console.error('Error sending status update email:', emailError);
    }

    res.json({
      success: true,
      message: 'Status updated successfully',
      submission
    });

  } catch (error) {
    console.error('Error updating submission status:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Issue store credit (gift card) for accepted trade-in
app.post('/api/trade-in/:id/issue-credit', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const id = parseInt(req.params.id);
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(staffIdentifier, 'tradeInCredit')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "tradeInCredit" permission to issue gift cards.' 
      });
    }
    
    let submission = null;

    // Ensure MongoDB connection
    await ensureMongoConnection();

    // Try MongoDB first
    if (db) {
      try {
        submission = await db.collection('submissions').findOne({ id: id });
        if (submission) {
          console.log(`Found submission #${id} in MongoDB for credit issuance`);
        }
      } catch (error) {
        console.error('Error fetching from MongoDB:', error);
      }
    }
    
    // Fallback to in-memory
    if (!submission) {
      submission = tradeInSubmissions.find(s => s.id === id);
    }

    if (!submission) {
      return res.status(404).json({ 
        success: false,
        error: 'Submission not found' 
      });
    }

    if (submission.status !== 'accepted') {
      return res.status(400).json({ 
        success: false,
        error: 'Can only issue credit for accepted submissions',
        currentStatus: submission.status
      });
    }

    if (submission.giftCardCode) {
      return res.status(400).json({ 
        success: false,
        error: 'Credit already issued for this submission',
        giftCardCode: submission.giftCardCode
      });
    }

    // First, try to find customer by email to assign gift card to them
    // This ensures the gift card appears in their account
    let customerId = null;
    try {
      const customerQuery = `
        query {
          customers(first: 1, query: "email:${submission.email}") {
            edges {
              node {
                id
              }
            }
          }
        }
      `;

      const customerResponse = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        },
        body: JSON.stringify({ query: customerQuery })
      });

      const customerData = await customerResponse.json();
      if (customerData.data?.customers?.edges?.length > 0) {
        customerId = customerData.data.customers.edges[0].node.id;
        console.log(`Found customer ${customerId} for email ${submission.email}`);
      }
    } catch (error) {
      console.warn('Could not find customer by email, creating code-based gift card:', error);
    }

    // Generate secure, random gift card code (not predictable)
    // Format: TECHCORNER-XXXX-XXXX (e.g., TECHCORNER-A3K9-M7P2)
    // This prevents guessing sequential codes
    function generateSecureGiftCardCode() {
      const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // Removed confusing chars (0, O, I, 1)
      let code = 'TECHCORNER-';
      
      // Generate 4 random characters
      for (let i = 0; i < 4; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
      }
      
      code += '-';
      
      // Generate another 4 random characters
      for (let i = 0; i < 4; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
      }
      
      return code;
    }
    
    // Check if gift card code already exists (prevent duplicates)
    async function isGiftCardCodeUnique(code) {
      // Check in our submissions database
      let existingSubmission = null;
      if (db) {
        try {
          existingSubmission = await db.collection('submissions').findOne({ giftCardCode: code });
        } catch (error) {
          console.warn('Error checking MongoDB for duplicate code:', error);
        }
      }
      
      // Also check in-memory submissions
      if (!existingSubmission) {
        existingSubmission = tradeInSubmissions.find(s => s.giftCardCode === code);
      }
      
      // Check with Shopify API to see if code exists
      try {
        const checkResponse = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/gift_cards.json?code=${encodeURIComponent(code)}`, {
          method: 'GET',
          headers: {
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          }
        });
        
        if (checkResponse.ok) {
          const checkData = await checkResponse.json();
          if (checkData.gift_cards && checkData.gift_cards.length > 0) {
            console.log(`Code ${code} already exists in Shopify`);
            return false; // Code exists
          }
        }
      } catch (error) {
        console.warn('Error checking Shopify for duplicate code:', error);
        // If we can't check Shopify, assume it's unique (Shopify will reject if duplicate)
      }
      
      return !existingSubmission; // Return true if no existing submission found
    }
    
    // Generate unique gift card code with duplicate checking
    let giftCardCode;
    let attempts = 0;
    const maxAttempts = 10; // Prevent infinite loop
    
    do {
      giftCardCode = generateSecureGiftCardCode();
      attempts++;
      
      if (attempts >= maxAttempts) {
        // Fallback: use submission ID + random suffix if we can't generate unique code
        const randomSuffix = Math.random().toString(36).substring(2, 6).toUpperCase();
        giftCardCode = `TECHCORNER-${submission.id.toString().padStart(6, '0')}-${randomSuffix}`;
        console.warn(`Could not generate unique code after ${maxAttempts} attempts, using fallback: ${giftCardCode}`);
        break;
      }
    } while (!(await isGiftCardCodeUnique(giftCardCode)));
    
    console.log(`Generated unique gift card code: ${giftCardCode} (attempts: ${attempts})`);
    
    // Convert price to string with 2 decimal places for MoneyV2 format
    const amount = parseFloat(submission.finalPrice).toFixed(2);
    
    // Use REST API instead of GraphQL to avoid permission scope issues
    // REST API: POST /admin/api/2024-01/gift_cards.json
    console.log('Creating gift card via REST API:', { code: giftCardCode, amount: amount, submissionId: submission.id });

    const restPayload = {
      gift_card: {
        initial_value: parseFloat(amount),
        code: giftCardCode,
        note: `Trade-in #${submission.id} - ${submission.brand} ${submission.model}`
      }
    };

    const response = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/gift_cards.json`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
      },
      body: JSON.stringify(restPayload)
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Shopify REST API response error:', response.status, errorText);
      
      // If it's a permissions error, provide helpful message
      if (response.status === 403 || response.status === 401) {
        return res.status(403).json({ 
          success: false,
          error: 'Permission denied: Your Shopify access token needs "write_gift_cards" scope. Please update your Shopify app permissions in Settings > Apps and sales channels > Develop apps.',
          details: errorText
        });
      }
      
      return res.status(500).json({ 
        success: false,
        error: `Shopify API error: ${response.status}`,
        details: errorText
      });
    }

    const data = await response.json();
    console.log('Shopify gift card creation response:', JSON.stringify(data, null, 2));
    
    // REST API returns gift_card object directly
    if (!data.gift_card) {
      console.error('No gift card returned from Shopify:', data);
      return res.status(500).json({ 
        success: false,
        error: 'Failed to create gift card - no gift card returned',
        details: data
      });
    }

    const giftCard = data.gift_card;
    const finalGiftCardCode = giftCard.code || giftCardCode;
    
    // If customer was found, try to assign the gift card to them via REST API
    if (customerId) {
      try {
        // Extract customer ID number from GID
        const customerIdNum = customerId.split('/').pop();
        
        // Update gift card to assign to customer
        const updatePayload = {
          gift_card: {
            customer_id: parseInt(customerIdNum)
          }
        };
        
        const updateResponse = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/gift_cards/${giftCard.id}.json`, {
          method: 'PUT',
          headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          },
          body: JSON.stringify(updatePayload)
        });
        
        if (updateResponse.ok) {
          console.log(`Gift card assigned to customer ${customerId}`);
        } else {
          console.warn('Could not assign gift card to customer (gift card still created)');
        }
      } catch (assignError) {
        console.warn('Error assigning gift card to customer (gift card still created):', assignError);
        // Gift card is still created, just not assigned - customer can still use the code
      }
    }
    
    // Update submission
    submission.giftCardCode = finalGiftCardCode;
    submission.giftCardId = giftCard.id.toString();
    submission.status = 'completed';
    submission.updatedAt = new Date().toISOString();
    submission.lastEditedBy = staffIdentifier;
    
    // Save to MongoDB if available
    if (db) {
      try {
        await db.collection('submissions').replaceOne({ id: submission.id }, submission);
      } catch (error) {
        console.error('Error saving to MongoDB:', error);
      }
    }
    
    // Also update in-memory
    const index = tradeInSubmissions.findIndex(s => s.id === submission.id);
    if (index !== -1) {
      tradeInSubmissions[index] = submission;
    } else {
      tradeInSubmissions.push(submission);
    }
    
    // Save to file as fallback
    await saveSubmissions();
    
    // Log audit trail
    await logAudit({
      action: 'issue_credit',
      resourceType: 'submission',
      resourceId: submission.id.toString(),
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'giftCardCode',
        old: null,
        new: finalGiftCardCode,
        description: `Issued gift card code: ${finalGiftCardCode} for £${submission.finalPrice.toFixed(2)}`
      }, {
        field: 'status',
        old: 'accepted',
        new: 'completed'
      }]
    });

    // Send gift card email to customer
    try {
      await transporter.sendMail({
        from: SMTP_FROM,
        to: submission.email,
        subject: `Your Store Credit - Trade-In #${submission.id}`,
        html: `
          <h2>Your Store Credit Has Been Issued!</h2>
          <p>Hello ${submission.name},</p>
          <p>Your trade-in request (#${submission.id}) has been completed and store credit has been issued.</p>
          <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3 style="margin-top: 0;">Gift Card Code</h3>
            <p style="font-size: 24px; font-weight: bold; letter-spacing: 2px; color: #ef4444;">${finalGiftCardCode}</p>
            <p><strong>Amount:</strong> £${submission.finalPrice.toFixed(2)}</p>
          </div>
          <p>You can use this code at checkout to apply your store credit to any purchase.</p>
          <p>Thank you for trading in with us!</p>
        `
      });
    } catch (emailError) {
      console.error('Error sending gift card email:', emailError);
    }

    res.json({
      success: true,
      message: 'Store credit issued successfully',
      giftCard: {
        code: finalGiftCardCode,
        id: giftCard.id,
        amount: submission.finalPrice,
        currency: 'GBP'
      },
      submission
    });

  } catch (error) {
    console.error('Error issuing credit:', error);
    res.status(500).json({ 
      success: false,
      error: 'Internal server error',
      message: error.message,
      details: error.stack
    });
  }
});

// Issue cash payment (Bank Transfer or PayPal)
app.post('/api/trade-in/:id/issue-cash-payment', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const id = parseInt(req.params.id);
    const { paymentMethod } = req.body; // 'bank_transfer' or 'paypal'
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    // Check permission
    if (!await hasPermission(staffIdentifier, 'tradeInPayment')) {
      return res.status(403).json({ 
        error: 'Permission denied. You need "tradeInPayment" permission to issue cash payments.' 
      });
    }

    if (!['bank_transfer', 'paypal'].includes(paymentMethod)) {
      return res.status(400).json({ error: 'Invalid payment method' });
    }

    let submission = null;

    // Try MongoDB first
    if (db) {
      try {
        submission = await db.collection('submissions').findOne({ id: id });
      } catch (error) {
        console.error('Error fetching from MongoDB:', error);
      }
    }
    
    // Fallback to in-memory
    if (!submission) {
      submission = tradeInSubmissions.find(s => s.id === id);
    }

    if (!submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    if (submission.status !== 'accepted') {
      return res.status(400).json({ 
        error: 'Can only issue payment for accepted submissions' 
      });
    }

    if (submission.paymentReference) {
      return res.status(400).json({ 
        error: 'Payment already issued for this submission' 
      });
    }

    // Generate payment reference
    const paymentReference = `${paymentMethod.toUpperCase()}-${submission.id.toString().padStart(6, '0')}-${Date.now().toString().slice(-6)}`;
    
    // Process payment based on method
    let paymentResult = null;
    
    if (paymentMethod === 'paypal') {
      // PayPal payment processing
      const paypalEmail = submission.paymentDetails?.paypalEmail;
      
      if (!paypalEmail) {
        return res.status(400).json({ 
          error: 'PayPal email not found in submission' 
        });
      }

      // TODO: Integrate with PayPal API for automated payments
      // For now, this creates a payment record and sends email
      // You can integrate PayPal Payouts API here for automated payments
      
      paymentResult = {
        method: 'paypal',
        email: paypalEmail,
        amount: submission.finalPrice,
        status: 'processing', // Will be 'completed' when PayPal confirms
        reference: paymentReference
      };
      
      console.log(`PayPal payment initiated: ${paypalEmail}, Amount: £${submission.finalPrice}, Reference: ${paymentReference}`);
      
      // Note: To enable automated PayPal payments, you need to:
      // 1. Set up PayPal Payouts API
      // 2. Add PAYPAL_CLIENT_ID and PAYPAL_SECRET to environment variables
      // 3. Implement PayPal Payout API call here
      
    } else if (paymentMethod === 'bank_transfer') {
      // Bank transfer processing
      const bankDetails = submission.paymentDetails;
      
      if (!bankDetails?.firstName || !bankDetails?.lastName || !bankDetails?.sortCode || !bankDetails?.accountNumber) {
        return res.status(400).json({ 
          error: 'Bank account details not found in submission' 
        });
      }

      // TODO: Integrate with bank transfer API (e.g., Stripe Connect, Open Banking)
      // For now, this creates a payment record and sends email
      // You can integrate bank transfer APIs here for automated payments
      
      paymentResult = {
        method: 'bank_transfer',
        firstName: bankDetails.firstName,
        lastName: bankDetails.lastName,
        accountNumber: bankDetails.accountNumber,
        sortCode: bankDetails.sortCode,
        amount: submission.finalPrice,
        status: 'processing', // Will be 'completed' when transfer is confirmed
        reference: paymentReference
      };
      
      console.log(`Bank transfer initiated: ${bankDetails.firstName} ${bankDetails.lastName}, Amount: £${submission.finalPrice}, Reference: ${paymentReference}`);
      
      // Note: To enable automated bank transfers, you need to:
      // 1. Set up Stripe Connect or Open Banking API
      // 2. Add API credentials to environment variables
      // 3. Implement bank transfer API call here
    }

    // Update submission
    submission.paymentReference = paymentReference;
    submission.paymentStatus = 'processing';
    submission.paymentDate = new Date().toISOString();
    submission.status = 'completed';
    submission.updatedAt = new Date().toISOString();
    submission.lastEditedBy = staffIdentifier;
    
    // Save to MongoDB if available
    if (db) {
      try {
        await db.collection('submissions').replaceOne({ id: submission.id }, submission);
      } catch (error) {
        console.error('Error saving to MongoDB:', error);
      }
    }
    
    // Also update in-memory
    const index = tradeInSubmissions.findIndex(s => s.id === submission.id);
    if (index !== -1) {
      tradeInSubmissions[index] = submission;
    } else {
      tradeInSubmissions.push(submission);
    }
    
    // Save to file as fallback
    await saveSubmissions();
    
    // Log audit trail
    await logAudit({
      action: 'issue_payment',
      resourceType: 'submission',
      resourceId: submission.id.toString(),
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'paymentReference',
        old: null,
        new: paymentReference,
        description: `Issued ${paymentMethod} payment: ${paymentReference} for £${submission.finalPrice.toFixed(2)}`
      }, {
        field: 'status',
        old: 'accepted',
        new: 'completed'
      }]
    });

    // Send payment email to customer
    try {
      let emailSubject = '';
      let emailContent = '';
      
      if (paymentMethod === 'paypal') {
        emailSubject = `Your PayPal Payment - Trade-In #${submission.id}`;
        emailContent = `
          <h2>Your Payment Has Been Processed!</h2>
          <p>Hello ${submission.name},</p>
          <p>Your trade-in request (#${submission.id}) has been completed and payment has been sent to your PayPal account.</p>
          <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3 style="margin-top: 0;">Payment Details</h3>
            <p><strong>Amount:</strong> £${submission.finalPrice.toFixed(2)}</p>
            <p><strong>PayPal Email:</strong> ${submission.paymentDetails.paypalEmail}</p>
            <p><strong>Payment Reference:</strong> ${paymentReference}</p>
            <p><strong>Status:</strong> Processing (usually completes within 1-3 business days)</p>
          </div>
          <p>You should receive the payment in your PayPal account shortly. Please check your PayPal account for confirmation.</p>
          <p>Thank you for trading in with us!</p>
        `;
      } else {
        emailSubject = `Your Bank Transfer - Trade-In #${submission.id}`;
        emailContent = `
          <h2>Your Payment Has Been Processed!</h2>
          <p>Hello ${submission.name},</p>
          <p>Your trade-in request (#${submission.id}) has been completed and bank transfer has been initiated.</p>
          <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <h3 style="margin-top: 0;">Payment Details</h3>
            <p><strong>Amount:</strong> £${submission.finalPrice.toFixed(2)}</p>
            <p><strong>First Name:</strong> ${submission.paymentDetails.firstName}</p>
            <p><strong>Last Name:</strong> ${submission.paymentDetails.lastName}</p>
            <p><strong>Sort Code:</strong> ${submission.paymentDetails.sortCode}</p>
            <p><strong>Account Number:</strong> ${submission.paymentDetails.accountNumber}</p>
            <p><strong>Payment Reference:</strong> ${paymentReference}</p>
            <p><strong>Status:</strong> Processing (usually completes within 1-3 business days)</p>
          </div>
          <p>The payment will be transferred to your bank account. Please allow 1-3 business days for the transfer to complete.</p>
          <p>Thank you for trading in with us!</p>
        `;
      }

      await transporter.sendMail({
        from: SMTP_FROM,
        to: submission.email,
        subject: emailSubject,
        html: emailContent
      });
    } catch (emailError) {
      console.error('Error sending payment email:', emailError);
    }

    res.json({
      success: true,
      message: 'Payment processed successfully',
      paymentReference: paymentReference,
      paymentMethod: paymentMethod,
      amount: submission.finalPrice,
      submission
    });

  } catch (error) {
    console.error('Error processing cash payment:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================
// BACKUP SYSTEM
// ============================================

// Helper function to get last backup timestamp for a collection
async function getLastBackupTimestamp(collectionName, backupType) {
  try {
    const lastBackup = await db.collection('backups').findOne(
      { 
        'collections.name': collectionName,
        type: backupType
      },
      { sort: { createdAt: -1 } }
    );
    return lastBackup ? new Date(lastBackup.createdAt) : null;
  } catch (error) {
    console.error('Error getting last backup timestamp:', error);
    return null;
  }
}

// Helper function to get changed documents since last backup
async function getChangedDocuments(collectionName, lastBackupTime) {
  if (!lastBackupTime) {
    // Full backup - get all documents
    return await db.collection(collectionName).find({}).toArray();
  }
  
  // Incremental backup - get only changed documents
  const query = {
    $or: [
      { createdAt: { $gte: lastBackupTime } },
      { updatedAt: { $gte: lastBackupTime } }
    ]
  };
  
  return await db.collection(collectionName).find(query).toArray();
}

// ============================================
// SHOPIFY BACKUP HELPERS
// ============================================

// Fetch all products from Shopify
async function fetchShopifyProducts() {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      return { error: 'Shopify credentials not configured' };
    }

    const products = [];
    let hasNextPage = true;
    let cursor = null;

    while (hasNextPage) {
      const query = cursor 
        ? `?limit=250&since_id=${cursor}`
        : '?limit=250';

      const response = await fetch(
        `https://${SHOPIFY_SHOP}/admin/api/2024-01/products.json${query}`,
        {
          headers: {
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
            'Content-Type': 'application/json'
          }
        }
      );

      if (!response.ok) {
        throw new Error(`Shopify API error: ${response.status} ${response.statusText}`);
      }

      const data = await response.json();
      if (data.products && data.products.length > 0) {
        products.push(...data.products);
        cursor = data.products[data.products.length - 1].id;
        hasNextPage = data.products.length === 250;
      } else {
        hasNextPage = false;
      }
    }

    return { products, count: products.length };
  } catch (error) {
    console.error('Error fetching Shopify products:', error);
    return { error: error.message, products: [], count: 0 };
  }
}

// Fetch theme files from Shopify
async function fetchShopifyThemes() {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      return { error: 'Shopify credentials not configured' };
    }

    const response = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/themes.json`,
      {
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Shopify API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    const themes = data.themes || [];

    // Fetch assets for each theme
    const themesWithAssets = await Promise.all(
      themes.map(async (theme) => {
        try {
          const assetsResponse = await fetch(
            `https://${SHOPIFY_SHOP}/admin/api/2024-01/themes/${theme.id}/assets.json`,
            {
              headers: {
                'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
                'Content-Type': 'application/json'
              }
            }
          );

          if (assetsResponse.ok) {
            const assetsData = await assetsResponse.json();
            theme.assets = assetsData.assets || [];
          } else {
            theme.assets = [];
          }
        } catch (error) {
          console.error(`Error fetching assets for theme ${theme.id}:`, error);
          theme.assets = [];
        }
        return theme;
      })
    );

    return { themes: themesWithAssets, count: themesWithAssets.length };
  } catch (error) {
    console.error('Error fetching Shopify themes:', error);
    return { error: error.message, themes: [], count: 0 };
  }
}

// Fetch script tags from Shopify
async function fetchShopifyScriptTags() {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      return { error: 'Shopify credentials not configured' };
    }

    const response = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/script_tags.json`,
      {
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!response.ok) {
      // Get error details from response
      let errorMessage = `Shopify API error: ${response.status} ${response.statusText}`;
      try {
        const errorData = await response.json();
        if (errorData.errors) {
          errorMessage += ` - ${JSON.stringify(errorData.errors)}`;
        } else if (errorData.error) {
          errorMessage += ` - ${errorData.error}`;
        }
      } catch (e) {
        // If response is not JSON, use status text
      }
      
      // If 401 or 403, log detailed error but continue backup
      if (response.status === 401 || response.status === 403) {
        console.warn('⚠️ Script tags backup failed:', errorMessage);
        console.warn('⚠️ Check: 1) Access token is valid, 2) read_script_tags scope is granted, 3) Token has not expired');
        return { scriptTags: [], count: 0, error: errorMessage };
      }
      throw new Error(errorMessage);
    }

    const data = await response.json();
    return { scriptTags: data.script_tags || [], count: (data.script_tags || []).length };
  } catch (error) {
    console.error('Error fetching Shopify script tags:', error);
    // Return empty array instead of failing the entire backup
    return { error: error.message, scriptTags: [], count: 0 };
  }
}

// Fetch metaobjects from Shopify (GraphQL)
async function fetchShopifyMetaobjects() {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      return { error: 'Shopify credentials not configured' };
    }

    const query = `
      {
        metaobjects(first: 250) {
          edges {
            node {
              id
              type
              handle
              fields {
                key
                value
                type
              }
            }
          }
        }
      }
    `;

    const response = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`,
      {
        method: 'POST',
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query })
      }
    );

    if (!response.ok) {
      throw new Error(`Shopify API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    const metaobjects = data.data?.metaobjects?.edges?.map(edge => edge.node) || [];
    return { metaobjects, count: metaobjects.length };
  } catch (error) {
    console.error('Error fetching Shopify metaobjects:', error);
    return { error: error.message, metaobjects: [], count: 0 };
  }
}

// Fetch blog posts and articles from Shopify
async function fetchShopifyContent() {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      return { error: 'Shopify credentials not configured' };
    }

    // Fetch blogs
    const blogsResponse = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/blogs.json`,
      {
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        }
      }
    );

    const blogs = blogsResponse.ok ? (await blogsResponse.json()).blogs || [] : [];

    // Fetch articles for each blog
    const blogsWithArticles = await Promise.all(
      blogs.map(async (blog) => {
        try {
          const articlesResponse = await fetch(
            `https://${SHOPIFY_SHOP}/admin/api/2024-01/blogs/${blog.id}/articles.json`,
            {
              headers: {
                'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
                'Content-Type': 'application/json'
              }
            }
          );

          if (articlesResponse.ok) {
            const articlesData = await articlesResponse.json();
            blog.articles = articlesData.articles || [];
          } else {
            blog.articles = [];
          }
        } catch (error) {
          console.error(`Error fetching articles for blog ${blog.id}:`, error);
          blog.articles = [];
        }
        return blog;
      })
    );

    return { blogs: blogsWithArticles, count: blogsWithArticles.length };
  } catch (error) {
    console.error('Error fetching Shopify content:', error);
    return { error: error.message, blogs: [], count: 0 };
  }
}

// Fetch files from Shopify
async function fetchShopifyFiles() {
  try {
    if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
      return { error: 'Shopify credentials not configured' };
    }

    const response = await fetch(
      `https://${SHOPIFY_SHOP}/admin/api/2024-01/files.json`,
      {
        headers: {
          'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Shopify API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    return { files: data.files || [], count: (data.files || []).length };
  } catch (error) {
    console.error('Error fetching Shopify files:', error);
    return { error: error.message, files: [], count: 0 };
  }
}

// Create backup (full or incremental)
app.post('/api/backup/create', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffIdentifier, 'backupCreate')) {
      return res.status(403).json({ error: 'Permission denied. You need "backupCreate" permission.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { type = 'manual', forceFull = false } = req.body;
    
    // Determine backup type
    let backupType = 'incremental';
    if (forceFull || type === 'full' || type === 'manual') {
      backupType = 'full';
    } else {
      // Check if we need a full backup (weekly)
      const lastFullBackup = await db.collection('backups').findOne(
        { type: 'full' },
        { sort: { createdAt: -1 } }
      );
      
      if (!lastFullBackup) {
        backupType = 'full'; // First backup must be full
      } else {
        const daysSinceFullBackup = (Date.now() - new Date(lastFullBackup.createdAt).getTime()) / (1000 * 60 * 60 * 24);
        if (daysSinceFullBackup >= 7) {
          backupType = 'full'; // Weekly full backup
        }
      }
    }

    const { includeShopify = true } = req.body;
    
    const collectionsToBackup = ['products', 'submissions', 'staff_members', 'audit_logs'];
    const backupData = {
      type: backupType,
      createdAt: new Date().toISOString(),
      createdBy: staffIdentifier,
      collections: [],
      shopify: {}
    };

    // Backup MongoDB collections
    for (const collectionName of collectionsToBackup) {
      let documents;
      
      if (backupType === 'full') {
        // Full backup - get all documents
        documents = await db.collection(collectionName).find({}).toArray();
      } else {
        // Incremental backup - get only changed documents since last backup (incremental or full)
        const lastBackup = await db.collection('backups').findOne(
          { 
            'collections.name': collectionName
          },
          { sort: { createdAt: -1 } }
        );
        
        const lastBackupTime = lastBackup ? new Date(lastBackup.createdAt) : null;
        documents = await getChangedDocuments(collectionName, lastBackupTime);
      }

      backupData.collections.push({
        name: collectionName,
        count: documents.length,
        data: documents
      });
    }

    // Backup Shopify data (only on full backups or if explicitly requested)
    if (includeShopify && (backupType === 'full' || req.body.forceShopify)) {
      console.log('🔄 Starting Shopify data backup...');
      
      try {
        // Fetch all Shopify data in parallel
        const [productsData, themesData, scriptTagsData, metaobjectsData, contentData] = await Promise.all([
          fetchShopifyProducts(),
          fetchShopifyThemes(),
          fetchShopifyScriptTags(),
          fetchShopifyMetaobjects(),
          fetchShopifyContent()
        ]);

        backupData.shopify = {
          products: productsData.products || [],
          productsCount: productsData.count || 0,
          productsError: productsData.error || null,
          themes: themesData.themes || [],
          themesCount: themesData.count || 0,
          themesError: themesData.error || null,
          scriptTags: scriptTagsData.scriptTags || [],
          scriptTagsCount: scriptTagsData.count || 0,
          scriptTagsError: scriptTagsData.error || null,
          metaobjects: metaobjectsData.metaobjects || [],
          metaobjectsCount: metaobjectsData.count || 0,
          metaobjectsError: metaobjectsData.error || null,
          blogs: contentData.blogs || [],
          blogsCount: contentData.count || 0,
          blogsError: contentData.error || null,
          backedUpAt: new Date().toISOString()
        };

        console.log('✅ Shopify backup completed:', {
          products: backupData.shopify.productsCount,
          themes: backupData.shopify.themesCount,
          scriptTags: backupData.shopify.scriptTagsCount,
          metaobjects: backupData.shopify.metaobjectsCount,
          blogs: backupData.shopify.blogsCount
        });
      } catch (error) {
        console.error('❌ Error during Shopify backup:', error);
        backupData.shopify = {
          error: error.message,
          backedUpAt: new Date().toISOString()
        };
      }
    }

    // Calculate total size
    const totalSize = JSON.stringify(backupData).length;
    backupData.size = totalSize;
    backupData.sizeFormatted = formatBytes(totalSize);

    // Save backup
    const result = await db.collection('backups').insertOne(backupData);

    // Calculate MongoDB and Shopify counts
    const mongoCount = backupData.collections.reduce((sum, col) => sum + col.count, 0);
    const shopifyCount = backupData.shopify && Object.keys(backupData.shopify).length > 0
      ? (backupData.shopify.productsCount || 0) +
        (backupData.shopify.themesCount || 0) +
        (backupData.shopify.scriptTagsCount || 0) +
        (backupData.shopify.metaobjectsCount || 0) +
        (backupData.shopify.blogsCount || 0)
      : 0;

    // Log audit
    await logAudit({
      action: 'create_backup',
      resourceType: 'backup',
      resourceId: result.insertedId.toString(),
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'type',
        old: null,
        new: backupType,
        description: `Created ${backupType} backup with ${mongoCount} MongoDB records and ${shopifyCount} Shopify items`
      }],
      metadata: {
        collections: collectionsToBackup,
        totalSize: totalSize,
        shopifyIncluded: includeShopify && (backupType === 'full' || req.body.forceShopify)
      }
    });

    res.json({
      success: true,
      backup: {
        _id: result.insertedId.toString(),
        type: backupType,
        createdAt: backupData.createdAt,
        size: totalSize,
        sizeFormatted: backupData.sizeFormatted,
        collections: backupData.collections.map(col => ({
          name: col.name,
          count: col.count
        })),
        shopify: backupData.shopify && Object.keys(backupData.shopify).length > 0 ? {
          productsCount: backupData.shopify.productsCount || 0,
          themesCount: backupData.shopify.themesCount || 0,
          scriptTagsCount: backupData.shopify.scriptTagsCount || 0,
          metaobjectsCount: backupData.shopify.metaobjectsCount || 0,
          blogsCount: backupData.shopify.blogsCount || 0,
          hasErrors: !!(backupData.shopify.productsError || backupData.shopify.themesError || backupData.shopify.scriptTagsError || backupData.shopify.metaobjectsError || backupData.shopify.blogsError),
          productsError: backupData.shopify.productsError || null,
          themesError: backupData.shopify.themesError || null,
          scriptTagsError: backupData.shopify.scriptTagsError || null,
          metaobjectsError: backupData.shopify.metaobjectsError || null,
          blogsError: backupData.shopify.blogsError || null,
          backedUpAt: backupData.shopify.backedUpAt || null
        } : null
      }
    });

  } catch (error) {
    console.error('Error creating backup:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Helper function to format bytes
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

// Get all backups
app.get('/api/backup/list', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffIdentifier, 'backupView')) {
      return res.status(403).json({ error: 'Permission denied. You need "backupView" permission.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const backups = await db.collection('backups')
      .find({})
      .sort({ createdAt: -1 })
      .toArray();

    // Format backups (exclude full data for list view)
    const formattedBackups = backups.map(backup => ({
      _id: backup._id.toString(),
      type: backup.type,
      createdAt: backup.createdAt,
      createdBy: backup.createdBy,
      size: backup.size || 0,
      sizeFormatted: backup.sizeFormatted || formatBytes(backup.size || 0),
      collections: backup.collections.map(col => ({
        name: col.name,
        count: col.count
      })),
      shopify: backup.shopify && Object.keys(backup.shopify).length > 0 ? {
        productsCount: backup.shopify.productsCount || 0,
        themesCount: backup.shopify.themesCount || 0,
        scriptTagsCount: backup.shopify.scriptTagsCount || 0,
        metaobjectsCount: backup.shopify.metaobjectsCount || 0,
        blogsCount: backup.shopify.blogsCount || 0,
        hasErrors: !!(backup.shopify.productsError || backup.shopify.themesError || backup.shopify.scriptTagsError || backup.shopify.metaobjectsError || backup.shopify.blogsError),
        productsError: backup.shopify.productsError || null,
        themesError: backup.shopify.themesError || null,
        scriptTagsError: backup.shopify.scriptTagsError || null,
        metaobjectsError: backup.shopify.metaobjectsError || null,
        blogsError: backup.shopify.blogsError || null,
        backedUpAt: backup.shopify.backedUpAt || null
      } : null
    }));

    res.json({
      success: true,
      backups: formattedBackups,
      count: formattedBackups.length
    });

  } catch (error) {
    console.error('Error fetching backups:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get backup details
app.get('/api/backup/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffIdentifier, 'backupView')) {
      return res.status(403).json({ error: 'Permission denied. You need "backupView" permission.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id } = req.params;
    
    // Validate ObjectId format
    if (!id || id.length !== 24 || !/^[0-9a-fA-F]{24}$/.test(id)) {
      return res.status(400).json({ error: 'Invalid backup ID format' });
    }
    
    const backup = await db.collection('backups').findOne({ _id: new ObjectId(id) });

    if (!backup) {
      return res.status(404).json({ error: 'Backup not found' });
    }

    res.json({
      success: true,
      backup: backup
    });

  } catch (error) {
    console.error('Error fetching backup:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Restore from backup
app.post('/api/backup/restore', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffIdentifier, 'backupRestore')) {
      return res.status(403).json({ error: 'Permission denied. You need "backupRestore" permission.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { backupId, collections = [] } = req.body;

    if (!backupId) {
      return res.status(400).json({ error: 'Backup ID is required' });
    }

    // Validate ObjectId format
    if (backupId.length !== 24 || !/^[0-9a-fA-F]{24}$/.test(backupId)) {
      return res.status(400).json({ error: 'Invalid backup ID format' });
    }

    // Get backup
    const backup = await db.collection('backups').findOne({ _id: new ObjectId(backupId) });

    if (!backup) {
      return res.status(404).json({ error: 'Backup not found' });
    }

    // If specific collections requested, restore only those
    const collectionsToRestore = collections.length > 0 
      ? backup.collections.filter(col => collections.includes(col.name))
      : backup.collections;

    const { restoreShopify = false } = req.body;

    // Restore each collection
    const restoredCollections = [];
    for (const collectionData of collectionsToRestore) {
      const collectionName = collectionData.name;
      
      // Clear existing data (optional - could merge instead)
      await db.collection(collectionName).deleteMany({});
      
      // Insert backup data
      if (collectionData.data && collectionData.data.length > 0) {
        await db.collection(collectionName).insertMany(collectionData.data);
      }
      
      restoredCollections.push({
        name: collectionName,
        count: collectionData.count
      });
    }

    // Restore Shopify data if requested and available
    const shopifyRestoreResult = {
      restored: false,
      message: 'Shopify restore skipped',
      details: {}
    };

    if (restoreShopify && backup.shopify && Object.keys(backup.shopify).length > 0) {
      if (!SHOPIFY_SHOP || !SHOPIFY_ACCESS_TOKEN) {
        shopifyRestoreResult.message = 'Shopify credentials not configured';
      } else {
        try {
          console.log('🔄 Starting Shopify data restore...');
          // Note: Full Shopify restore requires write permissions and is complex
          // For now, we'll log that Shopify data exists but restoration requires manual process
          // or separate endpoint with proper write scopes
          shopifyRestoreResult.restored = false;
          shopifyRestoreResult.message = 'Shopify restore requires write permissions. Please restore manually or use separate restore endpoint.';
          shopifyRestoreResult.details = {
            productsCount: backup.shopify.productsCount || 0,
            themesCount: backup.shopify.themesCount || 0,
            scriptTagsCount: backup.shopify.scriptTagsCount || 0,
            metaobjectsCount: backup.shopify.metaobjectsCount || 0,
            blogsCount: backup.shopify.blogsCount || 0
          };
        } catch (error) {
          console.error('Error during Shopify restore:', error);
          shopifyRestoreResult.message = `Shopify restore error: ${error.message}`;
        }
      }
    }

    // Log audit
    await logAudit({
      action: 'restore_backup',
      resourceType: 'backup',
      resourceId: backupId,
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'restored',
        old: null,
        new: 'completed',
        description: `Restored backup from ${new Date(backup.createdAt).toLocaleString()}. Restored ${restoredCollections.length} MongoDB collections. ${shopifyRestoreResult.restored ? 'Shopify data restored.' : 'Shopify data not restored (requires write permissions).'}`
      }],
      metadata: {
        backupType: backup.type,
        collections: restoredCollections.map(col => col.name),
        shopifyRestored: shopifyRestoreResult.restored
      }
    });

    res.json({
      success: true,
      message: 'Backup restored successfully',
      restoredCollections: restoredCollections,
      shopify: shopifyRestoreResult
    });

  } catch (error) {
    console.error('Error restoring backup:', error);
    res.status(500).json({ error: 'Internal server error', message: error.message });
  }
});

// Delete backup
app.delete('/api/backup/:id', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffIdentifier, 'backupDelete')) {
      return res.status(403).json({ error: 'Permission denied. You need "backupDelete" permission.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { id } = req.params;
    
    // Validate ObjectId format
    if (!id || id.length !== 24 || !/^[0-9a-fA-F]{24}$/.test(id)) {
      return res.status(400).json({ error: 'Invalid backup ID format' });
    }
    
    const backup = await db.collection('backups').findOne({ _id: new ObjectId(id) });

    if (!backup) {
      return res.status(404).json({ error: 'Backup not found' });
    }

    await db.collection('backups').deleteOne({ _id: new ObjectId(id) });

    // Log audit
    await logAudit({
      action: 'delete_backup',
      resourceType: 'backup',
      resourceId: id,
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'status',
        old: 'active',
        new: 'deleted',
        description: `Deleted backup from ${new Date(backup.createdAt).toLocaleString()} (${backup.type})`
      }]
    });

    res.json({
      success: true,
      message: 'Backup deleted successfully'
    });

  } catch (error) {
    console.error('Error deleting backup:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get backup configuration
app.get('/api/backup/config', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    
    // Check permission with error handling
    try {
      const hasAccess = await hasPermission(staffIdentifier, 'backupConfig');
      if (!hasAccess) {
        return res.status(403).json({ error: 'Permission denied. You need "backupConfig" permission.' });
      }
    } catch (permError) {
      console.error('Error checking permission:', permError);
      // If permission check fails, allow if staffIdentifier is provided (graceful degradation)
      if (!staffIdentifier) {
        return res.status(403).json({ error: 'Permission denied. Staff identifier required.' });
      }
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    // Get backup configuration
    let config;
    try {
      config = await db.collection('backup_config').findOne({ _id: 'main' });
    } catch (dbError) {
      console.error('Error querying backup_config:', dbError);
      // Return default config if query fails
      config = null;
    }
    
    if (!config) {
      // Default configuration
      config = {
        _id: 'main',
        fullBackupFrequency: 'weekly', // weekly, daily
        incrementalBackupFrequency: 'hourly', // hourly, every2hours, every4hours, daily
        autoBackupEnabled: true,
        retentionDays: 30
      };
      
      // Try to save default config, but don't fail if it errors
      try {
        await db.collection('backup_config').insertOne(config);
      } catch (insertError) {
        console.error('Error saving default backup config:', insertError);
        // Continue anyway - return the default config
      }
    }

    res.json({
      success: true,
      config: config
    });

  } catch (error) {
    console.error('Error fetching backup config:', error);
    console.error('Error stack:', error.stack);
    res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
});

// Update backup configuration
app.put('/api/backup/config', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'];
    if (!await hasPermission(staffIdentifier, 'backupConfig')) {
      return res.status(403).json({ error: 'Permission denied. You need "backupConfig" permission.' });
    }

    await ensureMongoConnection();
    if (!db) {
      return res.status(500).json({ error: 'Database connection failed' });
    }

    const { fullBackupFrequency, incrementalBackupFrequency, autoBackupEnabled, retentionDays } = req.body;

    const config = {
      _id: 'main',
      fullBackupFrequency: fullBackupFrequency || 'weekly',
      incrementalBackupFrequency: incrementalBackupFrequency || 'hourly',
      autoBackupEnabled: autoBackupEnabled !== undefined ? autoBackupEnabled : true,
      retentionDays: retentionDays || 30,
      updatedAt: new Date().toISOString(),
      updatedBy: staffIdentifier
    };

    await db.collection('backup_config').replaceOne({ _id: 'main' }, config, { upsert: true });

    // Restart scheduler if auto backup is enabled
    if (autoBackupEnabled) {
      startBackupScheduler();
    } else {
      stopBackupScheduler();
    }

    // Log audit
    await logAudit({
      action: 'update_backup_config',
      resourceType: 'backup_config',
      resourceId: 'main',
      staffIdentifier: staffIdentifier,
      changes: [{
        field: 'config',
        old: null,
        new: JSON.stringify(config),
        description: `Updated backup configuration: Full=${fullBackupFrequency}, Incremental=${incrementalBackupFrequency}, Auto=${autoBackupEnabled}`
      }]
    });

    res.json({
      success: true,
      config: config
    });

  } catch (error) {
    console.error('Error updating backup config:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Auto backup scheduler functions
async function performAutoBackup(type = 'incremental') {
  try {
    // Ensure connection with retry for cold starts
    let connectionAttempts = 0;
    const maxConnectionAttempts = 3;
    while (connectionAttempts < maxConnectionAttempts) {
      await ensureMongoConnection();
      if (db) break;
      connectionAttempts++;
      if (connectionAttempts < maxConnectionAttempts) {
        const delay = Math.pow(2, connectionAttempts) * 1000;
        console.log(`⏳ Retrying MongoDB connection (attempt ${connectionAttempts + 1}/${maxConnectionAttempts})...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    if (!db) {
      console.error('❌ Cannot perform auto backup: Database not connected after retries');
      throw new Error('Database connection failed');
    }

    // Get backup config
    const config = await db.collection('backup_config').findOne({ _id: 'main' });
    if (!config || !config.autoBackupEnabled) {
      console.log('⏸️ Auto backup is disabled');
      return;
    }

    // Use system identifier for auto backups
    const systemIdentifier = 'system@auto-backup';

    // Determine backup type
    let backupType = type;
    if (type === 'incremental') {
      const lastFullBackup = await db.collection('backups').findOne(
        { type: 'full' },
        { sort: { createdAt: -1 } }
      );
      
      if (!lastFullBackup) {
        backupType = 'full'; // First backup must be full
      } else {
        const daysSinceFullBackup = (Date.now() - new Date(lastFullBackup.createdAt).getTime()) / (1000 * 60 * 60 * 24);
        const fullBackupInterval = config.fullBackupFrequency === 'daily' ? 1 : 7;
        if (daysSinceFullBackup >= fullBackupInterval) {
          backupType = 'full';
        }
      }
    }

    console.log(`🔄 Starting auto ${backupType} backup...`);

    const collectionsToBackup = ['products', 'submissions', 'staff_members', 'audit_logs'];
    const backupData = {
      type: backupType,
      createdAt: new Date().toISOString(),
      createdBy: systemIdentifier,
      collections: [],
      shopify: {}
    };

    // Backup MongoDB collections
    let totalDocuments = 0;
    for (const collectionName of collectionsToBackup) {
      let documents;
      
      if (backupType === 'full') {
        documents = await db.collection(collectionName).find({}).toArray();
      } else {
        // For incremental backups, compare against the LAST backup (incremental or full)
        const lastBackup = await db.collection('backups').findOne(
          { 
            'collections.name': collectionName
          },
          { sort: { createdAt: -1 } }
        );
        
        const lastBackupTime = lastBackup ? new Date(lastBackup.createdAt) : null;
        documents = await getChangedDocuments(collectionName, lastBackupTime);
      }

      totalDocuments += documents.length;
      backupData.collections.push({
        name: collectionName,
        count: documents.length,
        data: documents
      });
    }

    // Skip backup if no documents changed (for incremental) or no documents exist (for full)
    if (backupType === 'incremental' && totalDocuments === 0) {
      console.log('⏭️ Skipping incremental backup - no changes detected');
      throw new Error('Skipping incremental backup - no changes detected');
    }

    // Backup Shopify data (only on full backups)
    if (backupType === 'full') {
      try {
        const [productsData, themesData, scriptTagsData, metaobjectsData, contentData] = await Promise.all([
          fetchShopifyProducts(),
          fetchShopifyThemes(),
          fetchShopifyScriptTags(),
          fetchShopifyMetaobjects(),
          fetchShopifyContent()
        ]);

        backupData.shopify = {
          products: productsData.products || [],
          productsCount: productsData.count || 0,
          productsError: productsData.error || null,
          themes: themesData.themes || [],
          themesCount: themesData.count || 0,
          themesError: themesData.error || null,
          scriptTags: scriptTagsData.scriptTags || [],
          scriptTagsCount: scriptTagsData.count || 0,
          scriptTagsError: scriptTagsData.error || null,
          metaobjects: metaobjectsData.metaobjects || [],
          metaobjectsCount: metaobjectsData.count || 0,
          metaobjectsError: metaobjectsData.error || null,
          blogs: contentData.blogs || [],
          blogsCount: contentData.count || 0,
          blogsError: contentData.error || null,
          backedUpAt: new Date().toISOString()
        };
      } catch (error) {
        console.error('❌ Error during Shopify backup:', error);
        backupData.shopify = {
          error: error.message,
          backedUpAt: new Date().toISOString()
        };
      }
    }

    // Calculate total size
    const totalSize = JSON.stringify(backupData).length;
    backupData.size = totalSize;
    backupData.sizeFormatted = formatBytes(totalSize);

    // Save backup
    await db.collection('backups').insertOne(backupData);

    const mongoCount = backupData.collections.reduce((sum, col) => sum + col.count, 0);
    const shopifyCount = backupData.shopify && Object.keys(backupData.shopify).length > 0
      ? (backupData.shopify.productsCount || 0) +
        (backupData.shopify.themesCount || 0) +
        (backupData.shopify.scriptTagsCount || 0) +
        (backupData.shopify.metaobjectsCount || 0) +
        (backupData.shopify.blogsCount || 0)
      : 0;

    console.log(`✅ Auto ${backupType} backup completed: ${mongoCount} MongoDB records, ${shopifyCount} Shopify items`);

    // Cleanup old backups based on retention policy
    if (config.retentionDays) {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - config.retentionDays);
      
      const deleteResult = await db.collection('backups').deleteMany({
        createdAt: { $lt: cutoffDate.toISOString() }
      });
      
      if (deleteResult.deletedCount > 0) {
        console.log(`🗑️ Cleaned up ${deleteResult.deletedCount} old backups (older than ${config.retentionDays} days)`);
      }
    }

  } catch (error) {
    console.error('❌ Error performing auto backup:', error);
  }
}

function getIntervalMs(frequency) {
  const intervals = {
    'hourly': 60 * 60 * 1000,
    'every2hours': 2 * 60 * 60 * 1000,
    'every4hours': 4 * 60 * 60 * 1000,
    'daily': 24 * 60 * 60 * 1000,
    'weekly': 7 * 24 * 60 * 60 * 1000
  };
  return intervals[frequency] || intervals.hourly;
}

async function startBackupScheduler() {
  // Stop existing scheduler if running
  stopBackupScheduler();

  try {
    await ensureMongoConnection();
    if (!db) {
      console.error('❌ Cannot start backup scheduler: Database not connected');
      return;
    }

    const config = await db.collection('backup_config').findOne({ _id: 'main' });
    if (!config || !config.autoBackupEnabled) {
      console.log('⏸️ Auto backup is disabled, scheduler not started');
      return;
    }

    const intervalMs = getIntervalMs(config.incrementalBackupFrequency);
    console.log(`🔄 Starting backup scheduler: ${config.incrementalBackupFrequency} (${intervalMs / 1000 / 60} minutes)`);

    // Perform initial backup check
    performAutoBackup('incremental');

    // Schedule periodic backups
    backupScheduler = setInterval(async () => {
      await performAutoBackup('incremental');
    }, intervalMs);

    lastBackupCheck = new Date();
  } catch (error) {
    console.error('❌ Error starting backup scheduler:', error);
  }
}

function stopBackupScheduler() {
  if (backupScheduler) {
    clearInterval(backupScheduler);
    backupScheduler = null;
    console.log('⏹️ Backup scheduler stopped');
  }
}

// Initialize backup scheduler on server start
async function initializeBackupScheduler() {
  try {
    await ensureMongoConnection();
    if (!db) {
      console.log('⏳ Waiting for database connection before starting backup scheduler...');
      // Retry after a delay
      setTimeout(initializeBackupScheduler, 5000);
      return;
    }

    const config = await db.collection('backup_config').findOne({ _id: 'main' });
    if (config && config.autoBackupEnabled) {
      startBackupScheduler();
    } else {
      console.log('⏸️ Auto backup is disabled in configuration');
    }
  } catch (error) {
    console.error('❌ Error initializing backup scheduler:', error);
  }
}

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Auto backup endpoint (for Vercel Cron Jobs - accepts both GET and POST)
const handleAutoBackup = async (req, res) => {
  let lockAcquired = false;
  try {
    // Vercel Cron Jobs send requests with x-vercel-cron header (value can vary)
    // Manual triggers can use POST with API key
    const authHeader = req.headers['x-api-key'];
    const vercelCronHeader = req.headers['x-vercel-cron'];
    const isVercelCron = !!vercelCronHeader; // Just check if header exists
    
    console.log('🕐 Auto backup triggered:', {
      method: req.method,
      isVercelCron: isVercelCron,
      vercelCronHeader: vercelCronHeader,
      hasApiKey: !!authHeader,
      timestamp: new Date().toISOString()
    });
    
    if (!isVercelCron && authHeader !== API_SECRET) {
      console.log('❌ Unauthorized backup attempt - missing Vercel cron header or API key');
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Retry MongoDB connection with exponential backoff for cold starts
    let connectionAttempts = 0;
    const maxConnectionAttempts = 3;
    while (connectionAttempts < maxConnectionAttempts) {
      await ensureMongoConnection();
      if (db) break;
      connectionAttempts++;
      if (connectionAttempts < maxConnectionAttempts) {
        const delay = Math.pow(2, connectionAttempts) * 1000; // 2s, 4s, 8s
        console.log(`⏳ Retrying MongoDB connection (attempt ${connectionAttempts + 1}/${maxConnectionAttempts}) in ${delay}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    if (!db) {
      console.error('❌ Failed to connect to MongoDB after retries');
      return res.status(500).json({ error: 'Database connection failed after retries' });
    }

    // Check if enough time has passed since last backup BEFORE acquiring lock
    // This prevents unnecessary lock acquisition
    const lastBackupCheck = await db.collection('backups').findOne(
      {},
      { sort: { createdAt: -1 } }
    );

    if (lastBackupCheck) {
      const config = await db.collection('backup_config').findOne({ _id: 'main' });
      if (config && config.autoBackupEnabled) {
        const timeSinceLastBackup = Date.now() - new Date(lastBackupCheck.createdAt).getTime();
        const incrementalInterval = getIntervalMs(config.incrementalBackupFrequency);
        
        // If last backup was recent (less than required interval), skip early
        if (timeSinceLastBackup < incrementalInterval) {
          const minutesRemaining = Math.ceil((incrementalInterval - timeSinceLastBackup) / 1000 / 60);
          console.log(`⏭️ Skipping backup - only ${Math.ceil(timeSinceLastBackup / 1000 / 60)} minutes since last backup, need ${Math.ceil(incrementalInterval / 1000 / 60)} minutes`);
          return res.json({ 
            success: true, 
            message: `Skipping backup - next backup in ${minutesRemaining} minutes`,
            skipped: true
          });
        }
      }
    }

    // Acquire backup lock atomically to prevent concurrent backups
    const lockKey = 'backup_in_progress';
    const lockExpiry = new Date(Date.now() + 30 * 60 * 1000); // 30 minutes expiry
    const now = new Date();
    
    try {
      // First, check if lock exists and is still valid
      const existingLock = await db.collection('backup_locks').findOne({ _id: lockKey });
      if (existingLock && new Date(existingLock.expiresAt) > now) {
        const minutesRemaining = Math.ceil((new Date(existingLock.expiresAt) - now.getTime()) / 1000 / 60);
        console.log(`⏭️ Backup already in progress. Lock expires in ${minutesRemaining} minutes`);
        return res.json({ 
          success: true, 
          message: `Backup already in progress. Lock expires in ${minutesRemaining} minutes`,
          skipped: true
        });
      }

      // Try to acquire lock atomically - only succeeds if lock doesn't exist or has expired
      // Use updateOne with upsert to ensure atomicity
      const updateResult = await db.collection('backup_locks').updateOne(
        { 
          _id: lockKey,
          $or: [
            { expiresAt: { $exists: false } },
            { expiresAt: { $lt: now.toISOString() } }
          ]
        },
        { 
          $set: {
            _id: lockKey,
            expiresAt: lockExpiry.toISOString(),
            createdAt: new Date().toISOString()
          }
        },
        { upsert: true }
      );

      // If matchedCount is 0 and upsertedCount is 0, another process acquired the lock between our check and update
      if (updateResult.matchedCount === 0 && updateResult.upsertedCount === 0) {
        // Double-check if lock was acquired by another process
        const newLock = await db.collection('backup_locks').findOne({ _id: lockKey });
        if (newLock && new Date(newLock.expiresAt) > now) {
          const minutesRemaining = Math.ceil((new Date(newLock.expiresAt) - now.getTime()) / 1000 / 60);
          console.log(`⏭️ Backup lock acquired by another process. Lock expires in ${minutesRemaining} minutes`);
          return res.json({ 
            success: true, 
            message: `Backup already in progress. Lock expires in ${minutesRemaining} minutes`,
            skipped: true
          });
        }
      }
      
      lockAcquired = true;
      console.log('🔒 Backup lock acquired atomically');
    } catch (lockError) {
      console.error('Error acquiring backup lock:', lockError);
      // Check if lock exists (another process might have acquired it)
      const existingLock = await db.collection('backup_locks').findOne({ _id: lockKey });
      if (existingLock && new Date(existingLock.expiresAt) > new Date()) {
        const minutesRemaining = Math.ceil((new Date(existingLock.expiresAt) - Date.now()) / 1000 / 60);
        return res.json({ 
          success: true, 
          message: `Backup already in progress. Lock expires in ${minutesRemaining} minutes`,
          skipped: true
        });
      }
      return res.status(500).json({ error: 'Failed to acquire backup lock' });
    }

    // Get backup config
    const config = await db.collection('backup_config').findOne({ _id: 'main' });
    if (!config || !config.autoBackupEnabled) {
      // Release lock
      await db.collection('backup_locks').deleteOne({ _id: lockKey });
      return res.json({ success: true, message: 'Auto backup is disabled, skipping' });
    }

    // Check if enough time has passed since last backup based on backup type
    const lastBackup = await db.collection('backups').findOne(
      {},
      { sort: { createdAt: -1 } }
    );

    if (lastBackup) {
      const timeSinceLastBackup = Date.now() - new Date(lastBackup.createdAt).getTime();
      
      // Determine which frequency to check based on last backup type
      let requiredInterval;
      let backupType = 'incremental';
      
      if (lastBackup.type === 'full') {
        // If last backup was full, check if we need another full backup
        const fullBackupInterval = getIntervalMs(config.fullBackupFrequency === 'daily' ? 'daily' : 'weekly');
        const incrementalInterval = getIntervalMs(config.incrementalBackupFrequency);
        
        if (timeSinceLastBackup >= fullBackupInterval) {
          // Time for a full backup
          backupType = 'full';
          requiredInterval = fullBackupInterval;
        } else if (timeSinceLastBackup >= incrementalInterval) {
          // Can do incremental backup
          backupType = 'incremental';
          requiredInterval = incrementalInterval;
        } else {
          // Not enough time for any backup
          requiredInterval = incrementalInterval;
        }
      } else {
        // Last backup was incremental, check incremental frequency
        requiredInterval = getIntervalMs(config.incrementalBackupFrequency);
        backupType = 'incremental';
        
        // But also check if we need a full backup
        const lastFullBackup = await db.collection('backups').findOne(
          { type: 'full' },
          { sort: { createdAt: -1 } }
        );
        
        if (lastFullBackup) {
          const daysSinceFullBackup = (Date.now() - new Date(lastFullBackup.createdAt).getTime()) / (1000 * 60 * 60 * 24);
          const fullBackupInterval = config.fullBackupFrequency === 'daily' ? 1 : 7;
          if (daysSinceFullBackup >= fullBackupInterval) {
            backupType = 'full';
            requiredInterval = getIntervalMs(config.fullBackupFrequency === 'daily' ? 'daily' : 'weekly');
          }
        } else {
          // No full backup exists, do a full backup
          backupType = 'full';
          requiredInterval = 0; // No wait needed
        }
      }
      
      if (timeSinceLastBackup < requiredInterval) {
        const minutesRemaining = Math.ceil((requiredInterval - timeSinceLastBackup) / 1000 / 60);
        // Release lock
        await db.collection('backup_locks').deleteOne({ _id: lockKey });
        return res.json({ 
          success: true, 
          message: `Skipping backup - next ${backupType} backup in ${minutesRemaining} minutes`,
          skipped: true
        });
      }
    }

    // Perform backup
    console.log(`🔄 Starting auto ${backupType} backup...`);
    await performAutoBackup(backupType);
    
    // Release lock
    await db.collection('backup_locks').deleteOne({ _id: lockKey });
    lockAcquired = false;
    
    res.json({ success: true, message: `Auto ${backupType} backup completed` });
  } catch (error) {
    console.error('❌ Error triggering auto backup:', error);
    console.error('Error stack:', error.stack);
    
    // Release lock if we acquired it
    if (lockAcquired && db) {
      try {
        await db.collection('backup_locks').deleteOne({ _id: 'backup_in_progress' });
      } catch (lockError) {
        console.error('Error releasing backup lock:', lockError);
      }
    }
    
    res.status(500).json({ 
      error: 'Internal server error',
      message: error.message 
    });
  }
};

// Accept both GET (for Vercel Cron) and POST (for manual triggers)
app.get('/api/backup/auto', handleAutoBackup);
app.post('/api/backup/auto', handleAutoBackup);

// Root endpoint (for testing)
app.get('/', (req, res) => {
  res.json({ 
    status: 'ok', 
    message: 'Wishlist API Server',
    endpoints: {
      health: '/health',
      wishlist: '/api/wishlist (POST)'
    }
  });
});

// Export for Vercel serverless functions
module.exports = app;

// for local development
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, async () => {
    console.log(`Server running on port ${PORT}`);
    // Initialize backup scheduler after server starts
    setTimeout(initializeBackupScheduler, 2000);
  });
} else {
  // For serverless (Vercel), initialize scheduler after a delay
  setTimeout(initializeBackupScheduler, 3000);
}
