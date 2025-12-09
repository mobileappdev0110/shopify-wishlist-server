const express = require('express');
const fetch = require('node-fetch');
const nodemailer = require('nodemailer');
const { MongoClient, ObjectId } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const XLSX = require('xlsx');
const app = express();

// Enable CORS for Shopify store - MUST BE BEFORE OTHER MIDDLEWARE
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*'); // Allow all origins
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, X-API-Key');
  
  // Handle preflight requests
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  
  next();
});

app.use(express.json());

// Your Shopify credentials (from environment variables)
const SHOPIFY_SHOP = process.env.SHOPIFY_SHOP;
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API_SECRET = process.env.API_SECRET;

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
    return true;
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

// Wishlist save endpoint
app.post('/api/wishlist', async (req, res) => {
  try {
    // Simple authentication check
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { customer_id, wishlist } = req.body;

    if (!customer_id || !wishlist) {
      return res.status(400).json({ error: 'Missing customer_id or wishlist' });
    }

    // Prepare GraphQL mutation to save to metafield
    const metafieldValue = JSON.stringify(wishlist);
    
    const mutation = `
      mutation {
        metafieldsSet(metafields: [{
          ownerId: "gid://shopify/Customer/${customer_id}"
          namespace: "custom"
          key: "wishlist"
          type: "json"
          value: ${JSON.stringify(metafieldValue)}
        }]) {
          metafields {
            id
            key
            value
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    // Call Shopify Admin API
    const response = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
      },
      body: JSON.stringify({ query: mutation })
    });

    const data = await response.json();

    if (data.errors || (data.data?.metafieldsSet?.userErrors?.length > 0)) {
      console.error('GraphQL errors:', data.errors || data.data?.metafieldsSet?.userErrors);
      return res.status(500).json({ 
        error: 'Failed to save wishlist',
        details: data.errors || data.data?.metafieldsSet?.userErrors
      });
    }

    res.json({ 
      success: true, 
      message: 'Wishlist saved successfully' 
    });

  } catch (error) {
    console.error('Error saving wishlist:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get wishlist endpoint (for loading wishlist)
app.get('/api/wishlist/get', async (req, res) => {
  try {
    // Simple authentication check
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const customerId = req.query.customer_id;
    if (!customerId) {
      return res.status(400).json({ error: 'Missing customer_id' });
    }

    // Query Shopify Admin API to get customer metafield
    const query = `
      query getCustomerMetafield($id: ID!) {
        customer(id: $id) {
          id
          metafields(first: 1, namespace: "custom", keys: ["wishlist"]) {
            edges {
              node {
                id
                key
                value
              }
            }
          }
        }
      }
    `;

    const response = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
      },
      body: JSON.stringify({
        query: query,
        variables: { id: `gid://shopify/Customer/${customerId}` }
      })
    });

    const data = await response.json();

    if (data.errors) {
      console.error('GraphQL errors:', data.errors);
      // Don't return 500 - return empty wishlist instead
      return res.json({ 
        success: true,
        wishlist: [],
        warning: 'Could not fetch from metafield'
      });
    }

    // Check if customer exists
    if (!data.data?.customer) {
      console.warn('Customer not found:', customerId);
      return res.json({ success: true, wishlist: [] });
    }

    const metafield = data.data?.customer?.metafields?.edges?.[0]?.node;
    if (metafield && metafield.value) {
      try {
        const wishlist = JSON.parse(metafield.value);
        res.json({ 
          success: true, 
          wishlist: Array.isArray(wishlist) ? wishlist : []
        });
      } catch (e) {
        console.error('Error parsing metafield value:', e);
        res.json({ success: true, wishlist: [] });
      }
    } else {
      // Metafield doesn't exist yet - return empty array (not an error)
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

    // Fetch products from MongoDB
    const products = await db.collection('trade_in_products').find(query).toArray();
    
    console.log(`📦 Fetched ${products.length} products from MongoDB for deviceType: ${deviceType || 'all'}`);
    if (products.length > 0) {
      console.log(`✅ Sample product:`, {
        brand: products[0].brand,
        model: products[0].model,
        storage: products[0].storage,
        deviceType: products[0].deviceType,
        hasImage: !!products[0].imageUrl,
        hasPrices: !!products[0].prices
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
          variants: []
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

      // Add variant (storage + color combination)
      // Use full gid format for both id and gid to ensure consistency
      const variantGid = `gid://database/Variant/${product._id}_${product.storage}_${product.color || 'default'}`;
      const variant = {
        id: variantGid, // Use full gid format for id as well
        gid: variantGid,
        title: `${product.storage}${product.color ? ` - ${product.color}` : ''}`,
        price: product.prices?.Excellent || product.basePrice || 0, // Use Excellent as base or fallback
        availableForSale: true,
        image: {
          url: product.imageUrl || getDefaultImageUrl(product.deviceType),
          altText: `${product.brand} ${product.model} ${product.storage}`
        },
        options: {
          storage: product.storage,
          color: product.color || 'Default'
        },
        // Store full product data for pricing calculation
        _productData: product
      };

      acc[key].variants.push(variant);

      return acc;
    }, {});

    const productArray = Object.values(transformedProducts);
    
    console.log(`✅ Transformed to ${productArray.length} grouped products (by brand/model)`);
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

    const products = await db.collection('trade_in_products').find({}).sort({ brand: 1, model: 1, storage: 1 }).toArray();
    
    // Log sample product to verify imageUrl is present
    if (products.length > 0) {
      console.log(`📦 Admin: Fetched ${products.length} products`);
      console.log(`🖼️ Sample product imageUrl:`, {
        brand: products[0].brand,
        model: products[0].model,
        storage: products[0].storage,
        imageUrl: products[0].imageUrl || 'NULL/EMPTY',
        hasImageUrl: !!products[0].imageUrl
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

    const { id, brand, model, storage, color, deviceType, imageUrl, prices } = req.body;

    if (!brand || !model || !storage || !deviceType) {
      return res.status(400).json({ error: 'Missing required fields: brand, model, storage, deviceType' });
    }

    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';
    
    const productData = {
      brand: brand.trim(),
      model: model.trim(),
      storage: storage.trim(),
      color: color ? color.trim() : null,
      deviceType: deviceType.toLowerCase(),
      imageUrl: imageUrl || null,
      prices: prices || {}, // { Excellent: 500, Good: 400, Fair: 300, Faulty: null }
      updatedAt: new Date().toISOString(),
      lastEditedBy: staffIdentifier
    };

    if (id) {
      // Update existing - get old data for audit
      const oldProduct = await db.collection('trade_in_products').findOne({ _id: new ObjectId(id) });
      
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
      // Create new
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
    const { brand, model, storage, color, deviceType, imageUrl, prices } = req.body;
    const staffIdentifier = req.headers['x-staff-identifier'] || req.body.staffIdentifier || 'Unknown';

    if (!brand || !model || !storage || !deviceType) {
      return res.status(400).json({ error: 'Missing required fields: brand, model, storage, deviceType' });
    }

    // Get old product for audit
    const oldProduct = await db.collection('trade_in_products').findOne({ _id: new ObjectId(id) });
    if (!oldProduct) {
      return res.status(404).json({ error: 'Product not found' });
    }

    const productData = {
      brand: brand.trim(),
      model: model.trim(),
      storage: storage.trim(),
      color: color ? color.trim() : null,
      deviceType: deviceType.toLowerCase(),
      imageUrl: imageUrl || null,
      prices: prices || {},
      updatedAt: new Date().toISOString(),
      lastEditedBy: staffIdentifier
    };

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

    // Verify admin access
    const staffEmail = req.headers['x-staff-identifier'];
    if (!await verifyAdminAccess(staffEmail)) {
      return res.status(403).json({ error: 'Admin access required. Only admin and manager roles can view audit logs.' });
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

// Helper function to verify admin access
async function verifyAdminAccess(staffEmail) {
  if (!staffEmail || staffEmail === 'Unknown') {
    return false;
  }

  try {
    await ensureMongoConnection();
    if (!db) {
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

    const staff = await db.collection('staff_members').findOne({ 
      email: staffEmail.trim().toLowerCase(),
      active: true 
    });

    if (!staff) {
      return false;
    }

    // Only admin and manager roles can access admin pages
    return staff.role === 'admin' || staff.role === 'manager';
  } catch (error) {
    console.error('Error verifying admin access:', error);
    return false;
  }
}

// Verify staff admin access
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

    // Check if staff has admin role
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

// Get all staff members (admin only)
app.get('/api/staff', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Verify admin access
    const staffEmail = req.headers['x-staff-identifier'];
    if (!await verifyAdminAccess(staffEmail)) {
      return res.status(403).json({ error: 'Admin access required. Only admin and manager roles can access staff management.' });
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

    // If admins exist, verify admin access
    if (adminCount > 0) {
      if (!await verifyAdminAccess(adminIdentifier)) {
        return res.status(403).json({ error: 'Admin access required. Only admin and manager roles can manage staff.' });
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
    
    // Verify admin access
    if (!await verifyAdminAccess(adminIdentifier)) {
      return res.status(403).json({ error: 'Admin access required. Only admin and manager roles can manage staff.' });
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
    
    // Verify admin access
    if (!await verifyAdminAccess(adminIdentifier)) {
      return res.status(403).json({ error: 'Admin access required. Only admin and manager roles can manage staff.' });
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
    
    for (let col = range.s.c; col <= range.e.c; col++) {
      const colIdx = col - range.s.c;
      const mainHeader = mainHeaderRow[colIdx] || '';
      const subHeader = subHeaderRow && subHeaderRow[colIdx] ? subHeaderRow[colIdx] : '';
      
      const mainUpper = mainHeader.toUpperCase().trim();
      let columnName;
      
      // Check if this is a new condition header
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
      } else if (currentCondition && subHeader) {
        // We're in a condition section and have a sub-header (storage size)
        // This is a storage column under the current condition
        columnName = subHeader;
        columnConditionByIndex[colIdx] = currentCondition;
      } else {
        // Regular column (DEVICE, BRAND, MODEL, image_url, etc.)
        columnName = mainHeader || subHeader || `__EMPTY_${colIdx}`;
        // Reset current condition when we hit a non-condition column
        if (mainUpper && mainUpper !== '' && !mainUpper.includes('EMPTY')) {
          currentCondition = null;
        }
      }
      
      columnMapping.push(columnName);
    }
    
    console.log('📋 Column mapping:', columnMapping);
    console.log('📋 Column condition by index:', columnConditionByIndex);
    
    // Read data starting from row after sub-header row
    const dataStartRow = subHeaderRowIndex !== -1 ? subHeaderRowIndex + 1 : mainHeaderRowIndex + 1;
    const allRows = [];
    for (let row = dataStartRow; row <= range.e.r; row++) {
      const rowData = {};
      for (let col = range.s.c; col <= range.e.c; col++) {
        const cellAddress = XLSX.utils.encode_cell({ r: row, c: col });
        const cell = worksheet[cellAddress];
        const value = cell ? (cell.v !== undefined ? String(cell.v).trim() : '') : '';
        const headerName = columnMapping[col - range.s.c] || `__EMPTY_${col}`;
        rowData[headerName] = value;
      }
      // Only add non-empty rows
      if (Object.values(rowData).some(v => v && v !== '')) {
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
          for (const colIdx of matchingIndices) {
            const condition = columnConditionByIndex[colIdx];
            const colName = columnMapping[colIdx];
            
            // Debug logging for first row, first storage
            if (storage === '64GB' && i === 0) {
              console.log(`🔍 Column ${colIdx} (${colName}): condition=${condition}, value="${row[colName]}"`);
            }
            
            if (condition && row[colName] !== undefined && row[colName] !== null && row[colName] !== '') {
              const val = parseFloat(row[colName]);
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

          const productData = {
            brand: brand.trim(),
            model: model.trim(),
            storage: storage,
            color: color ? color.trim() : null,
            deviceType: deviceType,
            imageUrl: imageUrl ? imageUrl.trim() : null, // Ensure imageUrl is trimmed
            prices: prices,
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
// TRADE-IN SUBMISSION ENDPOINTS
// ============================================

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
      paymentDetails
    } = req.body;

    if (!name || !email || !brand || !model || !condition) {
      return res.status(400).json({ 
        error: 'Missing required fields' 
      });
    }

    // Validate payment details based on payment method
    if (paymentMethod === 'bank_transfer') {
      if (!paymentDetails?.accountNumber || !paymentDetails?.sortCode || !paymentDetails?.accountName) {
        return res.status(400).json({ 
          error: 'Bank account details are required for bank transfer' 
        });
      }
    } else if (paymentMethod === 'paypal') {
      if (!paymentDetails?.paypalEmail) {
        return res.status(400).json({ 
          error: 'PayPal email is required for PayPal payment' 
        });
      }
    }

    // For custom devices, finalPrice can be 0
    const price = isCustomDevice ? 0 : (finalPrice || 0);
    
    // Default to store_credit if not specified
    const selectedPaymentMethod = paymentMethod || 'store_credit';

    // Create submission
    const submission = {
      id: submissionIdCounter++,
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
      pageUrl: pageUrl || '',
      status: 'pending', // pending, accepted, rejected, completed
      paymentStatus: 'pending', // pending, processing, completed, failed
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      giftCardCode: null,
      giftCardId: null,
      paymentReference: null,
      paymentDate: null
    };

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
      await transporter.sendMail({
        from: SMTP_FROM,
        to: email,
        subject: 'Trade-In Request Received',
        html: `
          <h2>Thank you for your trade-in request!</h2>
          <p>Hello ${name},</p>
          <p>We've received your trade-in request for:</p>
          <ul>
            <li><strong>Device:</strong> ${brand} ${model} ${storage}</li>
            <li><strong>Condition:</strong> ${condition}</li>
            <li><strong>Estimated Value:</strong> £${parseFloat(finalPrice).toFixed(2)}</li>
          </ul>
          <p>Our team will review your request and get back to you shortly.</p>
          <p>Submission ID: #${submission.id}</p>
        `
      });
    } catch (emailError) {
      console.error('Error sending confirmation email:', emailError);
      // Don't fail the request if email fails
    }

    // Send notification email to admin
    try {
      await transporter.sendMail({
        from: SMTP_FROM,
        to: ADMIN_EMAIL,
        subject: `New Trade-In Request #${submission.id}`,
        html: `
          <h2>New Trade-In Request</h2>
          <p><strong>Customer:</strong> ${name} (${email})</p>
          <p><strong>Phone:</strong> ${phone || 'N/A'}</p>
          <p><strong>Device:</strong> ${brand} ${model} ${storage}</p>
          <p><strong>Condition:</strong> ${condition}</p>
          <p><strong>Estimated Value:</strong> £${parseFloat(finalPrice).toFixed(2)}</p>
          <p><strong>Notes:</strong> ${notes || 'None'}</p>
          <p><strong>Submission ID:</strong> #${submission.id}</p>
        `
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
      
      if (!bankDetails?.accountNumber || !bankDetails?.sortCode || !bankDetails?.accountName) {
        return res.status(400).json({ 
          error: 'Bank account details not found in submission' 
        });
      }

      // TODO: Integrate with bank transfer API (e.g., Stripe Connect, Open Banking)
      // For now, this creates a payment record and sends email
      // You can integrate bank transfer APIs here for automated payments
      
      paymentResult = {
        method: 'bank_transfer',
        accountNumber: bankDetails.accountNumber,
        sortCode: bankDetails.sortCode,
        accountName: bankDetails.accountName,
        amount: submission.finalPrice,
        status: 'processing', // Will be 'completed' when transfer is confirmed
        reference: paymentReference
      };
      
      console.log(`Bank transfer initiated: ${bankDetails.accountName}, Amount: £${submission.finalPrice}, Reference: ${paymentReference}`);
      
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
            <p><strong>Account Name:</strong> ${submission.paymentDetails.accountName}</p>
            <p><strong>Account Number:</strong> ${submission.paymentDetails.accountNumber}</p>
            <p><strong>Sort Code:</strong> ${submission.paymentDetails.sortCode}</p>
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

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

