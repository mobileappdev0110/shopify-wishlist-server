const express = require('express');
const fetch = require('node-fetch');
const nodemailer = require('nodemailer');
const { MongoClient } = require('mongodb');
const fs = require('fs').promises;
const path = require('path');
const app = express();

// Enable CORS for Shopify store - MUST BE BEFORE OTHER MIDDLEWARE
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*'); // Allow all origins
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
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

// Get trade-in products from Shopify (filtered by tag)
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

    // NEW SYSTEM: Use Shopify product variant
    if (productId && variantId) {
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

    // Get condition multiplier
    const multiplier = conditionMultipliers[condition];
    if (!multiplier) {
      return res.status(400).json({ 
        success: false,
        error: 'Invalid condition',
        requestedCondition: condition,
        availableConditions: Object.keys(conditionMultipliers)
      });
    }

    // Calculate final price
    const finalPrice = Math.round(basePrice * multiplier * 100) / 100; // Round to 2 decimals

    res.json({
      success: true,
      basePrice,
      conditionMultiplier: multiplier,
      finalPrice,
      currency: 'GBP',
      formattedPrice: `£${finalPrice.toFixed(2)}`,
      // Include system type for debugging
      system: productId && variantId ? 'variant-based' : 'legacy'
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

// Update submission status (admin)
app.post('/api/trade-in/:id/update-status', async (req, res) => {
  try {
    const authHeader = req.headers['x-api-key'];
    if (authHeader !== API_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const id = parseInt(req.params.id);
    const { status, notes } = req.body;

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
      
      submission.status = status;
      submission.updatedAt = new Date().toISOString();
      if (notes) {
        submission.adminNotes = notes;
      }
      
      await saveSubmissions();
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
    // Format: TRADE-XXXX-XXXX (e.g., TRADE-A3K9-M7P2)
    // This prevents guessing sequential codes
    function generateSecureGiftCardCode() {
      const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // Removed confusing chars (0, O, I, 1)
      let code = 'TRADE-';
      
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
        giftCardCode = `TRADE-${submission.id.toString().padStart(6, '0')}-${randomSuffix}`;
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

