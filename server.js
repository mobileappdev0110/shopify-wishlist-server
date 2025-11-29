const express = require('express');
const fetch = require('node-fetch');
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

