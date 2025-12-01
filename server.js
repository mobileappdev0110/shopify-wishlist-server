const express = require('express');
const fetch = require('node-fetch');
const nodemailer = require('nodemailer');
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
  "Like New": 0.9,
  "Good": 0.8,
  "Fair": 0.6,
  "Faulty": 0.3
};

// Trade-in submissions storage (in-memory, should be persisted to database in production)
let tradeInSubmissions = [];
let submissionIdCounter = 1;

// Load pricing rules from file if it exists
async function loadPricingRules() {
  try {
    const data = await fs.readFile(path.join(__dirname, 'pricing-rules.json'), 'utf8');
    pricingRules = JSON.parse(data);
    console.log('Pricing rules loaded from file');
  } catch (error) {
    console.log('No pricing rules file found, using defaults');
  }
}

// Save pricing rules to file
async function savePricingRules() {
  try {
    await fs.writeFile(
      path.join(__dirname, 'pricing-rules.json'),
      JSON.stringify(pricingRules, null, 2),
      'utf8'
    );
    console.log('Pricing rules saved to file');
  } catch (error) {
    console.error('Error saving pricing rules:', error);
  }
}

// Initialize pricing rules
loadPricingRules();

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

// Calculate valuation
app.post('/api/pricing/calculate', async (req, res) => {
  try {
    const { brand, model, storage, condition } = req.body;

    // Log the request for debugging
    console.log('Pricing calculation request:', { brand, model, storage, condition });

    if (!brand || !model || !storage || !condition) {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required fields: brand, model, storage, condition' 
      });
    }

    // Get base price - try exact match first
    let basePrice = pricingRules[brand]?.[model]?.[storage]?.base;
    
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
      formattedPrice: `£${finalPrice.toFixed(2)}`
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

    const { pricingRules: newRules, conditionMultipliers: newMultipliers } = req.body;

    if (newRules) {
      pricingRules = newRules;
    }
    if (newMultipliers) {
      Object.assign(conditionMultipliers, newMultipliers);
    }

    await savePricingRules();

    res.json({
      success: true,
      message: 'Pricing rules updated successfully'
    });

  } catch (error) {
    console.error('Error updating pricing rules:', error);
    res.status(500).json({ error: 'Internal server error' });
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
      pageUrl
    } = req.body;

    if (!name || !email || !brand || !model || !storage || !condition || !finalPrice) {
      return res.status(400).json({ 
        error: 'Missing required fields' 
      });
    }

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
      storage,
      condition,
      finalPrice: parseFloat(finalPrice),
      deviceType: deviceType || 'phone',
      pageUrl: pageUrl || '',
      status: 'pending', // pending, accepted, rejected, completed
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      giftCardCode: null,
      giftCardId: null
    };

    tradeInSubmissions.push(submission);

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

    const { status, limit = 100, offset = 0 } = req.query;

    let submissions = [...tradeInSubmissions];

    // Filter by status if provided
    if (status) {
      submissions = submissions.filter(s => s.status === status);
    }

    // Sort by newest first
    submissions.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    // Pagination
    const paginated = submissions.slice(parseInt(offset), parseInt(offset) + parseInt(limit));

    res.json({
      success: true,
      submissions: paginated,
      total: submissions.length,
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

    const submission = tradeInSubmissions.find(s => s.id === id);
    if (!submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    const oldStatus = submission.status;
    submission.status = status;
    submission.updatedAt = new Date().toISOString();
    if (notes) {
      submission.adminNotes = notes;
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
    const submission = tradeInSubmissions.find(s => s.id === id);

    if (!submission) {
      return res.status(404).json({ error: 'Submission not found' });
    }

    if (submission.status !== 'accepted') {
      return res.status(400).json({ 
        error: 'Can only issue credit for accepted submissions' 
      });
    }

    if (submission.giftCardCode) {
      return res.status(400).json({ 
        error: 'Credit already issued for this submission' 
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

    // Create gift card via Shopify Admin API
    // Note: Shopify gift cards can be assigned to customers, but the API may require
    // using giftCardAssign mutation after creation, or customerId in the create mutation
    // For now, we'll create the gift card and it will be code-based
    // If customer exists, they can still use the code and it will be linked when used
    const giftCardCode = `TRADE${submission.id.toString().padStart(6, '0')}`;
    
    const mutation = `
      mutation {
        giftCardCreate(giftCard: {
          initialValue: ${submission.finalPrice}
          code: "${giftCardCode}"
        }) {
          giftCard {
            id
            code
            balance {
              amount
              currencyCode
            }
          }
          userErrors {
            field
            message
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
      body: JSON.stringify({ query: mutation })
    });

    const data = await response.json();
    
    // If customer was found, try to assign the gift card to them
    // This ensures it appears in their account
    if (customerId && data.data?.giftCardCreate?.giftCard) {
      try {
        const assignMutation = `
          mutation {
            giftCardAssign(giftCardId: "${data.data.giftCardCreate.giftCard.id}", customerId: "${customerId}") {
              giftCard {
                id
                customer {
                  id
                  email
                }
              }
              userErrors {
                field
                message
              }
            }
          }
        `;

        const assignResponse = await fetch(`https://${SHOPIFY_SHOP}/admin/api/2024-01/graphql.json`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
          },
          body: JSON.stringify({ query: assignMutation })
        });

        const assignData = await assignResponse.json();
        if (assignData.data?.giftCardAssign?.giftCard) {
          console.log(`Gift card assigned to customer ${customerId}`);
        } else if (assignData.data?.giftCardAssign?.userErrors?.length > 0) {
          console.warn('Could not assign gift card to customer:', assignData.data.giftCardAssign.userErrors);
        }
      } catch (assignError) {
        console.warn('Error assigning gift card to customer (gift card still created):', assignError);
        // Gift card is still created, just not assigned - customer can still use the code
      }
    }

    if (data.errors || (data.data?.giftCardCreate?.userErrors?.length > 0)) {
      console.error('GraphQL errors:', data.errors || data.data?.giftCardCreate?.userErrors);
      return res.status(500).json({ 
        error: 'Failed to create gift card',
        details: data.errors || data.data?.giftCardCreate?.userErrors
      });
    }

    const giftCard = data.data.giftCardCreate.giftCard;
    
    // Update submission
    submission.giftCardCode = giftCard.code;
    submission.giftCardId = giftCard.id;
    submission.status = 'completed';
    submission.updatedAt = new Date().toISOString();

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
            <p style="font-size: 24px; font-weight: bold; letter-spacing: 2px; color: #ef4444;">${giftCard.code}</p>
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
        code: giftCard.code,
        amount: submission.finalPrice,
        currency: 'GBP'
      },
      submission
    });

  } catch (error) {
    console.error('Error issuing credit:', error);
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

