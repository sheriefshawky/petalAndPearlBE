import express from "express";
import { v4 as uuidv4 } from "uuid";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import pg from "pg";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import dotenv from "dotenv";
import cors from "cors";
import Stripe from "stripe";
import Mailjet from "node-mailjet";
import { createServer as createViteServer } from "vite";

import Database from "better-sqlite3";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config();

const { Pool } = pg;
let pool: pg.Pool | null = null;
let sqliteDb: Database.Database | null = null;

let isPostgres = !!process.env.DATABASE_URL && !process.env.DATABASE_URL.includes("REPLACE_WITH");

function getSqlite() {
  if (!sqliteDb) {
    sqliteDb = new Database("database.sqlite");
    sqliteDb.pragma("journal_mode = WAL");
  }
  return sqliteDb;
}

function getPool() {
  if (!isPostgres) return null;
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    pool = new Pool({
      connectionString,
      connectionTimeoutMillis: 5000,
      max: 20,
      idleTimeoutMillis: 30000,
    });
    
    pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
      pool = null;
    });
  }
  return pool;
}

const JWT_SECRET = process.env.JWT_SECRET || "fallback_secret";
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;

// --- Helper to handle DB queries with error catching ---
const query = async (text: string, params: any[] = []) => {
  if (isPostgres) {
    try {
      const p = getPool();
      if (!p) throw new Error("PostgreSQL pool is not initialized");
      return await p.query(text, params);
    } catch (err: any) {
      if (err.code === 'ENOTFOUND' || err.code === 'EAI_AGAIN') {
        const host = process.env.DATABASE_URL?.match(/@([^/:]+)/)?.[1];
        console.error(`[Database Error] Could not resolve host "${host}". This usually means the hostname in your DATABASE_URL is incorrect or a placeholder.`);
      } else {
        console.error("PostgreSQL query error:", err);
      }
      throw err;
    }
  } else {
    try {
      const db = getSqlite();
      // Convert $1, $2 to ? for SQLite
      let sqliteText = text;
      const sqliteParams = [...params];
      
      // Simple regex to replace $1, $2... with ?
      // This is safe as long as $ is not used in literal strings in the SQL
      sqliteText = sqliteText.replace(/\$\d+/g, '?');

      const isReturning = sqliteText.trim().toUpperCase().includes("RETURNING");
      if (sqliteText.trim().toUpperCase().startsWith("SELECT") || isReturning) {
        const rows = db.prepare(sqliteText).all(...sqliteParams);
        return { rows };
      } else {
        const result = db.prepare(sqliteText).run(...sqliteParams);
        // Mocking the pg structure for consistency
        return { 
          rows: result.lastInsertRowid ? [{ id: result.lastInsertRowid }] : [],
          rowCount: result.changes 
        };
      }
    } catch (err) {
      console.error("SQLite query error:", err);
      throw err;
    }
  }
};

// Initialize Database
async function initDb() {
  const schema = `
    CREATE TABLE IF NOT EXISTS categories (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sub_categories (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      category_id INTEGER REFERENCES categories(id),
      name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS products (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      category_id INTEGER REFERENCES categories(id),
      sub_category_id INTEGER REFERENCES sub_categories(id),
      title TEXT NOT NULL,
      description TEXT,
      main_image TEXT,
      price REAL NOT NULL,
      currency TEXT DEFAULT 'EGP',
      is_featured INTEGER DEFAULT 0,
      is_active INTEGER DEFAULT 1,
      stock_quantity INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS product_images (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      product_id INTEGER REFERENCES products(id),
      image_url TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS orders (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      guid TEXT UNIQUE NOT NULL,
      status TEXT DEFAULT 'new',
      customer_name TEXT,
      customer_mobile TEXT,
      customer_address TEXT,
      customer_email TEXT,
      customer_lat REAL,
      customer_lng REAL,
      payment_method TEXT DEFAULT 'cod',
      payment_status TEXT DEFAULT 'pending',
      stripe_session_id TEXT,
      shipping_fee REAL DEFAULT 0,
      total_amount REAL DEFAULT 0,
      discount_code TEXT,
      discount_amount REAL DEFAULT 0,
      discount_source TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS order_items (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      order_id INTEGER REFERENCES orders(id),
      product_id INTEGER REFERENCES products(id),
      quantity INTEGER,
      price REAL
    );

    CREATE TABLE IF NOT EXISTS admins (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS discount_codes (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      code TEXT UNIQUE NOT NULL,
      agent_name TEXT,
      agent_mobile TEXT,
      type TEXT NOT NULL,
      value REAL NOT NULL,
      start_date TIMESTAMP NOT NULL,
      end_date TIMESTAMP NOT NULL,
      is_active INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS scratch_codes (
      id ${isPostgres ? 'SERIAL' : 'INTEGER'} PRIMARY KEY ${isPostgres ? '' : 'AUTOINCREMENT'},
      code TEXT UNIQUE NOT NULL,
      type TEXT NOT NULL,
      value REAL NOT NULL,
      is_used INTEGER DEFAULT 0,
      is_active INTEGER DEFAULT 1,
      used_at TIMESTAMP,
      order_id INTEGER REFERENCES orders(id)
    );
  `;

  try {
    if (isPostgres) {
      try {
        const p = getPool();
        if (p) {
          const client = await p.connect();
          try {
            await client.query(schema);
            // Migrations
            const migrations = [
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method TEXT DEFAULT 'cod'",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status TEXT DEFAULT 'pending'",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS stripe_session_id TEXT",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS shipping_fee REAL DEFAULT 0",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount REAL DEFAULT 0",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_code TEXT",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_amount REAL DEFAULT 0",
              "ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_source TEXT",
              "ALTER TABLE products ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1",
              "ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_quantity INTEGER DEFAULT 0"
            ];
            for (const m of migrations) {
              try { await client.query(m); } catch (e) {}
            }
            console.log("PostgreSQL initialized");
          } finally {
            client.release();
          }
        } else {
          throw new Error("Pool creation returned null");
        }
      } catch (pgErr) {
        console.error("PostgreSQL connection failed, falling back to SQLite:", pgErr);
        isPostgres = false;
        // Fall through to SQLite initialization
      }
    }

    if (!isPostgres) {
      const db = getSqlite();
      db.exec(schema);
      console.log("SQLite initialized");
    }

    // Create default admin
    const adminUser = process.env.ADMIN_USER || "admin";
    const adminPass = process.env.ADMIN_PASS || "admin123";
    const existingAdmin = await query("SELECT * FROM admins WHERE username = $1", [adminUser]);
    if (existingAdmin.rows.length === 0) {
      const hashedPass = await bcrypt.hash(adminPass, 10);
      await query("INSERT INTO admins (username, password) VALUES ($1, $2)", [adminUser, hashedPass]);
      console.log("Default admin created");
    }
  } catch (err) {
    console.error("Database initialization failed:", err);
  }
}

async function startServer() {
  // We don't await initDb() here to prevent startup blocking
  initDb();
  
  const app = express();
  
  // Enable CORS for all origins to resolve cross-origin issues
  app.use(cors());

  // Stripe Webhook (must be before express.json())
  app.post("/api/webhooks/stripe", express.raw({ type: 'application/json' }), async (req, res) => {
    const sig = req.headers['stripe-signature'];
    const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;

    if (!stripe || !sig || !webhookSecret) {
      return res.status(400).send('Webhook Error: Missing configuration');
    }

    let event;
    try {
      event = stripe.webhooks.constructEvent(req.body, sig, webhookSecret);
    } catch (err: any) {
      console.error(`Webhook Error: ${err.message}`);
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    // Handle the event
    if (event.type === 'checkout.session.completed') {
      const session = event.data.object as Stripe.Checkout.Session;
      const orderGuid = session.client_reference_id;
      
      console.log(`[Stripe Webhook] Payment successful for order: ${orderGuid}`);
      
      try {
        await query(
          "UPDATE orders SET payment_status = 'paid', status = 'validated' WHERE guid = $1",
          [orderGuid]
        );
        // Send emails after payment success
        sendOrderEmails(orderGuid);
      } catch (err) {
        console.error(`[Stripe Webhook] Failed to update order ${orderGuid}:`, err);
      }
    }

    res.json({ received: true });
  });
  
  app.use(express.json());
  const PORT = parseInt(process.env.PORT || "3000", 10);

  // --- Mailjet Email Helper ---
  const sendOrderEmails = async (orderGuid: string) => {
    try {
      const apiKey = process.env.MAILJET_API_KEY;
      const apiSecret = process.env.MAILJET_API_SECRET;
      const adminEmail = process.env.ADMIN_EMAIL;
      const senderEmail = process.env.SENDER_EMAIL || "noreply@sweetgems.com";
      const appUrl = process.env.APP_URL || "http://localhost:3000";

      if (!apiKey || !apiSecret) {
        console.warn("[Mailjet] API keys missing, skipping emails.");
        return;
      }

      // Fetch order details
      const orderRes = await query("SELECT * FROM orders WHERE guid = $1", [orderGuid]);
      const order = orderRes.rows[0];
      if (!order) return;

      // Fetch items
      const itemsRes = await query(`
        SELECT oi.*, p.title as product_title
        FROM order_items oi
        JOIN products p ON oi.product_id = p.id
        WHERE oi.order_id = $1
      `, [order.id]);
      const items = itemsRes.rows;

      const mailjet = new Mailjet({ apiKey, apiSecret });
      const trackingLink = `${appUrl}/track?guid=${order.guid}`;
      
      const itemsHtml = items.map(item => 
        `<li>${item.product_title} x ${item.quantity} - ${item.price} ${order.currency || 'EGP'}</li>`
      ).join('');

      const total = items.reduce((sum, item) => sum + (item.price * item.quantity), 0);

      const customerEmailContent = {
        Messages: [
          {
            From: { Email: senderEmail, Name: "Sweetgems" },
            To: [{ Email: order.customer_email, Name: order.customer_name }],
            Subject: `Order Confirmation - ${order.guid}`,
            HTMLPart: `
              <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #eee; border-radius: 10px;">
                <h1 style="color: #000;">Thank you for your order, ${order.customer_name}!</h1>
                <p>Your order has been placed successfully. You can track your order using the link below:</p>
                <div style="margin: 30px 0;">
                  <a href="${trackingLink}" style="background: #000; color: #fff; padding: 15px 25px; text-decoration: none; border-radius: 5px; font-weight: bold;">Track Your Order</a>
                </div>
                <p><strong>Order ID:</strong> ${order.guid}</p>
                <p><strong>Payment Method:</strong> ${order.payment_method.toUpperCase()}</p>
                <h3 style="border-bottom: 1px solid #eee; padding-bottom: 10px;">Order Details:</h3>
                <ul style="list-style: none; padding: 0;">${itemsHtml}</ul>
                <p style="font-size: 1.2em; font-weight: bold; border-top: 1px solid #eee; padding-top: 10px;">Total: ${total} ${order.currency || 'EGP'}</p>
                <p style="color: #666; font-size: 0.9em; margin-top: 40px;">We'll notify you when your order status changes.</p>
              </div>
            `
          }
        ]
      };

      const adminEmailContent = {
        Messages: [
          {
            From: { Email: senderEmail, Name: "Sweetgems System" },
            To: [{ Email: adminEmail, Name: "Admin" }],
            Subject: `New Order Received - ${order.guid}`,
            HTMLPart: `
              <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #eee; border-radius: 10px;">
                <h1 style="color: #000;">New Order Received</h1>
                <p><strong>Customer:</strong> ${order.customer_name}</p>
                <p><strong>Email:</strong> ${order.customer_email}</p>
                <p><strong>Mobile:</strong> ${order.customer_mobile}</p>
                <p><strong>Address:</strong> ${order.customer_address}</p>
                <p><strong>Payment Method:</strong> ${order.payment_method.toUpperCase()}</p>
                <h3 style="border-bottom: 1px solid #eee; padding-bottom: 10px;">Order Items:</h3>
                <ul style="list-style: none; padding: 0;">${itemsHtml}</ul>
                <p style="font-size: 1.2em; font-weight: bold; border-top: 1px solid #eee; padding-top: 10px;">Total: ${total} ${order.currency || 'EGP'}</p>
                <div style="margin: 30px 0;">
                  <a href="${appUrl}/admin" style="background: #000; color: #fff; padding: 15px 25px; text-decoration: none; border-radius: 5px; font-weight: bold;">Go to Admin Dashboard</a>
                </div>
              </div>
            `
          }
        ]
      };

      await mailjet.post("send", { version: 'v3.1' }).request(customerEmailContent);
      if (adminEmail) {
        await mailjet.post("send", { version: 'v3.1' }).request(adminEmailContent);
      }
      console.log(`[Mailjet] Emails sent for order: ${orderGuid}`);
    } catch (err) {
      console.error("[Mailjet Error]", err);
    }
  };

  // --- Auth Middleware ---
  const authenticateToken = (req: any, res: any, next: any) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return res.sendStatus(401);
    const token = authHeader.split(' ')[1];
    if (!token) return res.sendStatus(401);

    jwt.verify(token, JWT_SECRET, (err: any, user: any) => {
      if (err) return res.sendStatus(403);
      req.user = user;
      next();
    });
  };

  // --- Auth Routes ---
  app.post("/api/admin/login", async (req, res) => {
    try {
      const { username, password } = req.body;
      const result = await query("SELECT * FROM admins WHERE username = $1", [username]);
      const admin = result.rows[0];

      if (admin && await bcrypt.compare(password, admin.password)) {
        const token = jwt.sign({ id: admin.id, username: admin.username }, JWT_SECRET, { expiresIn: '24h' });
        res.json({ token });
      } else {
        res.status(401).json({ error: "Invalid credentials" });
      }
    } catch (err) {
      res.status(500).json({ error: "Database connection error" });
    }
  });

  // --- API Routes ---

  // Categories
  app.get("/api/categories", async (req, res) => {
    try {
      const result = await query("SELECT * FROM categories");
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.post("/api/categories", authenticateToken, async (req, res) => {
    try {
      const { name } = req.body;
      const result = await query("INSERT INTO categories (name) VALUES ($1) RETURNING *", [name]);
      res.json(result.rows[0]);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.put("/api/categories/:id", authenticateToken, async (req, res) => {
    try {
      const { name } = req.body;
      await query("UPDATE categories SET name = $1 WHERE id = $2", [name, req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.delete("/api/categories/:id", authenticateToken, async (req, res) => {
    try {
      const catId = req.params.id;
      // Check for sub-categories
      const subRes = await query("SELECT id FROM sub_categories WHERE category_id = $1", [catId]);
      if (subRes.rows.length > 0) {
        return res.status(400).json({ error: "Cannot delete category with linked sub-categories" });
      }
      // Check for products
      const prodRes = await query("SELECT id FROM products WHERE category_id = $1", [catId]);
      if (prodRes.rows.length > 0) {
        return res.status(400).json({ error: "Cannot delete category with linked products" });
      }
      await query("DELETE FROM categories WHERE id = $1", [catId]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  // Sub-categories
  app.get("/api/sub-categories", async (req, res) => {
    try {
      const result = await query("SELECT * FROM sub_categories");
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.get("/api/sub-categories/:categoryId", async (req, res) => {
    try {
      const result = await query("SELECT * FROM sub_categories WHERE category_id = $1", [req.params.categoryId]);
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.post("/api/sub-categories", authenticateToken, async (req, res) => {
    try {
      const { category_id, name } = req.body;
      const result = await query("INSERT INTO sub_categories (category_id, name) VALUES ($1, $2) RETURNING *", [category_id, name]);
      res.json(result.rows[0]);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.put("/api/sub-categories/:id", authenticateToken, async (req, res) => {
    try {
      const { name } = req.body;
      await query("UPDATE sub_categories SET name = $1 WHERE id = $2", [name, req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.delete("/api/sub-categories/:id", authenticateToken, async (req, res) => {
    try {
      const subId = req.params.id;
      // Check for products
      const prodRes = await query("SELECT id FROM products WHERE sub_category_id = $1", [subId]);
      if (prodRes.rows.length > 0) {
        return res.status(400).json({ error: "Cannot delete sub-category with linked products" });
      }
      await query("DELETE FROM sub_categories WHERE id = $1", [subId]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  // Products
  app.get("/api/products", async (req, res) => {
    try {
      const activeOnly = req.query.activeOnly === 'true';
      let sql = `
        SELECT p.*, c.name as category_name, s.name as sub_category_name 
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        LEFT JOIN sub_categories s ON p.sub_category_id = s.id
      `;
      
      if (activeOnly) {
        sql += " WHERE p.is_active = 1";
      }

      const result = await query(sql);
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.get("/api/products/featured", async (req, res) => {
    try {
      const result = await query("SELECT * FROM products WHERE is_featured = 1 AND is_active = 1");
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.get("/api/products/:id", async (req, res) => {
    try {
      const productResult = await query("SELECT * FROM products WHERE id = $1", [req.params.id]);
      const product = productResult.rows[0];
      if (!product) return res.status(404).json({ error: "Product not found" });
      const imagesResult = await query("SELECT image_url FROM product_images WHERE product_id = $1", [req.params.id]);
      res.json({ ...product, images: imagesResult.rows.map(img => img.image_url) });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.post("/api/products", authenticateToken, async (req, res) => {
    try {
      const { category_id, sub_category_id, title, description, main_image, price, currency, is_featured, is_active, stock_quantity, extra_images } = req.body;
      const result = await query(`
        INSERT INTO products (category_id, sub_category_id, title, description, main_image, price, currency, is_featured, is_active, stock_quantity)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id
      `, [
        category_id, sub_category_id, title, description, main_image, price, currency || 'EGP', 
        is_featured ? 1 : 0, 
        is_active === false ? 0 : 1,
        stock_quantity || 0
      ]);
      
      const productId = result.rows[0].id;
      if (extra_images && Array.isArray(extra_images)) {
        for (const img of extra_images) {
          await query("INSERT INTO product_images (product_id, image_url) VALUES ($1, $2)", [productId, img]);
        }
      }
      res.json({ id: productId });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.put("/api/products/:id", authenticateToken, async (req, res) => {
    try {
      const { category_id, sub_category_id, title, description, main_image, price, currency, is_featured, is_active, stock_quantity, extra_images } = req.body;
      const productId = req.params.id;

      await query(`
        UPDATE products 
        SET category_id = $1, sub_category_id = $2, title = $3, description = $4, main_image = $5, price = $6, currency = $7, is_featured = $8, is_active = $9, stock_quantity = $10
        WHERE id = $11
      `, [
        category_id, sub_category_id, title, description, main_image, price, currency || 'EGP', 
        is_featured ? 1 : 0, 
        is_active === false ? 0 : 1,
        stock_quantity || 0,
        productId
      ]);

      // Update extra images: simplest way is to delete and re-insert
      await query("DELETE FROM product_images WHERE product_id = $1", [productId]);
      if (extra_images && Array.isArray(extra_images)) {
        for (const img of extra_images) {
          await query("INSERT INTO product_images (product_id, image_url) VALUES ($1, $2)", [productId, img]);
        }
      }
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.delete("/api/products/:id", authenticateToken, async (req, res) => {
    try {
      const productId = req.params.id;
      // Delete related images first
      await query("DELETE FROM product_images WHERE product_id = $1", [productId]);
      // Delete product
      await query("DELETE FROM products WHERE id = $1", [productId]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  // Discounts
  app.get("/api/admin/discounts", authenticateToken, async (req, res) => {
    try {
      const result = await query("SELECT * FROM discount_codes ORDER BY id DESC");
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.post("/api/admin/discounts", authenticateToken, async (req, res) => {
    try {
      const { code, agent_name, agent_mobile, type, value, start_date, end_date, is_active } = req.body;
      const result = await query(`
        INSERT INTO discount_codes (code, agent_name, agent_mobile, type, value, start_date, end_date, is_active)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *
      `, [code.toUpperCase(), agent_name, agent_mobile, type, value, start_date, end_date, is_active ? 1 : 0]);
      res.json(result.rows[0]);
    } catch (err: any) {
      if (err.code === '23505') return res.status(400).json({ error: "Discount code already exists" });
      res.status(500).json({ error: "Database error" });
    }
  });

  app.put("/api/admin/discounts/:id", authenticateToken, async (req, res) => {
    try {
      const { code, agent_name, agent_mobile, type, value, start_date, end_date, is_active } = req.body;
      await query(`
        UPDATE discount_codes 
        SET code = $1, agent_name = $2, agent_mobile = $3, type = $4, value = $5, start_date = $6, end_date = $7, is_active = $8
        WHERE id = $9
      `, [code.toUpperCase(), agent_name, agent_mobile, type, value, start_date, end_date, is_active ? 1 : 0, req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.delete("/api/admin/discounts/:id", authenticateToken, async (req, res) => {
    try {
      await query("DELETE FROM discount_codes WHERE id = $1", [req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  // Scratch Codes
  app.get("/api/admin/scratch-codes", authenticateToken, async (req, res) => {
    try {
      const result = await query("SELECT * FROM scratch_codes ORDER BY id DESC");
      res.json(result.rows);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.post("/api/admin/scratch-codes", authenticateToken, async (req, res) => {
    try {
      const { code, type, value, is_active } = req.body;
      const result = await query(`
        INSERT INTO scratch_codes (code, type, value, is_active)
        VALUES ($1, $2, $3, $4) RETURNING *
      `, [code.toUpperCase(), type, value, is_active ? 1 : 0]);
      res.json(result.rows[0]);
    } catch (err: any) {
      if (err.code === '23505') return res.status(400).json({ error: "Scratch code already exists" });
      res.status(500).json({ error: "Database error" });
    }
  });

  app.put("/api/admin/scratch-codes/:id", authenticateToken, async (req, res) => {
    try {
      const { code, type, value, is_active } = req.body;
      await query(`
        UPDATE scratch_codes 
        SET code = $1, type = $2, value = $3, is_active = $4
        WHERE id = $5
      `, [code.toUpperCase(), type, value, is_active ? 1 : 0, req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.delete("/api/admin/scratch-codes/:id", authenticateToken, async (req, res) => {
    try {
      await query("DELETE FROM scratch_codes WHERE id = $1", [req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.post("/api/discounts/validate", async (req, res) => {
    try {
      const { code } = req.body;
      if (!code) return res.status(400).json({ error: "Code is required" });

      // Check regular discount codes first
      const discRes = await query(`
        SELECT *, 'regular' as source FROM discount_codes 
        WHERE code = $1 AND is_active = 1 
        AND start_date <= CURRENT_TIMESTAMP 
        AND end_date >= CURRENT_TIMESTAMP
      `, [code.toUpperCase()]);

      if (discRes.rows.length > 0) {
        return res.json(discRes.rows[0]);
      }

      // Check scratch codes
      const scratchRes = await query(`
        SELECT *, 'scratch' as source FROM scratch_codes 
        WHERE code = $1 AND is_active = 1 AND is_used = 0
      `, [code.toUpperCase()]);

      if (scratchRes.rows.length > 0) {
        return res.json(scratchRes.rows[0]);
      }

      res.status(404).json({ error: "Invalid, expired, or already used code" });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  // Orders
  app.post("/api/orders", async (req, res) => {
    try {
      const { 
        customer_name, customer_mobile, customer_address, customer_email, 
        customer_lat, customer_lng, items, payment_method, discount_code 
      } = req.body;
      const guid = uuidv4();
      
      let discountAmount = 0;
      let appliedDiscountCode = null;
      let discountSource = null;

      if (discount_code) {
        const upperCode = discount_code.toUpperCase();
        // Check regular discount codes first
        const discRes = await query(`
          SELECT *, 'regular' as source FROM discount_codes 
          WHERE code = $1 AND is_active = 1 
          AND start_date <= CURRENT_TIMESTAMP 
          AND end_date >= CURRENT_TIMESTAMP
        `, [upperCode]);

        if (discRes.rows.length > 0) {
          appliedDiscountCode = upperCode;
          discountSource = 'regular';
        } else {
          // Check scratch codes
          const scratchRes = await query(`
            SELECT *, 'scratch' as source FROM scratch_codes 
            WHERE code = $1 AND is_active = 1 AND is_used = 0
          `, [upperCode]);

          if (scratchRes.rows.length > 0) {
            appliedDiscountCode = upperCode;
            discountSource = 'scratch';
          }
        }
      }

      // Stock Validation
      for (const item of items) {
        const productRes = await query("SELECT stock_quantity, title FROM products WHERE id = $1", [item.product_id]);
        const product = productRes.rows[0];
        if (!product) {
          return res.status(400).json({ error: `Product not found: ${item.product_id}` });
        }
        if (product.stock_quantity < item.quantity) {
          return res.status(400).json({ error: `Insufficient stock for ${product.title}. Available: ${product.stock_quantity}` });
        }
      }

      const orderResult = await query(`
        INSERT INTO orders (
          guid, customer_name, customer_mobile, customer_address, 
          customer_email, customer_lat, customer_lng, payment_method, shipping_fee, total_amount, discount_code, discount_amount, discount_source
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) RETURNING id
      `, [
        guid, customer_name, customer_mobile, customer_address, 
        customer_email, customer_lat, customer_lng, payment_method || 'cod', 0, 0, appliedDiscountCode, 0, discountSource
      ]);
      
      const orderId = orderResult.rows[0].id;

      let subtotal = 0;
      const lineItems = [];

      for (const item of items) {
        await query(`
          INSERT INTO order_items (order_id, product_id, quantity, price)
          VALUES ($1, $2, $3, $4)
        `, [orderId, item.product_id, item.quantity, item.price]);
        
        // Reduce Stock
        await query(`
          UPDATE products SET stock_quantity = stock_quantity - $1 WHERE id = $2
        `, [item.quantity, item.product_id]);

        subtotal += item.price * item.quantity;
      }

      // Calculate discount
      if (appliedDiscountCode) {
        let discount;
        if (discountSource === 'regular') {
          const discRes = await query("SELECT * FROM discount_codes WHERE code = $1", [appliedDiscountCode]);
          discount = discRes.rows[0];
        } else {
          const scratchRes = await query("SELECT * FROM scratch_codes WHERE code = $1", [appliedDiscountCode]);
          discount = scratchRes.rows[0];
          // Mark scratch code as used
          await query("UPDATE scratch_codes SET is_used = 1, used_at = CURRENT_TIMESTAMP, order_id = $1 WHERE id = $2", [orderId, discount.id]);
        }

        if (discount.type === 'percentage') {
          discountAmount = subtotal * (discount.value / 100);
        } else {
          discountAmount = discount.value;
        }
        // Ensure discount doesn't exceed subtotal
        discountAmount = Math.min(discountAmount, subtotal);
      }

      let totalAmount = subtotal - discountAmount;
      let shippingFee = 0;

      // Add shipping fee if total is <= 150
      if (totalAmount <= 150) {
        shippingFee = 50;
        totalAmount += shippingFee;
      }

      await query("UPDATE orders SET shipping_fee = $1, total_amount = $2, discount_amount = $3 WHERE id = $4", [shippingFee, totalAmount, discountAmount, orderId]);

      if (payment_method === 'online' && stripe) {
        // Prepare Stripe line items
        // We add the discount as a negative line item if possible, or just adjust the total
        // Stripe Checkout doesn't easily support negative amounts in line_items without coupons
        // So we'll just send the final items and a "Discount" item
        
        for (const item of items) {
          const productRes = await query("SELECT title FROM products WHERE id = $1", [item.product_id]);
          const productTitle = productRes.rows[0]?.title || "Product";
          lineItems.push({
            price_data: {
              currency: 'egp',
              product_data: { name: productTitle },
              unit_amount: Math.round(item.price * 100),
            },
            quantity: item.quantity,
          });
        }

        if (discountAmount > 0) {
          lineItems.push({
            price_data: {
              currency: 'egp',
              product_data: { name: `Discount (${appliedDiscountCode})` },
              unit_amount: -Math.round(discountAmount * 100),
            },
            quantity: 1,
          });
        }

        if (shippingFee > 0) {
          lineItems.push({
            price_data: {
              currency: 'egp',
              product_data: { name: "Shipping Fee" },
              unit_amount: Math.round(shippingFee * 100),
            },
            quantity: 1,
          });
        }

        console.log(`[Stripe] Creating checkout session for order: ${guid}`);
        const session = await stripe.checkout.sessions.create({
          payment_method_types: ['card'],
          line_items: lineItems,
          mode: 'payment',
          success_url: `${req.headers.origin}/track?guid=${guid}&payment=success`,
          cancel_url: `${req.headers.origin}/cart?payment=cancelled`,
          client_reference_id: guid,
          customer_email: customer_email,
        });

        await query("UPDATE orders SET stripe_session_id = $1 WHERE id = $2", [session.id, orderId]);
        return res.json({ guid, checkoutUrl: session.url });
      }

      // For COD, send emails immediately
      sendOrderEmails(guid);
      res.json({ guid });
    } catch (err: any) {
      console.error("[Order Creation Error]", err);
      res.status(500).json({ error: "Database error", message: err.message });
    }
  });

  app.get("/api/orders/track/:guid", async (req, res) => {
    try {
      const result = await query("SELECT * FROM orders WHERE guid = $1", [req.params.guid]);
      const order = result.rows[0];
      if (!order) return res.status(404).json({ error: "Order not found" });
      res.json(order);
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.get("/api/admin/orders", authenticateToken, async (req, res) => {
    try {
      const { status, productIds, dateFrom, dateTo, sortBy, sortOrder } = req.query;
      
      let sql = "SELECT DISTINCT o.* FROM orders o";
      const params: any[] = [];
      const conditions: string[] = [];

      if (productIds) {
        sql += " JOIN order_items oi ON o.id = oi.order_id";
        const ids = (productIds as string).split(',');
        conditions.push(`oi.product_id IN (${ids.map((_, i) => `$${params.length + i + 1}`).join(',')})`);
        params.push(...ids);
      }

      if (status) {
        const statuses = (status as string).split(',');
        conditions.push(`o.status IN (${statuses.map((_, i) => `$${params.length + i + 1}`).join(',')})`);
        params.push(...statuses);
      }

      if (dateFrom) {
        conditions.push(`o.created_at >= $${params.length + 1}`);
        params.push(dateFrom);
      }

      if (dateTo) {
        conditions.push(`o.created_at <= $${params.length + 1}`);
        params.push(dateTo);
      }

      if (conditions.length > 0) {
        sql += " WHERE " + conditions.join(" AND ");
      }

      const validSortBy = ['id', 'created_at'].includes(sortBy as string) ? sortBy : 'created_at';
      const validSortOrder = ['ASC', 'DESC'].includes((sortOrder as string)?.toUpperCase()) ? sortOrder : 'DESC';
      
      sql += ` ORDER BY o.${validSortBy} ${validSortOrder}`;

      const result = await query(sql, params);
      res.json(result.rows);
    } catch (err) {
      console.error(err);
      res.status(500).json({ error: "Database error" });
    }
  });

  app.get("/api/admin/orders/:id", authenticateToken, async (req, res) => {
    try {
      const orderResult = await query("SELECT * FROM orders WHERE id = $1", [req.params.id]);
      const order = orderResult.rows[0];
      if (!order) return res.status(404).json({ error: "Order not found" });
      
      const itemsResult = await query(`
        SELECT oi.*, p.title as product_title, p.main_image
        FROM order_items oi
        JOIN products p ON oi.product_id = p.id
        WHERE oi.order_id = $1
      `, [req.params.id]);
      
      res.json({ ...order, items: itemsResult.rows });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  app.patch("/api/admin/orders/:id/status", authenticateToken, async (req, res) => {
    try {
      const { status } = req.body;
      await query("UPDATE orders SET status = $1 WHERE id = $2", [status, req.params.id]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: "Database error" });
    }
  });

  // Analytics
  app.get("/api/admin/analytics", authenticateToken, async (req, res) => {
    try {
      const { startDate, endDate } = req.query;
      let sql = "SELECT * FROM orders";
      const params: any[] = [];

      if (startDate && endDate) {
        // Use TO_TIMESTAMP if needed, but standard ISO strings usually work with TIMESTAMP columns in PG
        sql += " WHERE created_at >= $1 AND created_at <= $2";
        params.push(startDate, endDate);
      }

      sql += " ORDER BY created_at DESC";
      const result = await query(sql, params);
      
      const orders = result.rows;
      const totalOrders = orders.length;
      const totalRevenue = orders.reduce((sum: number, order: any) => sum + (order.total_amount || 0), 0);
      const paidOrders = orders.filter((o: any) => o.payment_status === 'paid').length;

      res.json({
        stats: {
          totalOrders,
          totalRevenue,
          paidOrders
        },
        orders
      });
    } catch (err) {
      console.error("Analytics error:", err);
      res.status(500).json({ error: "Database error" });
    }
  });

  app.get("/api/health", async (req, res) => {
    try {
      await query("SELECT 1");
      res.json({ status: "ok", message: "Sweetgems API is running", database: "connected" });
    } catch (err) {
      res.status(500).json({ status: "error", message: "Sweetgems API is running", database: "disconnected" });
    }
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer().catch(err => {
  console.error("Failed to start server:", err);
});
