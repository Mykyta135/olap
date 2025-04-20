// fact-transaction-loader.js
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// Configuration
const config = {
  oltp: {
    host: 'localhost',
    database: 'OLTP',
    user: 'postgres',
    password: '1234', // Replace with your actual password
    port: 5432
  },
  olap: {
    host: 'localhost',
    database: 'OLAP',
    user: 'postgres',
    password: '1234', // Replace with your actual password
    port: 5432
  },
  batchSize: 1000,
  checkpointDir: './checkpoints',
  logDir: './logs'
};

// Create directories if they don't exist
if (!fs.existsSync(config.checkpointDir)) {
  fs.mkdirSync(config.checkpointDir, { recursive: true });
}
if (!fs.existsSync(config.logDir)) {
  fs.mkdirSync(config.logDir, { recursive: true });
}

// Initialize database connections
const oltpPool = new Pool(config.oltp);
const olapPool = new Pool(config.olap);

// Logger utility
class Logger {
  constructor(filename) {
    this.logPath = path.join(config.logDir, filename);
    this.stream = fs.createWriteStream(this.logPath, { flags: 'a' });
  }

  log(message) {
    const timestamp = new Date().toISOString();
    const formattedMessage = `[${timestamp}] INFO: ${message}\n`;
    this.stream.write(formattedMessage);
    console.log(formattedMessage.trim());
  }

  error(message, error) {
    const timestamp = new Date().toISOString();
    const formattedMessage = `[${timestamp}] ERROR: ${message} - ${error.message}\n`;
    this.stream.write(formattedMessage);
    console.error(formattedMessage.trim());
  }

  close() {
    this.stream.end();
  }
}

// Checkpoint management
class CheckpointManager {
  constructor(name) {
    this.checkpointPath = path.join(config.checkpointDir, `${name}_checkpoint.json`);
  }

  saveCheckpoint(data) {
    fs.writeFileSync(this.checkpointPath, JSON.stringify(data), 'utf8');
  }

  loadCheckpoint() {
    if (fs.existsSync(this.checkpointPath)) {
      const data = fs.readFileSync(this.checkpointPath, 'utf8');
      return JSON.parse(data);
    }
    return null;
  }

  clearCheckpoint() {
    if (fs.existsSync(this.checkpointPath)) {
      fs.unlinkSync(this.checkpointPath);
    }
  }
}

// Transaction Fact Table Loader
class TransactionFactLoader {
  constructor() {
    this.name = 'fact_transactions';
    this.logger = new Logger(`${this.name}_loader.log`);
    this.checkpointManager = new CheckpointManager(this.name);
    this.errors = [];
    
    // Dimension lookup caches
    this.dateCache = {};
    this.addressCache = {};
    this.propertyTypeCache = {};
    this.methodCache = {};
    
    // Statistics
    this.stats = {
      total: 0,
      processed: 0,
      inserted: 0,
      errors: 0,
      start: null,
      end: null
    };
  }

  async loadDimensionCaches() {
    const olapClient = await olapPool.connect();
    try {
      // Load date dimension cache
      this.logger.log('Loading date dimension cache...');
      const dateResult = await olapClient.query('SELECT date_id, full_date FROM dim_date');
      dateResult.rows.forEach(row => {
        // Store as YYYY-MM-DD string for easy lookup
        const dateStr = new Date(row.full_date).toISOString().split('T')[0];
        this.dateCache[dateStr] = row.date_id;
      });
      this.logger.log(`Loaded ${Object.keys(this.dateCache).length} date entries`);
      
      // Load property type cache
      this.logger.log('Loading property type dimension cache...');
      const propertyTypeResult = await olapClient.query('SELECT property_type_id, property_type_name FROM dim_property_type');
      propertyTypeResult.rows.forEach(row => {
        this.propertyTypeCache[row.property_type_name.toLowerCase()] = row.property_type_id;
      });
      this.logger.log(`Loaded ${Object.keys(this.propertyTypeCache).length} property type entries`);
      
      // Load method cache
      this.logger.log('Loading method dimension cache...');
      const methodResult = await olapClient.query('SELECT method_id, method_name FROM dim_method');
      methodResult.rows.forEach(row => {
        this.methodCache[row.method_name.toLowerCase()] = row.method_id;
      });
      this.logger.log(`Loaded ${Object.keys(this.methodCache).length} method entries`);
      
      // We won't cache all addresses as there might be too many
      // We'll look them up as needed
    } finally {
      olapClient.release();
    }
  }
  
  async lookupAddressId(oltpPropertyId) {
    // Check cache first
    if (this.addressCache[oltpPropertyId]) {
      return this.addressCache[oltpPropertyId];
    }
  
    const oltpClient = await oltpPool.connect();
    const olapClient = await olapPool.connect();
  
    try {
      // 1. Отримуємо з OLTP потрібні поля
      const propertyResult = await oltpClient.query(`
        SELECT 
          suburb, 
          address,
          council_area,
          region_name
        FROM property_location 
        WHERE id = $1
      `, [oltpPropertyId]);
  
      if (propertyResult.rows.length === 0) {
        return null;
      }
  
      const { suburb, address, council_area, region_name } = propertyResult.rows[0];
  
      // 2. Парсимо адресу в JS
      const parsed = this.parseAddress(address);
      const streetNumber = parsed?.streetNumber || '';
      const streetName = parsed?.streetName || address;
  
      // 3. Пошук address_id в OLAP
      const result = await olapClient.query(`
        SELECT a.address_id
        FROM dim_address a
        JOIN dim_suburb s ON a.suburb_id = s.suburb_id
        JOIN dim_council c ON s.council_id = c.council_id
        JOIN dim_metropolitan m ON c.metropolitan_id = m.metropolitan_id
        WHERE s.suburb_name = $1
          AND c.council_name = $2
          AND m.metropolitan_name = $3
          AND a.street_name = $4
          AND a.street_number = $5
          AND a.is_current = TRUE
        LIMIT 1
      `, [suburb, council_area, region_name, streetName, streetNumber]);
  
      if (result.rows.length > 0) {
        const addressId = result.rows[0].address_id;
        this.addressCache[oltpPropertyId] = addressId;
        return addressId;
      }
  
      return null;
    } finally {
      oltpClient.release();
      olapClient.release();
    }
  }
  
  parseAddress(address) {
    const match = address.match(/^(\d+[a-zA-Z]?)\s+(.+)$/);
    if (match) {
      return {
        streetNumber: match[1],
        streetName: match[2]
      };
    }
  
    return {
      streetNumber: '',
      streetName: address
    };
  }
  
  
  mapPropertyType(type) {
    // Map the short property type codes to full names
    const typeMap = {
      'h': 'House',
      'u': 'Unit',
      't': 'Townhouse',
      'v': 'Villa',
      'd': 'Duplex',
      'a': 'Apartment'
      // Add more mappings as needed
    };
    
    return typeMap[type.toLowerCase()] || type;
  }
  
  validateTransaction(transaction) {
    // Basic validation
    if (!transaction.sale_date) {
      return { isValid: false, error: 'Missing sale date' };
    }
    
    if (!transaction.property_id) {
      return { isValid: false, error: 'Missing property ID' };
    }
    
    if (!transaction.price || transaction.price <= 0) {
      return { isValid: false, error: 'Invalid price' };
    }
    
    if (!transaction.method) {
      return { isValid: false, error: 'Missing sale method' };
    }
    
    return { isValid: true };
  }
  
  async loadByDateRange(startDate, endDate, checkpoint = null) {
    this.logger.log(`Starting transaction fact load for date range: ${startDate} to ${endDate}`);
    
    // Use checkpoint if available
    let offset = checkpoint?.offset || 0;
    let lastProcessedId = checkpoint?.lastProcessedId || 0;
    
    // Get total count for progress reporting
    const oltpClient = await oltpPool.connect();
    try {
      const countQuery = `
        SELECT COUNT(*) 
        FROM transactions t
        WHERE t.sale_date BETWEEN $1 AND $2
      `;
      
      const countResult = await oltpClient.query(countQuery, [startDate, endDate]);
      this.stats.total = parseInt(countResult.rows[0].count);
      this.logger.log(`Total transactions in range: ${this.stats.total}`);
      
      // Process in batches
      let hasMore = true;
      this.stats.start = new Date();
      
      while (hasMore) {
        // Get a batch of transactions
        const query = `
          SELECT 
            t.id, 
            t.property_id, 
            t.price, 
            t.method, 
            t.sale_date,
            t.feature_id,
            f.bedroom,
            f.bathroom,
            f.car,
            f.landsize,
            f.building_area,
            f.year_built,
            f.distance,
            f.type as property_type
          FROM 
            transactions t
            JOIN property_features f ON t.feature_id = f.id
          WHERE 
            t.sale_date BETWEEN $1 AND $2
            AND t.id > $3
          ORDER BY 
            t.id
          LIMIT $4
        `;
        
        const result = await oltpClient.query(query, [startDate, endDate, lastProcessedId, config.batchSize]);
        
        if (result.rows.length === 0) {
          hasMore = false;
          continue;
        }
        
        this.logger.log(`Processing batch of ${result.rows.length} transactions`);
        
        // Transform and load the batch
        const batch = [];
        for (const row of result.rows) {
          // Update lastProcessedId for checkpoint
          lastProcessedId = row.id;
          this.stats.processed++;
          
          // Validate transaction
          const validationResult = this.validateTransaction(row);
          if (!validationResult.isValid) {
            this.errors.push({
              transaction_id: row.id,
              error: validationResult.error
            });
            this.stats.errors++;
            continue;
          }
          
          // Look up dimension keys
          const saleDate = new Date(row.sale_date).toISOString().split('T')[0];
          const dateId = this.dateCache[saleDate];
          
          if (!dateId) {
            this.errors.push({
              transaction_id: row.id,
              error: `Date not found in dimension: ${saleDate}`
            });
            this.stats.errors++;
            continue;
          }
          
          // Look up address
          const addressId = await this.lookupAddressId(row.property_id);
          if (!addressId) {
            this.errors.push({
              transaction_id: row.id,
              error: `Address not found for property ID: ${row.property_id}`
            });
            this.stats.errors++;
            continue;
          }
          
          // Look up property type
          const propertyTypeName = this.mapPropertyType(row.property_type);
          const propertyTypeId = this.propertyTypeCache[propertyTypeName.toLowerCase()];
          
          if (!propertyTypeId) {
            this.errors.push({
              transaction_id: row.id,
              error: `Property type not found: ${propertyTypeName}`
            });
            this.stats.errors++;
            continue;
          }
          
          // Look up method
          const methodId = this.methodCache[row.method.toLowerCase()];
          if (!methodId) {
            this.errors.push({
              transaction_id: row.id,
              error: `Method not found: ${row.method}`
            });
            this.stats.errors++;
            continue;
          }
          
          // Add to batch
          batch.push({
            date_id: dateId,
            address_id: addressId,
            property_type_id: propertyTypeId,
            method_id: methodId,
            price: row.price,
            year_built: row.year_built,
            building_area: row.building_area,
            land_area: row.landsize,
            bathroom_count: row.bathroom,
            bedroom_count: row.bedroom,
            garage_count: row.car,
            distance_to_center: row.distance
          });
        }
        
        // Insert the batch
        if (batch.length > 0) {
          const inserted = await this.insertBatch(batch);
          this.stats.inserted += inserted;
        }
        
        // Save checkpoint after each batch
        this.checkpointManager.saveCheckpoint({ 
          offset: offset + result.rows.length, 
          lastProcessedId: lastProcessedId,
          dateRange: { startDate, endDate },
          stats: this.stats
        });
        
        offset += result.rows.length;
        
        // Check if we've processed everything
        if (result.rows.length < config.batchSize) {
          hasMore = false;
        }
        
        // Log progress
        const progress = Math.round((this.stats.processed / this.stats.total) * 100);
        this.logger.log(`Progress: ${progress}% (${this.stats.processed}/${this.stats.total})`);
      }
      
      this.stats.end = new Date();
      const duration = (this.stats.end - this.stats.start) / 1000;
      
      this.logger.log(`Completed processing ${this.stats.processed} transactions in ${duration.toFixed(2)} seconds`);
      this.logger.log(`Inserted: ${this.stats.inserted}, Errors: ${this.stats.errors}`);
      
      // Save error report
      this.saveErrors();
      
      // Clear checkpoint if complete
      if (this.stats.processed >= this.stats.total) {
        this.checkpointManager.clearCheckpoint();
      }
      
      return this.stats;
    } finally {
      oltpClient.release();
    }
  }
  
  async insertBatch(records) {
    if (records.length === 0) return 0;
    
    const olapClient = await olapPool.connect();
    try {
      await olapClient.query('BEGIN');
      
      // Build multi-row insert statement
      const columns = [
        'date_id', 'address_id', 'property_type_id', 'method_id', 
        'price', 'year_built', 'building_area', 'land_area',
        'bathroom_count', 'bedroom_count', 'garage_count', 'distance_to_center'
      ];
      
      const placeholders = [];
      const values = [];
      let valueIndex = 1;
      
      records.forEach(record => {
        const rowPlaceholders = [];
        columns.forEach(column => {
          values.push(record[column]);
          rowPlaceholders.push(`$${valueIndex++}`);
        });
        placeholders.push(`(${rowPlaceholders.join(', ')})`);
      });
      
      const query = `
        INSERT INTO fact_transactions (${columns.join(', ')})
        VALUES ${placeholders.join(', ')}
      `;
      
      await olapClient.query(query, values);
      await olapClient.query('COMMIT');
      
      this.logger.log(`Inserted ${records.length} records into fact_transactions`);
      return records.length;
    } catch (error) {
      await olapClient.query('ROLLBACK');
      this.logger.error(`Error inserting batch into fact_transactions`, error);
      this.stats.errors += records.length;
      return 0;
    } finally {
      olapClient.release();
    }
  }
  
  saveErrors() {
    if (this.errors.length === 0) return;
    
    const errorLogPath = path.join(config.logDir, `${this.name}_errors.json`);
    fs.writeFileSync(errorLogPath, JSON.stringify(this.errors, null, 2), 'utf8');
    this.logger.log(`Errors saved to ${errorLogPath}`);
  }
  
  async load(startDate, endDate) {
    try {
      // Check for existing checkpoint
      const checkpoint = this.checkpointManager.loadCheckpoint();
      if (checkpoint && 
          checkpoint.dateRange && 
          checkpoint.dateRange.startDate === startDate && 
          checkpoint.dateRange.endDate === endDate) {
        this.logger.log(`Resuming from checkpoint: processed ${checkpoint.offset} transactions, last ID: ${checkpoint.lastProcessedId}`);
        this.stats = checkpoint.stats || this.stats;
      } else {
        this.logger.log('No matching checkpoint found, starting fresh load');
      }
      
      // Load dimension caches first
      await this.loadDimensionCaches();
      
      // Load transactions
      await this.loadByDateRange(startDate, endDate, checkpoint);
    } catch (error) {
      this.logger.error('Unhandled error during transaction fact loading', error);
      throw error;
    } finally {
      this.logger.close();
    }
  }
}

// Range-based loading function
async function loadTransactionsByRanges(dateRanges) {
  try {
    console.log('Starting transaction fact table loading...');
    
    for (const range of dateRanges) {
      console.log(`\n--- Processing date range: ${range.start} to ${range.end} ---`);
      const loader = new TransactionFactLoader();
      await loader.load(range.start, range.end);
    }
    
    console.log('\nTransaction fact loading process complete!');
  } catch (error) {
    console.error('Error in ETL process:', error);
  } finally {
    // Close connection pools
    await oltpPool.end();
    await olapPool.end();
  }
}

// Example usage with multiple date ranges
async function main() {
  // Define date ranges for batch processing
  // Each range will be processed separately
  const dateRanges = [
    { start: '2016-01-01', end: '2016-03-31' },
    { start: '2016-04-01', end: '2016-06-30' },
    { start: '2016-07-01', end: '2016-09-30' },
    { start: '2016-10-01', end: '2016-12-31' }
    // Add more ranges as needed
  ];
  
  await loadTransactionsByRanges(dateRanges);
}

// Entry point
if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = {
  TransactionFactLoader,
  loadTransactionsByRanges
};