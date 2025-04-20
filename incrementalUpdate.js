// incremental-updates.js
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const { ChangeTrackingService, CHANGE_DETECTION } = require('./changeDetection');

// Configuration
const config = {
  oltp: {
    host: 'localhost',
    database: 'OLTP',
    user: 'postgres',
    password: '1234',
    port: 5432
  },
  olap: {
    host: 'localhost',
    database: 'OLAP',
    user: 'postgres',
    password: '1234',
    port: 5432
  },
  tracking: {
    changelogDir: './changelog',
    stateFile: './changelog/incremental_state.json',
    batchSize: 1000
  }
};

// Create directories if they don't exist
if (!fs.existsSync(config.tracking.changelogDir)) {
  fs.mkdirSync(config.tracking.changelogDir, { recursive: true });
}

// Initialize database connections
const oltpPool = new Pool(config.oltp);
const olapPool = new Pool(config.olap);

// Logger utility
class Logger {
  constructor(filename) {
    this.logPath = path.join(config.tracking.changelogDir, filename);
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

// State Manager
class StateManager {
  constructor() {
    this.statePath = config.tracking.stateFile;
    this.state = this.loadState();
  }

  loadState() {
    try {
      if (fs.existsSync(this.statePath)) {
        const data = fs.readFileSync(this.statePath, 'utf8');
        return JSON.parse(data);
      }
    } catch (error) {
      console.error('Error loading state:', error);
    }
    
    // Default initial state
    return {
      lastRun: null,
      dimensions: {
        address: { lastProcessedUpdate: null },
        suburb: { lastProcessedUpdate: null },
        council: { lastProcessedUpdate: null },
        metropolitan: { lastProcessedUpdate: null },
        property_type: { lastProcessedUpdate: null },
        method: { lastProcessedUpdate: null },
        date: { lastProcessedUpdate: null }
      },
      facts: {
        transactions: { lastProcessedUpdate: null },
        property_month_performance: { lastProcessedUpdate: null }
      }
    };
  }

  saveState() {
    this.state.lastRun = new Date().toISOString();
    fs.writeFileSync(this.statePath, JSON.stringify(this.state, null, 2), 'utf8');
  }

  getDimensionState(dimensionName) {
    return this.state.dimensions[dimensionName] || { lastProcessedUpdate: null };
  }

  updateDimensionState(dimensionName, newState) {
    this.state.dimensions[dimensionName] = {
      ...this.getDimensionState(dimensionName),
      ...newState
    };
  }

  getFactState(factName) {
    return this.state.facts[factName] || { lastProcessedUpdate: null };
  }

  updateFactState(factName, newState) {
    this.state.facts[factName] = {
      ...this.getFactState(factName),
      ...newState
    };
  }
}

// Base class for incremental dimension updating
class DimensionUpdater {
  constructor(dimensionName, options = {}) {
    this.dimensionName = dimensionName;
    this.tableName = options.tableName || `dim_${dimensionName}`;
    this.type = options.type || 'Type 1'; // 'Type 1' or 'Type 2'
    this.naturalKey = options.naturalKey || ['name'];
    this.surrogateKey = options.surrogateKey || `${dimensionName}_id`;
    this.logger = new Logger(`${dimensionName}_updater.log`);
    this.stateManager = new StateManager();
    this.batchSize = config.tracking.batchSize;
    this.errorLog = [];
  }

  async processChanges(changes) {
    this.logger.log(`Processing changes for ${this.dimensionName}: ${changes.inserts.length} inserts, ${changes.updates.length} updates, ${changes.deletes.length} deletes`);
    
    try {
      // Process in batches
      await this.processInserts(changes.inserts);
      
      if (this.type === 'Type 1') {
        await this.processType1Updates(changes.updates);
      } else if (this.type === 'Type 2') {
        await this.processType2Updates(changes.updates);
      }
      
      // Update state
      this.stateManager.updateDimensionState(this.dimensionName, {
        lastProcessedUpdate: new Date().toISOString()
      });
      this.stateManager.saveState();
      
      // Return summary
      return {
        inserts: changes.inserts.length,
        updates: changes.updates.length,
        errors: this.errorLog.length
      };
    } catch (error) {
      this.logger.error(`Error processing changes for ${this.dimensionName}`, error);
      throw error;
    }
  }

  async processInserts(inserts) {
    if (inserts.length === 0) return;
    this.logger.log(`Processing ${inserts.length} inserts for ${this.dimensionName}`);
    
    // Process in batches
    for (let i = 0; i < inserts.length; i += this.batchSize) {
      const batch = inserts.slice(i, i + this.batchSize);
      await this.processBatchInserts(batch);
      this.logger.log(`Processed ${Math.min(i + this.batchSize, inserts.length)}/${inserts.length} inserts`);
    }
  }

  async processType1Updates(updates) {
    if (updates.length === 0) return;
    this.logger.log(`Processing ${updates.length} Type 1 updates for ${this.dimensionName}`);
    
    // Process in batches
    for (let i = 0; i < updates.length; i += this.batchSize) {
      const batch = updates.slice(i, i + this.batchSize);
      await this.processBatchType1Updates(batch);
      this.logger.log(`Processed ${Math.min(i + this.batchSize, updates.length)}/${updates.length} updates`);
    }
  }

  async processType2Updates(updates) {
    if (updates.length === 0) return;
    this.logger.log(`Processing ${updates.length} Type 2 updates for ${this.dimensionName}`);
    
    // Process in batches
    for (let i = 0; i < updates.length; i += this.batchSize) {
      const batch = updates.slice(i, i + this.batchSize);
      await this.processBatchType2Updates(batch);
      this.logger.log(`Processed ${Math.min(i + this.batchSize, updates.length)}/${updates.length} updates`);
    }
  }

  // Implementations to be provided by subclasses
  async processBatchInserts(batch) {
    throw new Error('Method processBatchInserts must be implemented by subclasses');
  }

  async processBatchType1Updates(batch) {
    throw new Error('Method processBatchType1Updates must be implemented by subclasses');
  }

  async processBatchType2Updates(batch) {
    throw new Error('Method processBatchType2Updates must be implemented by subclasses');
  }

  addError(record, error) {
    this.errorLog.push({
      timestamp: new Date().toISOString(),
      record,
      error: error.message
    });
    this.logger.error(`Error processing record: ${JSON.stringify(record)}`, error);
  }

  async saveErrorLog() {
    const errorLogPath = path.join(
      config.tracking.changelogDir, 
      `${this.dimensionName}_errors_${new Date().toISOString().replace(/[:.]/g, '_')}.json`
    );
    
    fs.writeFileSync(errorLogPath, JSON.stringify(this.errorLog, null, 2), 'utf8');
    this.logger.log(`Errors saved to ${errorLogPath}`);
  }
}

// Address dimension updater (SCD Type 2 example)
class AddressUpdater extends DimensionUpdater {
  constructor() {
    super('address', {
      tableName: 'dim_address',
      type: 'Type 2',
      naturalKey: ['suburb_id', 'street_number', 'street_name'],
      surrogateKey: 'address_id'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const address of batch) {
        try {
          // Get suburb_id from OLAP by matching hierarchy
          const suburbQuery = `
            SELECT s.suburb_id 
            FROM dim_suburb s
            JOIN dim_council c ON s.council_id = c.council_id
            JOIN dim_metropolitan m ON c.metropolitan_id = m.metropolitan_id
            WHERE s.suburb_name = $1 AND c.council_name = $2 AND m.metropolitan_name = $3
          `;
          
          const suburbResult = await client.query(suburbQuery, [
            address.suburb || 'Unknown',
            address.council_area || 'Unknown',
            address.region_name || 'Unknown'
          ]);
          
          if (suburbResult.rows.length === 0) {
            throw new Error(`Could not find matching suburb for address: ${address.address}`);
          }
          
          const suburbId = suburbResult.rows[0].suburb_id;
          
          // Parse address components
          const streetParts = this.parseAddress(address.address);
          
          // Insert new address
          const insertQuery = `
            INSERT INTO dim_address (
              suburb_id, street_number, street_name, is_current
            )
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (suburb_id, street_number, street_name)
            DO NOTHING
            RETURNING address_id
          `;
          
          await client.query(insertQuery, [
            suburbId,
            streetParts.number || '',
            streetParts.street || ''
          ]);
          
        } catch (error) {
          this.addError(address, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType2Updates(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const address of batch) {
        try {
          // Get suburb_id from OLAP by matching hierarchy
          const suburbQuery = `
            SELECT s.suburb_id 
            FROM dim_suburb s
            JOIN dim_council c ON s.council_id = c.council_id
            JOIN dim_metropolitan m ON c.metropolitan_id = m.metropolitan_id
            WHERE s.suburb_name = $1 AND c.council_name = $2 AND m.metropolitan_name = $3
          `;
          
          const suburbResult = await client.query(suburbQuery, [
            address.suburb || 'Unknown',
            address.council_area || 'Unknown',
            address.region_name || 'Unknown'
          ]);
          
          if (suburbResult.rows.length === 0) {
            throw new Error(`Could not find matching suburb for address: ${address.address}`);
          }
          
          const suburbId = suburbResult.rows[0].suburb_id;
          
          // Parse address components
          const streetParts = this.parseAddress(address.address);
          
          // Find current version of this address
          const findQuery = `
            SELECT address_id
            FROM dim_address
            WHERE suburb_id = $1 AND street_number = $2 AND street_name = $3 AND is_current = TRUE
          `;
          
          const findResult = await client.query(findQuery, [
            suburbId,
            streetParts.number || '',
            streetParts.street || ''
          ]);
          
          if (findResult.rows.length > 0) {
            const currentAddressId = findResult.rows[0].address_id;
            
            // Expire current version
            await client.query(
              'UPDATE dim_address SET is_current = FALSE WHERE address_id = $1',
              [currentAddressId]
            );
            
            // Insert new version
            const insertQuery = `
              INSERT INTO dim_address (
                suburb_id, street_number, street_name, is_current, previous_address_id
              )
              VALUES ($1, $2, $3, TRUE, $4)
              RETURNING address_id
            `;
            
            await client.query(insertQuery, [
              suburbId,
              streetParts.number || '',
              streetParts.street || '',
              currentAddressId
            ]);
          } else {
            // Insert as new if not found
            const insertQuery = `
              INSERT INTO dim_address (
                suburb_id, street_number, street_name, is_current
              )
              VALUES ($1, $2, $3, TRUE)
              RETURNING address_id
            `;
            
            await client.query(insertQuery, [
              suburbId,
              streetParts.number || '',
              streetParts.street || ''
            ]);
          }
          
        } catch (error) {
          this.addError(address, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    // For Type 2 dimensions, we implement SCD Type 2 logic instead
    return this.processBatchType2Updates(batch);
  }
  
  parseAddress(address) {
    if (!address) return { number: '', street: '' };
    
    // Simple parsing logic - can be improved for real-world addresses
    const match = address.match(/^(\d+[a-zA-Z]?)\s+(.+)$/);
    if (match) {
      return {
        number: match[1],
        street: match[2]
      };
    }
    return {
      number: '',
      street: address
    };
  }
}

// Property Type dimension updater (SCD Type 1 example)
class PropertyTypeUpdater extends DimensionUpdater {
  constructor() {
    super('property_type', {
      tableName: 'dim_property_type',
      type: 'Type 1',
      naturalKey: ['property_type_name'],
      surrogateKey: 'property_type_id'
    });
    
    // Map property type codes to names
    this.typeMap = {
      'h': 'House',
      'u': 'Unit',
      't': 'Townhouse',
      'd': 'Duplex',
      'v': 'Villa',
      'l': 'Land',
      'a': 'Apartment',
      'r': 'Rural',
      'o': 'Other'
    };
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      // Get unique property types from batch
      const uniqueTypes = [...new Set(batch.map(f => f.type))];
      
      for (const type of uniqueTypes) {
        try {
          const typeName = this.typeMap[type.toLowerCase()] || 'Unknown';
          
          // Insert property type if it doesn't exist
          await client.query(
            'INSERT INTO dim_property_type (property_type_name) VALUES ($1) ON CONFLICT (property_type_name) DO NOTHING',
            [typeName]
          );
        } catch (error) {
          this.addError({ type }, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    // For this dimension, Type 1 updates would be the same as inserts
    // since we're using ON CONFLICT DO NOTHING
    return this.processBatchInserts(batch);
  }
  
  async processBatchType2Updates(batch) {
    // This is a Type 1 dimension, so we don't implement Type 2 logic
    return this.processBatchType1Updates(batch);
  }
}

// Base class for incremental fact table updating
class FactTableUpdater {
  constructor(factName, options = {}) {
    this.factName = factName;
    this.tableName = options.tableName || `fact_${factName}`;
    this.logger = new Logger(`${factName}_updater.log`);
    this.stateManager = new StateManager();
    this.batchSize = config.tracking.batchSize;
    this.errorLog = [];
  }

  async processChanges(changes) {
    this.logger.log(`Processing changes for ${this.factName}: ${changes.inserts.length} inserts, ${changes.updates.length} updates, ${changes.deletes.length} deletes`);
    
    try {
      // Process in batches
      await this.processInserts(changes.inserts);
      await this.processUpdates(changes.updates);
      
      // Update state
      this.stateManager.updateFactState(this.factName, {
        lastProcessedUpdate: new Date().toISOString()
      });
      this.stateManager.saveState();
      
      // Return summary
      return {
        inserts: changes.inserts.length,
        updates: changes.updates.length,
        errors: this.errorLog.length
      };
    } catch (error) {
      this.logger.error(`Error processing changes for ${this.factName}`, error);
      throw error;
    } finally {
      if (this.errorLog.length > 0) {
        await this.saveErrorLog();
      }
    }
  }

  async processInserts(inserts) {
    if (inserts.length === 0) return;
    this.logger.log(`Processing ${inserts.length} inserts for ${this.factName}`);
    
    // Process in batches
    for (let i = 0; i < inserts.length; i += this.batchSize) {
      const batch = inserts.slice(i, i + this.batchSize);
      await this.processBatchInserts(batch);
      this.logger.log(`Processed ${Math.min(i + this.batchSize, inserts.length)}/${inserts.length} inserts`);
    }
  }

  async processUpdates(updates) {
    if (updates.length === 0) return;
    this.logger.log(`Processing ${updates.length} updates for ${this.factName}`);
    
    // Process in batches
    for (let i = 0; i < updates.length; i += this.batchSize) {
      const batch = updates.slice(i, i + this.batchSize);
      await this.processBatchUpdates(batch);
      this.logger.log(`Processed ${Math.min(i + this.batchSize, updates.length)}/${updates.length} updates`);
    }
  }

  // Implementations to be provided by subclasses
  async processBatchInserts(batch) {
    throw new Error('Method processBatchInserts must be implemented by subclasses');
  }

  async processBatchUpdates(batch) {
    throw new Error('Method processBatchUpdates must be implemented by subclasses');
  }

  addError(record, error) {
    this.errorLog.push({
      timestamp: new Date().toISOString(),
      record,
      error: error.message
    });
    this.logger.error(`Error processing record: ${JSON.stringify(record)}`, error);
  }

  async saveErrorLog() {
    const errorLogPath = path.join(
      config.tracking.changelogDir, 
      `${this.factName}_errors_${new Date().toISOString().replace(/[:.]/g, '_')}.json`
    );
    
    fs.writeFileSync(errorLogPath, JSON.stringify(this.errorLog, null, 2), 'utf8');
    this.logger.log(`Errors saved to ${errorLogPath}`);
  }
}

// Transactions fact table updater
class TransactionsUpdater extends FactTableUpdater {
  constructor() {
    super('transactions', {
      tableName: 'fact_transactions'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const transaction of batch) {
        try {
          // 1. Get the date_id from dim_date
          const dateQuery = `
            SELECT date_id FROM dim_date WHERE full_date = $1
          `;
          const dateResult = await client.query(dateQuery, [transaction.sale_date]);
          
          if (dateResult.rows.length === 0) {
            throw new Error(`Date not found in dim_date: ${transaction.sale_date}`);
          }
          const dateId = dateResult.rows[0].date_id;
          
          // 2. Get the address_id from dim_address
          // Parse address components
          const streetParts = this.parseAddress(transaction.address);
          
          const addressQuery = `
            SELECT a.address_id 
            FROM dim_address a
            JOIN dim_suburb s ON a.suburb_id = s.suburb_id
            JOIN dim_council c ON s.council_id = c.council_id
            JOIN dim_metropolitan m ON c.metropolitan_id = m.metropolitan_id
            WHERE s.suburb_name = $1
            AND a.street_number = $2
            AND a.street_name = $3
            AND a.is_current = TRUE
          `;
          
          const addressResult = await client.query(addressQuery, [
            transaction.suburb || 'Unknown',
            streetParts.number || '',
            streetParts.street || ''
          ]);
          
          if (addressResult.rows.length === 0) {
            throw new Error(`Address not found in dim_address: ${transaction.address}, ${transaction.suburb}`);
          }
          const addressId = addressResult.rows[0].address_id;
          
          // 3. Get property_type_id from dim_property_type
          const typeMap = {
            'h': 'House',
            'u': 'Unit',
            't': 'Townhouse',
            'd': 'Duplex',
            'v': 'Villa',
            'l': 'Land',
            'a': 'Apartment',
            'r': 'Rural',
            'o': 'Other'
          };
          
          const typeName = typeMap[transaction.type?.toLowerCase()] || 'Unknown';
          
          const typeQuery = `
            SELECT property_type_id FROM dim_property_type WHERE property_type_name = $1
          `;
          const typeResult = await client.query(typeQuery, [typeName]);
          
          if (typeResult.rows.length === 0) {
            throw new Error(`Property type not found: ${typeName}`);
          }
          const propertyTypeId = typeResult.rows[0].property_type_id;
          
          // 4. Get method_id from dim_method
          const methodQuery = `
            SELECT method_id FROM dim_method WHERE method_name = $1
          `;
          const methodResult = await client.query(methodQuery, [transaction.method || 'Unknown']);
          
          if (methodResult.rows.length === 0) {
            throw new Error(`Method not found: ${transaction.method}`);
          }
          const methodId = methodResult.rows[0].method_id;
          
          // 5. Insert the fact record
          const insertQuery = `
            INSERT INTO fact_transactions (
              date_id, address_id, property_type_id, method_id,
              price, year_built, building_area, land_area,
              bathroom_count, bedroom_count, garage_count, distance_to_center
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT DO NOTHING
          `;
          
          await client.query(insertQuery, [
            dateId,
            addressId,
            propertyTypeId,
            methodId,
            transaction.price,
            transaction.year_built,
            transaction.building_area,
            transaction.landsize,
            transaction.bathroom,
            transaction.bedroom,
            transaction.car,
            transaction.distance
          ]);
          
        } catch (error) {
          this.addError(transaction, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchUpdates(batch) {
    // For fact tables, updates are often implemented as delete+insert
    // This is a simplified approach - in a real system, you might want
    // to handle updates differently based on your specific requirements
    
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const transaction of batch) {
        try {
          // First, delete existing fact records for this transaction
          // We'll use OLTP transaction ID as a reference
          const deleteQuery = `
            DELETE FROM fact_transactions
            WHERE transaction_id = $1
          `;
          
          await client.query(deleteQuery, [transaction.id]);
          
          // Then insert as new
          // This is the same as the insert logic
          // In a real implementation, you might want to extract this to avoid duplication
          await this.processBatchInserts([transaction]);
          
        } catch (error) {
          this.addError(transaction, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  parseAddress(address) {
    if (!address) return { number: '', street: '' };
    
    // Simple parsing logic - can be improved for real-world addresses
    const match = address.match(/^(\d+[a-zA-Z]?)\s+(.+)$/);
    if (match) {
      return {
        number: match[1],
        street: match[2]
      };
    }
    return {
      number: '',
      street: address
    };
  }
}

// Monthly property performance fact table updater
class PropertyMonthPerformanceUpdater extends FactTableUpdater {
  constructor() {
    super('property_month_performance', {
      tableName: 'fact_property_month_performance'
    });
  }

  async processBatchInserts(batch) {
    // For this aggregated fact table, we'll recalculate based on the transaction data
    // This is normally done through a separate ETL process, often using SQL aggregation
    
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      // Group transactions by year-month and suburb
      const groupedTransactions = this.groupTransactions(batch);
      
      for (const key in groupedTransactions) {
        try {
          const group = groupedTransactions[key];
          const { yearNum, monthNum, suburbName, propertyTypeName } = group.metadata;
          
          // 1. Get month_id
          const monthQuery = `
            SELECT m.month_id
            FROM dim_month m
            JOIN dim_year y ON m.year_id = y.year_id
            WHERE y.year_num = $1 AND m.month_num = $2
          `;
          
          const monthResult = await client.query(monthQuery, [yearNum, monthNum]);
          
          if (monthResult.rows.length === 0) {
            throw new Error(`Month not found for ${yearNum}-${monthNum}`);
          }
          const monthId = monthResult.rows[0].month_id;
          
          // 2. Get suburb_id
          const suburbQuery = `
            SELECT suburb_id
            FROM dim_suburb
            WHERE suburb_name = $1
          `;
          
          const suburbResult = await client.query(suburbQuery, [suburbName]);
          
          if (suburbResult.rows.length === 0) {
            throw new Error(`Suburb not found: ${suburbName}`);
          }
          const suburbId = suburbResult.rows[0].suburb_id;
          
          // 3. Get property_type_id
          const typeQuery = `
            SELECT property_type_id
            FROM dim_property_type
            WHERE property_type_name = $1
          `;
          
          const typeResult = await client.query(typeQuery, [propertyTypeName]);
          
          if (typeResult.rows.length === 0) {
            throw new Error(`Property type not found: ${propertyTypeName}`);
          }
          const propertyTypeId = typeResult.rows[0].property_type_id;
          
          // 4. Calculate aggregates
          const transactions = group.transactions;
          const totalSales = transactions.length;
          
          // Only proceed if there are transactions
          if (totalSales > 0) {
            const prices = transactions.map(t => parseFloat(t.price)).filter(p => !isNaN(p));
            const landAreas = transactions.map(t => parseFloat(t.landsize)).filter(a => !isNaN(a));
            const buildingAges = transactions
              .filter(t => t.year_built && !isNaN(parseInt(t.year_built)))
              .map(t => new Date(group.metadata.year, 0).getFullYear() - parseInt(t.year_built));
            
            const avgPrice = prices.length > 0 ? prices.reduce((sum, p) => sum + p, 0) / prices.length : 0;
            const medianPrice = this.calculateMedian(prices);
            const avgLandArea = landAreas.length > 0 ? landAreas.reduce((sum, a) => sum + a, 0) / landAreas.length : 0;
            const avgBuildingAge = buildingAges.length > 0 ? buildingAges.reduce((sum, a) => sum + a, 0) / buildingAges.length : 0;
            
           // Continuing from where the code cut off in the PropertyMonthPerformanceUpdater class:

            // Calculate price per sqm for properties with both price and building area
            const pricesPerSqm = transactions
              .filter(t => parseFloat(t.price) > 0 && parseFloat(t.building_area) > 0)
              .map(t => parseFloat(t.price) / parseFloat(t.building_area));
            
            const avgPricePerSqm = pricesPerSqm.length > 0 ? 
              pricesPerSqm.reduce((sum, p) => sum + p, 0) / pricesPerSqm.length : 0;
            
            // 5. Insert or update the fact record
            const upsertQuery = `
              INSERT INTO fact_property_month_performance (
                month_id, suburb_id, property_type_id, 
                total_sales, avg_price, median_price, 
                avg_land_area, avg_building_age, avg_price_per_sqm
              )
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
              ON CONFLICT (month_id, suburb_id, property_type_id) 
              DO UPDATE SET
                total_sales = $4,
                avg_price = $5,
                median_price = $6,
                avg_land_area = $7,
                avg_building_age = $8,
                avg_price_per_sqm = $9
            `;
            
            await client.query(upsertQuery, [
              monthId,
              suburbId,
              propertyTypeId,
              totalSales,
              avgPrice,
              medianPrice,
              avgLandArea,
              avgBuildingAge,
              avgPricePerSqm
            ]);
          }
        } catch (error) {
          this.addError(groupedTransactions[key], error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchUpdates(batch) {
    // For this aggregated fact table, updates are handled by recalculating
    // the entire aggregate for the affected periods and locations
    return this.processBatchInserts(batch);
  }
  
  groupTransactions(transactions) {
    const groups = {};
    
    for (const transaction of transactions) {
      try {
        const saleDate = new Date(transaction.sale_date);
        const yearNum = saleDate.getFullYear();
        const monthNum = saleDate.getMonth() + 1; // JavaScript months are 0-11
        const suburbName = transaction.suburb || 'Unknown';
        
        // Map property type codes to names
        const typeMap = {
          'h': 'House',
          'u': 'Unit',
          't': 'Townhouse',
          'd': 'Duplex',
          'v': 'Villa',
          'l': 'Land',
          'a': 'Apartment',
          'r': 'Rural',
          'o': 'Other'
        };
        
        const propertyTypeName = typeMap[transaction.type?.toLowerCase()] || 'Unknown';
        
        // Create a key for grouping
        const key = `${yearNum}-${monthNum}-${suburbName}-${propertyTypeName}`;
        
        if (!groups[key]) {
          groups[key] = {
            metadata: { yearNum, monthNum, suburbName, propertyTypeName },
            transactions: []
          };
        }
        
        groups[key].transactions.push(transaction);
      } catch (error) {
        this.addError(transaction, error);
      }
    }
    
    return groups;
  }
  
  calculateMedian(values) {
    if (!values.length) return 0;
    
    // Sort values
    const sorted = [...values].sort((a, b) => a - b);
    
    const mid = Math.floor(sorted.length / 2);
    
    // If even length, average the two middle values
    if (sorted.length % 2 === 0) {
      return (sorted[mid - 1] + sorted[mid]) / 2;
    }
    
    // If odd length, return the middle value
    return sorted[mid];
  }
}

// Main function to orchestrate the incremental updates
async function executeIncrementalUpdate() {
  const mainLogger = new Logger('incremental_update.log');
  const stateManager = new StateManager();
  
  try {
    mainLogger.log('Starting incremental update process');
    
    // Initialize change tracking service
    const changeTracker = new ChangeTrackingService(oltpPool);
    
    // Get last run timestamp from state
    const lastRunTime = stateManager.state.lastRun ? new Date(stateManager.state.lastRun) : null;
    
    // Process dimensions first (in the correct hierarchical order)
    mainLogger.log('Processing dimensions');
    
    // Geographic hierarchy
    const metropolitanTracker = changeTracker.trackTable(
      'metropolitan_areas',
      ['id', 'name', 'state', 'country'],
      'metropolitan',
      lastRunTime
    );
    
    const councilTracker = changeTracker.trackTable(
      'council_areas',
      ['id', 'name', 'metropolitan_id'],
      'council',
      lastRunTime
    );
    
    const suburbTracker = changeTracker.trackTable(
      'suburbs',
      ['id', 'name', 'council_id', 'postcode'],
      'suburb',
      lastRunTime
    );
    
    const addressTracker = changeTracker.trackTable(
      'addresses',
      ['id', 'address', 'suburb_id'],
      'address',
      lastRunTime,
      CHANGE_DETECTION.TIMESTAMP // Using timestamp-based change detection
    );
    
    // Other dimensions
    const propertyTypeTracker = changeTracker.trackTable(
      'property_types',
      ['id', 'type'],
      'property_type',
      lastRunTime
    );
    
    const methodTracker = changeTracker.trackTable(
      'sale_methods',
      ['id', 'name', 'description'],
      'method',
      lastRunTime
    );
    
    const dateTracker = changeTracker.trackTable(
      'dates',
      ['date_id', 'full_date', 'day_of_week', 'month_num', 'year_num'],
      'date',
      lastRunTime
    );
    
    // Update dimensions
    // Respecting hierarchical dependencies in the correct order
    
    // Metropolitan areas (top level)
    const metropolitanUpdater = new MetropolitanUpdater();
    const metropolitanChanges = await metropolitanTracker.getChanges();
    const metropolitanResults = await metropolitanUpdater.processChanges(metropolitanChanges);
    mainLogger.log(`Metropolitan updates: ${JSON.stringify(metropolitanResults)}`);
    
    // Council areas (depends on metropolitan)
    const councilUpdater = new CouncilUpdater();
    const councilChanges = await councilTracker.getChanges();
    const councilResults = await councilUpdater.processChanges(councilChanges);
    mainLogger.log(`Council updates: ${JSON.stringify(councilResults)}`);
    
    // Suburbs (depends on council)
    const suburbUpdater = new SuburbUpdater();
    const suburbChanges = await suburbTracker.getChanges();
    const suburbResults = await suburbUpdater.processChanges(suburbChanges);
    mainLogger.log(`Suburb updates: ${JSON.stringify(suburbResults)}`);
    
    // Addresses (depends on suburb)
    const addressUpdater = new AddressUpdater();
    const addressChanges = await addressTracker.getChanges();
    const addressResults = await addressUpdater.processChanges(addressChanges);
    mainLogger.log(`Address updates: ${JSON.stringify(addressResults)}`);
    
    // Other independent dimensions
    const propertyTypeUpdater = new PropertyTypeUpdater();
    const propertyTypeChanges = await propertyTypeTracker.getChanges();
    const propertyTypeResults = await propertyTypeUpdater.processChanges(propertyTypeChanges);
    mainLogger.log(`Property type updates: ${JSON.stringify(propertyTypeResults)}`);
    
    const methodUpdater = new MethodUpdater();
    const methodChanges = await methodTracker.getChanges();
    const methodResults = await methodUpdater.processChanges(methodChanges);
    mainLogger.log(`Method updates: ${JSON.stringify(methodResults)}`);
    
    const dateUpdater = new DateUpdater();
    const dateChanges = await dateTracker.getChanges();
    const dateResults = await dateUpdater.processChanges(dateChanges);
    mainLogger.log(`Date updates: ${JSON.stringify(dateResults)}`);
    
    // Process fact tables
    mainLogger.log('Processing fact tables');
    
    // Track changes to transaction data
    const transactionTracker = changeTracker.trackTable(
      'property_transactions',
      [
        'id', 'address', 'suburb', 'price', 'sale_date', 'type',
        'method', 'year_built', 'building_area', 'landsize',
        'bathroom', 'bedroom', 'car', 'distance'
      ],
      'transactions',
      lastRunTime,
      CHANGE_DETECTION.TIMESTAMP
    );
    
    // Update transaction facts
    const transactionUpdater = new TransactionsUpdater();
    const transactionChanges = await transactionTracker.getChanges();
    const transactionResults = await transactionUpdater.processChanges(transactionChanges);
    mainLogger.log(`Transaction updates: ${JSON.stringify(transactionResults)}`);
    
    // Update monthly performance facts (derived from transactions)
    // For this, we'll use the same changes as for transactions
    const performanceUpdater = new PropertyMonthPerformanceUpdater();
    const performanceResults = await performanceUpdater.processChanges(transactionChanges);
    mainLogger.log(`Monthly performance updates: ${JSON.stringify(performanceResults)}`);
    
    // Update state
    stateManager.saveState();
    
    mainLogger.log('Incremental update process completed successfully');
    return {
      dimensions: {
        metropolitan: metropolitanResults,
        council: councilResults,
        suburb: suburbResults,
        address: addressResults,
        property_type: propertyTypeResults,
        method: methodResults,
        date: dateResults
      },
      facts: {
        transactions: transactionResults,
        property_month_performance: performanceResults
      }
    };
  } catch (error) {
    mainLogger.error('Fatal error in incremental update process', error);
    throw error;
  } finally {
    mainLogger.close();
  }
}

// Missing dimension updater implementations

// Metropolitan dimension updater
class MetropolitanUpdater extends DimensionUpdater {
  constructor() {
    super('metropolitan', {
      tableName: 'dim_metropolitan',
      type: 'Type 1',
      naturalKey: ['metropolitan_name'],
      surrogateKey: 'metropolitan_id'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const metropolitan of batch) {
        try {
          // Insert metropolitan
          const insertQuery = `
            INSERT INTO dim_metropolitan (metropolitan_name, state, country)
            VALUES ($1, $2, $3)
            ON CONFLICT (metropolitan_name) DO NOTHING
            RETURNING metropolitan_id
          `;
          
          await client.query(insertQuery, [
            metropolitan.name || 'Unknown',
            metropolitan.state || 'Unknown',
            metropolitan.country || 'Australia'
          ]);
          
        } catch (error) {
          this.addError(metropolitan, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const metropolitan of batch) {
        try {
          // Update metropolitan with Type 1 changes
          const updateQuery = `
            UPDATE dim_metropolitan
            SET state = $2, country = $3
            WHERE metropolitan_name = $1
          `;
          
          await client.query(updateQuery, [
            metropolitan.name || 'Unknown',
            metropolitan.state || 'Unknown',
            metropolitan.country || 'Australia'
          ]);
          
        } catch (error) {
          this.addError(metropolitan, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  async processBatchType2Updates(batch) {
    // This is a Type 1 dimension, so we don't implement Type 2 logic
    return this.processBatchType1Updates(batch);
  }
}

// Council dimension updater
class CouncilUpdater extends DimensionUpdater {
  constructor() {
    super('council', {
      tableName: 'dim_council',
      type: 'Type 1',
      naturalKey: ['council_name'],
      surrogateKey: 'council_id'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const council of batch) {
        try {
          // Get metropolitan_id from OLAP
          const metroQuery = `
            SELECT metropolitan_id 
            FROM dim_metropolitan
            WHERE metropolitan_name = (
              SELECT name FROM metropolitan_areas WHERE id = $1
            )
          `;
          
          const metroResult = await client.query(metroQuery, [council.metropolitan_id]);
          
          if (metroResult.rows.length === 0) {
            throw new Error(`Metropolitan area not found for ID: ${council.metropolitan_id}`);
          }
          
          const metropolitanId = metroResult.rows[0].metropolitan_id;
          
          // Insert council
          const insertQuery = `
            INSERT INTO dim_council (council_name, metropolitan_id)
            VALUES ($1, $2)
            ON CONFLICT (council_name) DO NOTHING
            RETURNING council_id
          `;
          
          await client.query(insertQuery, [
            council.name || 'Unknown',
            metropolitanId
          ]);
          
        } catch (error) {
          this.addError(council, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const council of batch) {
        try {
          // Get metropolitan_id from OLAP
          const metroQuery = `
            SELECT metropolitan_id 
            FROM dim_metropolitan
            WHERE metropolitan_name = (
              SELECT name FROM metropolitan_areas WHERE id = $1
            )
          `;
          
          const metroResult = await client.query(metroQuery, [council.metropolitan_id]);
          
          if (metroResult.rows.length === 0) {
            throw new Error(`Metropolitan area not found for ID: ${council.metropolitan_id}`);
          }
          
          const metropolitanId = metroResult.rows[0].metropolitan_id;
          
          // Update council
          const updateQuery = `
            UPDATE dim_council
            SET metropolitan_id = $2
            WHERE council_name = $1
          `;
          
          await client.query(updateQuery, [
            council.name || 'Unknown',
            metropolitanId
          ]);
          
        } catch (error) {
          this.addError(council, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  async processBatchType2Updates(batch) {
    // This is a Type 1 dimension, so we don't implement Type 2 logic
    return this.processBatchType1Updates(batch);
  }
}

// Suburb dimension updater
class SuburbUpdater extends DimensionUpdater {
  constructor() {
    super('suburb', {
      tableName: 'dim_suburb',
      type: 'Type 1',
      naturalKey: ['suburb_name', 'postcode'],
      surrogateKey: 'suburb_id'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const suburb of batch) {
        try {
          // Get council_id from OLAP
          const councilQuery = `
            SELECT council_id 
            FROM dim_council
            WHERE council_name = (
              SELECT name FROM council_areas WHERE id = $1
            )
          `;
          
          const councilResult = await client.query(councilQuery, [suburb.council_id]);
          
          if (councilResult.rows.length === 0) {
            throw new Error(`Council area not found for ID: ${suburb.council_id}`);
          }
          
          const councilId = councilResult.rows[0].council_id;
          
          // Insert suburb
          const insertQuery = `
            INSERT INTO dim_suburb (suburb_name, postcode, council_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (suburb_name, postcode) DO NOTHING
            RETURNING suburb_id
          `;
          
          await client.query(insertQuery, [
            suburb.name || 'Unknown',
            suburb.postcode || '0000',
            councilId
          ]);
          
        } catch (error) {
          this.addError(suburb, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const suburb of batch) {
        try {
          // Get council_id from OLAP
          const councilQuery = `
            SELECT council_id 
            FROM dim_council
            WHERE council_name = (
              SELECT name FROM council_areas WHERE id = $1
            )
          `;
          
          const councilResult = await client.query(councilQuery, [suburb.council_id]);
          
          if (councilResult.rows.length === 0) {
            throw new Error(`Council area not found for ID: ${suburb.council_id}`);
          }
          
          const councilId = councilResult.rows[0].council_id;
          
          // Update suburb
          const updateQuery = `
            UPDATE dim_suburb
            SET council_id = $3
            WHERE suburb_name = $1 AND postcode = $2
          `;
          
          await client.query(updateQuery, [
            suburb.name || 'Unknown',
            suburb.postcode || '0000',
            councilId
          ]);
          
        } catch (error) {
          this.addError(suburb, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  async processBatchType2Updates(batch) {
    // This is a Type 1 dimension, so we don't implement Type 2 logic
    return this.processBatchType1Updates(batch);
  }
}

// Method dimension updater
class MethodUpdater extends DimensionUpdater {
  constructor() {
    super('method', {
      tableName: 'dim_method',
      type: 'Type 1',
      naturalKey: ['method_name'],
      surrogateKey: 'method_id'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const method of batch) {
        try {
          // Insert method
          const insertQuery = `
            INSERT INTO dim_method (method_name, description)
            VALUES ($1, $2)
            ON CONFLICT (method_name) DO NOTHING
            RETURNING method_id
          `;
          
          await client.query(insertQuery, [
            method.name || 'Unknown',
            method.description || ''
          ]);
          
        } catch (error) {
          this.addError(method, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const method of batch) {
        try {
          // Update method
          const updateQuery = `
            UPDATE dim_method
            SET description = $2
            WHERE method_name = $1
          `;
          
          await client.query(updateQuery, [
            method.name || 'Unknown',
            method.description || ''
          ]);
          
        } catch (error) {
          this.addError(method, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  async processBatchType2Updates(batch) {
    // This is a Type 1 dimension, so we don't implement Type 2 logic
    return this.processBatchType1Updates(batch);
  }
}

// Date dimension updater
class DateUpdater extends DimensionUpdater {
  constructor() {
    super('date', {
      tableName: 'dim_date',
      type: 'Type 1',
      naturalKey: ['full_date'],
      surrogateKey: 'date_id'
    });
  }

  async processBatchInserts(batch) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const dateRecord of batch) {
        try {
          // Insert date
          const insertQuery = `
            INSERT INTO dim_date (
              full_date, day_of_week, day_num, month_num, month_name, 
              quarter_num, year_num, is_weekend, is_holiday
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (full_date) DO NOTHING
            RETURNING date_id
          `;
          
          const date = new Date(dateRecord.full_date);
          const dayOfWeek = date.getDay();
          const dayNum = date.getDate();
          const monthNum = date.getMonth() + 1;
          const monthNames = [
            'January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December'
          ];
          const monthName = monthNames[date.getMonth()];
          const quarterNum = Math.ceil(monthNum / 3);
          const yearNum = date.getFullYear();
          const isWeekend = (dayOfWeek === 0 || dayOfWeek === 6);
          const isHoliday = false; // Would need a holiday calendar to determine
          
          await client.query(insertQuery, [
            dateRecord.full_date,
            dayOfWeek,
            dayNum,
            monthNum,
            monthName,
            quarterNum,
            yearNum,
            isWeekend,
            isHoliday
          ]);
          
        } catch (error) {
          this.addError(dateRecord, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async processBatchType1Updates(batch) {
    // For date dimension, updates are rare - typically only to mark holidays
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      for (const dateRecord of batch) {
        try {
          // Update date (only is_holiday would typically change)
          const updateQuery = `
            UPDATE dim_date
            SET is_holiday = $2
            WHERE full_date = $1
          `;
          
          await client.query(updateQuery, [
            dateRecord.full_date,
            dateRecord.is_holiday || false
          ]);
          
        } catch (error) {
          this.addError(dateRecord, error);
        }
      }
      
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  async processBatchType2Updates(batch) {
    // This is a Type 1 dimension, so we don't implement Type 2 logic
    return this.processBatchType1Updates(batch);
  }
}

// Export the main function
module.exports = {
  executeIncrementalUpdate,
  DimensionUpdater,
  FactTableUpdater,
  AddressUpdater,
  PropertyTypeUpdater,
  MetropolitanUpdater,
  CouncilUpdater,
  SuburbUpdater,
  MethodUpdater,
  DateUpdater,
  TransactionsUpdater,
  PropertyMonthPerformanceUpdater
};