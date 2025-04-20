// dimension-loader.js
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
  constructor(dimensionName) {
    this.checkpointPath = path.join(config.checkpointDir, `${dimensionName}_checkpoint.json`);
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

// Base dimension loader class
class DimensionLoader {
  constructor(dimensionName) {
    this.dimensionName = dimensionName;
    this.logger = new Logger(`${dimensionName}_loader.log`);
    this.checkpointManager = new CheckpointManager(dimensionName);
    this.errors = [];
  }

  async load() {
    try {
      this.logger.log(`Starting ${this.dimensionName} dimension load`);
      
      // Check for existing checkpoint
      const checkpoint = this.checkpointManager.loadCheckpoint();
      if (checkpoint) {
        this.logger.log(`Resuming from checkpoint: ${JSON.stringify(checkpoint)}`);
      } else {
        this.logger.log('No checkpoint found, starting fresh load');
      }

      await this.processData(checkpoint);
      
      if (this.errors.length > 0) {
        this.logger.log(`Completed with ${this.errors.length} errors`);
        this.saveErrors();
      } else {
        this.logger.log('Completed successfully');
        this.checkpointManager.clearCheckpoint();
      }
    } catch (error) {
      this.logger.error('Unhandled error during load process', error);
      throw error;
    } finally {
      this.logger.close();
    }
  }

  async processData(checkpoint) {
    throw new Error('Method processData must be implemented by subclasses');
  }

  saveErrors() {
    const errorLogPath = path.join(config.logDir, `${this.dimensionName}_errors.json`);
    fs.writeFileSync(errorLogPath, JSON.stringify(this.errors, null, 2), 'utf8');
    this.logger.log(`Errors saved to ${errorLogPath}`);
  }

  validateRecord(record) {
    // Override in subclasses for specific validation logic
    return { isValid: true };
  }

  async processBatch(records, targetTable, columns) {
    if (records.length === 0) return;
    
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      // Build multi-row insert statement
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
        INSERT INTO ${targetTable} (${columns.join(', ')})
        VALUES ${placeholders.join(', ')}
        ON CONFLICT DO NOTHING
      `;
      
      await client.query(query, values);
      await client.query('COMMIT');
      this.logger.log(`Inserted ${records.length} records into ${targetTable}`);
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(`Error inserting batch into ${targetTable}`, error);
      this.errors.push({
        table: targetTable,
        error: error.message,
        records: records
      });
    } finally {
      client.release();
    }
  }
}

// Metropolitan dimension loader
class MetropolitanLoader extends DimensionLoader {
  constructor() {
    super('metropolitan');
  }

  async processData(checkpoint) {
    const client = await oltpPool.connect();
    try {
      // Get distinct metropolitan areas (region_name in OLTP)
      const result = await client.query(`
        SELECT DISTINCT region_name
        FROM property_location
        WHERE region_name IS NOT NULL
        ORDER BY region_name
      `);
      
      this.logger.log(`Found ${result.rows.length} distinct metropolitan areas`);
      
      // Process in batches
      const batch = [];
      for (const row of result.rows) {
        const validationResult = this.validateRecord(row);
        if (!validationResult.isValid) {
          this.errors.push({
            record: row,
            error: validationResult.error
          });
          continue;
        }
        
        batch.push({
          metropolitan_name: row.region_name
        });
        
        if (batch.length >= config.batchSize) {
          await this.processBatch(batch, 'dim_metropolitan', ['metropolitan_name']);
          batch.length = 0;
        }
      }
      
      // Process remaining records
      if (batch.length > 0) {
        await this.processBatch(batch, 'dim_metropolitan', ['metropolitan_name']);
      }
    } finally {
      client.release();
    }
  }

  validateRecord(record) {
    if (!record.region_name || record.region_name.trim() === '') {
      return { isValid: false, error: 'Region name is empty' };
    }
    return { isValid: true };
  }
}

// Council dimension loader
class CouncilLoader extends DimensionLoader {
  constructor() {
    super('council');
  }

  async processData(checkpoint) {
    const client = await oltpPool.connect();
    try {
      // First, we need to map council areas to their metropolitan areas
      const result = await client.query(`
        SELECT DISTINCT council_area, region_name
        FROM property_location
        WHERE council_area IS NOT NULL AND region_name IS NOT NULL
        ORDER BY region_name, council_area
      `);
      
      this.logger.log(`Found ${result.rows.length} distinct council areas`);
      
      // Get metropolitan ID mapping from OLAP database
      const olapClient = await olapPool.connect();
      try {
        const metroMap = {};
        const metroResult = await olapClient.query('SELECT metropolitan_id, metropolitan_name FROM dim_metropolitan');
        metroResult.rows.forEach(row => {
          metroMap[row.metropolitan_name] = row.metropolitan_id;
        });
        
        // Process in batches
        const batch = [];
        for (const row of result.rows) {
          const validationResult = this.validateRecord(row);
          if (!validationResult.isValid) {
            this.errors.push({
              record: row,
              error: validationResult.error
            });
            continue;
          }
          
          const metropolitanId = metroMap[row.region_name];
          if (!metropolitanId) {
            this.errors.push({
              record: row,
              error: `Metropolitan area "${row.region_name}" not found in dim_metropolitan`
            });
            continue;
          }
          
          batch.push({
            metropolitan_id: metropolitanId,
            council_name: row.council_area
          });
          
          if (batch.length >= config.batchSize) {
            await this.processBatch(batch, 'dim_council', ['metropolitan_id', 'council_name']);
            batch.length = 0;
          }
        }
        
        // Process remaining records
        if (batch.length > 0) {
          await this.processBatch(batch, 'dim_council', ['metropolitan_id', 'council_name']);
        }
      } finally {
        olapClient.release();
      }
    } finally {
      client.release();
    }
  }

  validateRecord(record) {
    if (!record.council_area || record.council_area.trim() === '') {
      return { isValid: false, error: 'Council area is empty' };
    }
    if (!record.region_name || record.region_name.trim() === '') {
      return { isValid: false, error: 'Region name is empty' };
    }
    return { isValid: true };
  }
}

// Suburb dimension loader
class SuburbLoader extends DimensionLoader {
  constructor() {
    super('suburb');
  }

  async processData(checkpoint) {
    const client = await oltpPool.connect();
    try {
      // First, we need to map suburbs to their council areas
      const result = await client.query(`
        SELECT DISTINCT suburb, council_area, region_name
        FROM property_location
        WHERE suburb IS NOT NULL AND council_area IS NOT NULL AND region_name IS NOT NULL
        ORDER BY region_name, council_area, suburb
      `);
      
      this.logger.log(`Found ${result.rows.length} distinct suburbs`);
      
      // Get council ID mapping from OLAP database
      const olapClient = await olapPool.connect();
      try {
        // Build a mapping for councils
        const councilMap = {};
        const councilResult = await olapClient.query(`
          SELECT c.council_id, c.council_name, m.metropolitan_name
          FROM dim_council c
          JOIN dim_metropolitan m ON c.metropolitan_id = m.metropolitan_id
        `);
        
        councilResult.rows.forEach(row => {
          const key = `${row.metropolitan_name}|${row.council_name}`;
          councilMap[key] = row.council_id;
        });
        
        // Process in batches
        const batch = [];
        for (const row of result.rows) {
          const validationResult = this.validateRecord(row);
          if (!validationResult.isValid) {
            this.errors.push({
              record: row,
              error: validationResult.error
            });
            continue;
          }
          
          const key = `${row.region_name}|${row.council_area}`;
          const councilId = councilMap[key];
          if (!councilId) {
            this.errors.push({
              record: row,
              error: `Council "${row.council_area}" in "${row.region_name}" not found in dim_council`
            });
            continue;
          }
          
          batch.push({
            council_id: councilId,
            suburb_name: row.suburb
          });
          
          if (batch.length >= config.batchSize) {
            await this.processBatch(batch, 'dim_suburb', ['council_id', 'suburb_name']);
            batch.length = 0;
          }
        }
        
        // Process remaining records
        if (batch.length > 0) {
          await this.processBatch(batch, 'dim_suburb', ['council_id', 'suburb_name']);
        }
      } finally {
        olapClient.release();
      }
    } finally {
      client.release();
    }
  }

  validateRecord(record) {
    if (!record.suburb || record.suburb.trim() === '') {
      return { isValid: false, error: 'Suburb is empty' };
    }
    if (!record.council_area || record.council_area.trim() === '') {
      return { isValid: false, error: 'Council area is empty' };
    }
    if (!record.region_name || record.region_name.trim() === '') {
      return { isValid: false, error: 'Region name is empty' };
    }
    return { isValid: true };
  }
}

// Address dimension loader
class AddressLoader extends DimensionLoader {
  constructor() {
    super('address');
  }

  async processData(checkpoint) {
    let offset = checkpoint?.offset || 0;
    const client = await oltpPool.connect();

    try {
      const countResult = await client.query('SELECT COUNT(*) FROM property_location');
      const totalCount = parseInt(countResult.rows[0].count);
      this.logger.log(`Total addresses to process: ${totalCount}, starting from offset: ${offset}`);

      let hasMore = true;

      while (hasMore) {
        const result = await client.query(`
          SELECT id, suburb, address, postcode, council_area, region_name
          FROM property_location
          WHERE address IS NOT NULL
          ORDER BY id
          LIMIT $1 OFFSET $2
        `, [config.batchSize, offset]);

        if (result.rows.length === 0) {
          hasMore = false;
          continue;
        }

        this.logger.log(`Processing batch of ${result.rows.length} addresses starting at offset ${offset}`);

        const olapClient = await olapPool.connect();
        try {
          const batch = [];
          for (const row of result.rows) {
            const validationResult = this.validateRecord(row);
            if (!validationResult.isValid) {
              this.errors.push({
                record: row,
                error: validationResult.error
              });
              continue;
            }

            const addressParts = this.parseAddress(row.address);
            if (!addressParts) {
              this.errors.push({
                record: row,
                error: 'Could not parse address'
              });
              continue;
            }

            const suburbResult = await olapClient.query(`
              SELECT s.suburb_id
              FROM dim_suburb s
              JOIN dim_council c ON s.council_id = c.council_id
              JOIN dim_metropolitan m ON c.metropolitan_id = m.metropolitan_id
              WHERE s.suburb_name = $1
                AND c.council_name = $2
                AND m.metropolitan_name = $3
            `, [row.suburb, row.council_area, row.region_name]);

            if (suburbResult.rows.length === 0) {
              this.errors.push({
                record: row,
                error: `Could not find suburb_id for ${row.suburb}, ${row.council_area}, ${row.region_name}`
              });
              continue;
            }

            batch.push({
              suburb_id: suburbResult.rows[0].suburb_id,
              street_number: addressParts.streetNumber,
              street_name: addressParts.streetName,
              is_current: true
            });
          }

          if (batch.length > 0) {
            await this.processBatch(
              batch,
              'dim_address',
              ['suburb_id', 'street_number', 'street_name', 'is_current']
            );
          }
        } finally {
          olapClient.release();
        }

        offset += result.rows.length;
        this.checkpointManager.saveCheckpoint({ offset: offset });

        if (result.rows.length < config.batchSize) {
          hasMore = false;
        }
      }

      this.logger.log(`Completed processing ${offset} addresses`);
    } finally {
      client.release();
    }
  }

  validateRecord(record) {
    if (!record.address || record.address.trim() === '') {
      return { isValid: false, error: 'Address is empty' };
    }
    if (!record.suburb || record.suburb.trim() === '') {
      return { isValid: false, error: 'Suburb is empty' };
    }
    if (!record.council_area || record.council_area.trim() === '') {
      return { isValid: false, error: 'Council area is empty' };
    }
    if (!record.region_name || record.region_name.trim() === '') {
      return { isValid: false, error: 'Region name is empty' };
    }
    return { isValid: true };
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
}


// Property Type dimension loader
class PropertyTypeLoader extends DimensionLoader {
  constructor() {
    super('property_type');
  }

  async processData(checkpoint) {
    const client = await oltpPool.connect();
    try {
      // Get distinct property types
      const result = await client.query(`
        SELECT DISTINCT type
        FROM property_features
        WHERE type IS NOT NULL
        ORDER BY type
      `);
      
      this.logger.log(`Found ${result.rows.length} distinct property types`);
      
      // Process property types
      const batch = [];
      for (const row of result.rows) {
        const validationResult = this.validateRecord(row);
        if (!validationResult.isValid) {
          this.errors.push({
            record: row,
            error: validationResult.error
          });
          continue;
        }
        
        const propertyTypeName = this.mapPropertyType(row.type);
        
        batch.push({
          property_type_name: propertyTypeName
        });
      }
      
      await this.processBatch(batch, 'dim_property_type', ['property_type_name']);
      
    } finally {
      client.release();
    }
  }

  validateRecord(record) {
    if (!record.type || record.type.trim() === '') {
      return { isValid: false, error: 'Property type is empty' };
    }
    return { isValid: true };
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
}

// Method dimension loader
class MethodLoader extends DimensionLoader {
  constructor() {
    super('method');
  }

  async processData(checkpoint) {
    const client = await oltpPool.connect();
    try {
      // Get distinct sale methods
      const result = await client.query(`
        SELECT DISTINCT method
        FROM transactions
        WHERE method IS NOT NULL
        ORDER BY method
      `);
      
      this.logger.log(`Found ${result.rows.length} distinct sale methods`);
      
      // Process methods
      const batch = [];
      for (const row of result.rows) {
        const validationResult = this.validateRecord(row);
        if (!validationResult.isValid) {
          this.errors.push({
            record: row,
            error: validationResult.error
          });
          continue;
        }
        
        batch.push({
          method_name: row.method
        });
      }
      
      await this.processBatch(batch, 'dim_method', ['method_name']);
      
    } finally {
      client.release();
    }
  }

  validateRecord(record) {
    if (!record.method || record.method.trim() === '') {
      return { isValid: false, error: 'Sale method is empty' };
    }
    return { isValid: true };
  }
}

// Date dimensions loader
class DateDimensionsLoader extends DimensionLoader {
  constructor() {
    super('date_dimensions');
  }

  async processData(checkpoint) {
    const client = await oltpPool.connect();
    try {
      // Get min and max dates from transactions
      const result = await client.query(`
        SELECT 
          MIN(sale_date) as min_date,
          MAX(sale_date) as max_date
        FROM transactions
        WHERE sale_date IS NOT NULL
      `);
      
      if (!result.rows[0].min_date || !result.rows[0].max_date) {
        this.logger.log('No valid sale dates found in transactions');
        return;
      }
      
      const minDate = new Date(result.rows[0].min_date);
      const maxDate = new Date(result.rows[0].max_date);
      
      this.logger.log(`Generating date dimensions from ${minDate.toISOString().split('T')[0]} to ${maxDate.toISOString().split('T')[0]}`);
      
      // Generate all years first
      const years = new Set();
      let currentDate = new Date(minDate);
      while (currentDate <= maxDate) {
        years.add(currentDate.getFullYear());
        currentDate.setFullYear(currentDate.getFullYear() + 1);
      }
      
      const yearArray = Array.from(years).sort();
      this.logger.log(`Years to process: ${yearArray.join(', ')}`);
      
      // Load years
      const yearBatch = yearArray.map(year => ({ year_num: year }));
      await this.processBatch(yearBatch, 'dim_year', ['year_num']);
      
      // Get year IDs
      const olapClient = await olapPool.connect();
      try {
        const yearMap = {};
        const yearResult = await olapClient.query('SELECT year_id, year_num FROM dim_year');
        yearResult.rows.forEach(row => {
          yearMap[row.year_num] = row.year_id;
        });
        
        // Load months
        const monthBatch = [];
        const monthNames = ['January', 'February', 'March', 'April', 'May', 'June', 
                           'July', 'August', 'September', 'October', 'November', 'December'];
        
        yearArray.forEach(year => {
          for (let month = 1; month <= 12; month++) {
            monthBatch.push({
              year_id: yearMap[year],
              month_num: month,
              month_name: monthNames[month - 1]
            });
          }
        });
        
        await this.processBatch(monthBatch, 'dim_month', ['year_id', 'month_num', 'month_name']);
        
        // Get month IDs
        const monthMap = {};
        const monthResult = await olapClient.query(`
          SELECT month_id, year_id, month_num 
          FROM dim_month
          ORDER BY year_id, month_num
        `);
        
        monthResult.rows.forEach(row => {
          const key = `${row.year_id}_${row.month_num}`;
          monthMap[key] = row.month_id;
        });
        
        // Generate all dates
        currentDate = new Date(minDate);
        currentDate.setHours(0, 0, 0, 0);
        
        const endDate = new Date(maxDate);
        endDate.setHours(0, 0, 0, 0);
        
        const dateBatch = [];
        
        while (currentDate <= endDate) {
          const year = currentDate.getFullYear();
          const month = currentDate.getMonth() + 1;
          const yearId = yearMap[year];
          const monthId = monthMap[`${yearId}_${month}`];
          
          if (yearId && monthId) {
            dateBatch.push({
              month_id: monthId,
              full_date: new Date(currentDate)
            });
            
            if (dateBatch.length >= config.batchSize) {
              await this.processBatch(dateBatch, 'dim_date', ['month_id', 'full_date']);
              dateBatch.length = 0;
            }
          }
          
          // Move to next day
          currentDate.setDate(currentDate.getDate() + 1);
        }
        
        // Process remaining dates
        if (dateBatch.length > 0) {
          await this.processBatch(dateBatch, 'dim_date', ['month_id', 'full_date']);
        }
        
      } finally {
        olapClient.release();
      }
      
    } finally {
      client.release();
    }
  }
}

// Main ETL execution function
async function runDimensionLoaders() {
  try {
    console.log('Starting OLAP dimension loading process...');
    
    // Order matters due to hierarchical dependencies
    const loaders = [
      new MetropolitanLoader(),
      new CouncilLoader(),
      new SuburbLoader(),
      new AddressLoader(),
      new PropertyTypeLoader(),
      new MethodLoader(),
      new DateDimensionsLoader()
    ];
    
    for (const loader of loaders) {
      console.log(`\n--- Running ${loader.dimensionName} loader ---`);
      await loader.load();
    }
    
    console.log('\nDimension loading process complete!');
  } catch (error) {
    console.error('Error in ETL process:', error);
  } finally {
    // Close connection pools
    await oltpPool.end();
    await olapPool.end();
  }
}

// Entry point
if (require.main === module) {
  runDimensionLoaders().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = {
  DimensionLoader,
  MetropolitanLoader,
  CouncilLoader,
  SuburbLoader,
  AddressLoader,
  PropertyTypeLoader,
  MethodLoader,
  DateDimensionsLoader,
  runDimensionLoaders
};