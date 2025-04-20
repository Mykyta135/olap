// fact-aggregation-updater.js
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// Configuration
const config = {
  olap: {
    host: 'localhost',
    database: 'OLAP',
    user: 'postgres',
    password: '1234',
    port: 5432
  },
  batchSize: 100, // Smaller batch size for complex aggregations
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

// Initialize database connection
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

// Monthly Property Performance Aggregator
class PropertyPerformanceAggregator {
  constructor() {
    this.name = 'fact_property_month_performance';
    this.logger = new Logger(`${this.name}_updater.log`);
    this.checkpointManager = new CheckpointManager(this.name);
    this.errors = [];
    
    // Statistics
    this.stats = {
      total: 0,
      processed: 0,
      inserted: 0,
      updated: 0,
      errors: 0,
      start: null,
      end: null
    };
  }

  async calculateAggregates(startDate, endDate, processType = 'full') {
    // Load checkpoint if exists
    let checkpoint = this.checkpointManager.loadCheckpoint();
    if (checkpoint && checkpoint.startDate === startDate && checkpoint.endDate === endDate) {
      this.logger.log(`Resuming from checkpoint: ${JSON.stringify(checkpoint.progress)}`);
      this.stats = checkpoint.stats || this.stats;
    } else {
      this.logger.log(`Starting new ${processType} aggregation for period: ${startDate} to ${endDate}`);
    }
    
    // Start timing
    this.stats.start = this.stats.start || new Date();
    
    try {
      // Get all months in the date range
      const months = await this.getMonthsInRange(startDate, endDate);
      this.stats.total = months.length;
      this.logger.log(`Found ${months.length} months to process in range`);
      
      // Process each month
      let processedMonths = 0;
      for (const month of months) {
        // Skip if already processed in this run (for resuming)
        if (checkpoint && 
            checkpoint.progress && 
            checkpoint.progress.processedMonths &&
            checkpoint.progress.processedMonths.includes(month.month_id)) {
          this.logger.log(`Skipping already processed month_id: ${month.month_id}`);
          continue;
        }
        
        this.logger.log(`Processing month ${month.year_num}-${month.month_num} (ID: ${month.month_id})`);
        
        if (processType === 'incremental') {
          // For incremental updates, first check if data exists
          const hasData = await this.checkMonthHasData(month.month_id);
          if (hasData) {
            // Update existing records
            await this.updateMonthAggregates(month.month_id);
          } else {
            // Insert new records
            await this.insertMonthAggregates(month.month_id);
          }
        } else {
          // For full processing, delete and recreate
          await this.recreateMonthAggregates(month.month_id);
        }
        
        // Update checkpoint after each month
        processedMonths++;
        this.stats.processed++;
        
        // Save progress
        if (!checkpoint) checkpoint = {};
        if (!checkpoint.progress) checkpoint.progress = {};
        if (!checkpoint.progress.processedMonths) checkpoint.progress.processedMonths = [];
        
        checkpoint.progress.processedMonths.push(month.month_id);
        checkpoint.startDate = startDate;
        checkpoint.endDate = endDate;
        checkpoint.stats = this.stats;
        
        this.checkpointManager.saveCheckpoint(checkpoint);
        
        // Log progress
        const progress = Math.round((processedMonths / months.length) * 100);
        this.logger.log(`Progress: ${progress}% (${processedMonths}/${months.length})`);
      }
      
      // Finalize
      this.stats.end = new Date();
      const duration = (this.stats.end - this.stats.start) / 1000;
      
      this.logger.log(`Completed processing ${processedMonths} months in ${duration.toFixed(2)} seconds`);
      this.logger.log(`Inserted: ${this.stats.inserted}, Updated: ${this.stats.updated}, Errors: ${this.stats.errors}`);
      
      // Save error report
      this.saveErrors();
      
      // Clear checkpoint if complete
      if (processedMonths >= months.length) {
        this.checkpointManager.clearCheckpoint();
      }
      
      return this.stats;
      
    } catch (error) {
      this.logger.error(`Error during aggregate calculation`, error);
      throw error;
    }
  }
  
  async getMonthsInRange(startDate, endDate) {
    const client = await olapPool.connect();
    try {
      const query = `
        SELECT 
          m.month_id,
          m.month_num,
          y.year_num
        FROM 
          dim_date d
          JOIN dim_month m ON d.month_id = m.month_id
          JOIN dim_year y ON m.year_id = y.year_id
        WHERE 
          d.full_date BETWEEN $1 AND $2
        GROUP BY 
          m.month_id, m.month_num, y.year_num
        ORDER BY 
          y.year_num, m.month_num
      `;
      
      const result = await client.query(query, [startDate, endDate]);
      return result.rows;
    } finally {
      client.release();
    }
  }
  
  async checkMonthHasData(monthId) {
    const client = await olapPool.connect();
    try {
      const query = `
        SELECT COUNT(*) AS count
        FROM fact_property_month_performance
        WHERE month_id = $1
      `;
      
      const result = await client.query(query, [monthId]);
      return result.rows[0].count > 0;
    } finally {
      client.release();
    }
  }
  
  async recreateMonthAggregates(monthId) {
    const client = await olapPool.connect();
    try {
      // First delete existing records for this month
      const deleteQuery = `
        DELETE FROM fact_property_month_performance
        WHERE month_id = $1
      `;
      
      const deleteResult = await client.query(deleteQuery, [monthId]);
      if (deleteResult.rowCount > 0) {
        this.logger.log(`Deleted ${deleteResult.rowCount} existing aggregates for month ID: ${monthId}`);
      }
      
      // Then insert new records
      await this.insertMonthAggregates(monthId);
      
    } catch (error) {
      this.logger.error(`Error recreating aggregates for month ID: ${monthId}`, error);
      this.errors.push({
        month_id: monthId,
        operation: 'recreate',
        error: error.message
      });
      this.stats.errors++;
    } finally {
      client.release();
    }
  }
  
  async insertMonthAggregates(monthId) {
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      // Get the first and last day of the month
      const dateRangeQuery = `
        SELECT 
          MIN(d.full_date) AS start_date,
          MAX(d.full_date) AS end_date
        FROM 
          dim_date d
        WHERE 
          d.month_id = $1
      `;
      
      const dateRangeResult = await client.query(dateRangeQuery, [monthId]);
      const startDate = dateRangeResult.rows[0].start_date;
      const endDate = dateRangeResult.rows[0].end_date;
      
      // Calculate aggregates by suburb and property type
      const aggregateQuery = `
        WITH transaction_data AS (
          SELECT 
            s.suburb_id,
            ft.property_type_id,
            ft.price,
            ft.building_area,
            ft.land_area,
            ft.year_built,
            EXTRACT(YEAR FROM dd.full_date) - ft.year_built AS building_age
          FROM 
            fact_transactions ft
            JOIN dim_date dd ON ft.date_id = dd.date_id
            JOIN dim_address da ON ft.address_id = da.address_id
            JOIN dim_suburb s ON da.suburb_id = s.suburb_id
          WHERE 
            dd.full_date BETWEEN $1 AND $2
            AND ft.price > 0  -- Exclude invalid prices
        )
        SELECT 
          s.suburb_id,
          ft.property_type_id,
          
          -- Aggregate calculations
          COUNT(ft.price) AS total_sales,
          AVG(ft.price) AS avg_price,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.price) AS median_price,
          AVG(ft.land_area) AS avg_landsize,
          
          -- Calculate price per sqm only for records with valid building area
          AVG(
            CASE WHEN ft.building_area > 0 
            THEN ft.price / ft.building_area 
            ELSE NULL END
          ) AS avg_price_per_sqm,
          
          -- Calculate land value metrics only for records with valid land area
          AVG(
            CASE WHEN ft.land_area > 0 
            THEN ft.price / ft.land_area 
            ELSE NULL END
          ) AS avg_land_area_value,
          
          -- Calculate building age only for records with valid year_built
          AVG(ft.building_age) AS avg_building_age
          
        FROM 
          transaction_data ft
          JOIN dim_suburb s ON ft.suburb_id = s.suburb_id
        GROUP BY 
          s.suburb_id, ft.property_type_id
        HAVING 
          COUNT(ft.price) >= 3  -- Ensure we have at least 3 transactions for meaningful stats
      `;
      
      const aggregateResult = await client.query(aggregateQuery, [startDate, endDate]);
      this.logger.log(`Calculated ${aggregateResult.rows.length} aggregate records for month ID: ${monthId}`);
      
      // Insert aggregates in batches
      const batch = [];
      for (const row of aggregateResult.rows) {
        batch.push({
          month_id: monthId,
          suburb_id: row.suburb_id,
          property_type_id: row.property_type_id,
          avg_price_per_sqm: row.avg_price_per_sqm,
          avg_land_area_value: row.avg_land_area_value,
          avg_building_age: row.avg_building_age,
          avg_price: row.avg_price,
          total_sales: row.total_sales,
          avg_landsize: row.avg_landsize,
          median_price: row.median_price
        });
        
        if (batch.length >= config.batchSize) {
          await this.insertBatch(batch);
          this.stats.inserted += batch.length;
          batch.length = 0;
        }
      }
      
      // Insert remaining records
      if (batch.length > 0) {
        await this.insertBatch(batch);
        this.stats.inserted += batch.length;
      }
      
      await client.query('COMMIT');
      this.logger.log(`Successfully inserted aggregates for month ID: ${monthId}`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(`Error inserting aggregates for month ID: ${monthId}`, error);
      this.errors.push({
        month_id: monthId,
        operation: 'insert',
        error: error.message
      });
      this.stats.errors++;
    } finally {
      client.release();
    }
  }
  
  async updateMonthAggregates(monthId) {
    // For incremental updates, it's often simpler to delete and reinsert
    // But we'll implement a more sophisticated approach that updates existing records
    const client = await olapPool.connect();
    try {
      await client.query('BEGIN');
      
      // Get the first and last day of the month
      const dateRangeQuery = `
        SELECT 
          MIN(d.full_date) AS start_date,
          MAX(d.full_date) AS end_date
        FROM 
          dim_date d
        WHERE 
          d.month_id = $1
      `;
      
      const dateRangeResult = await client.query(dateRangeQuery, [monthId]);
      const startDate = dateRangeResult.rows[0].start_date;
      const endDate = dateRangeResult.rows[0].end_date;
      
      // Calculate current aggregates
      const aggregateQuery = `
        WITH transaction_data AS (
          SELECT 
            s.suburb_id,
            ft.property_type_id,
            ft.price,
            ft.building_area,
            ft.land_area,
            ft.year_built,
            EXTRACT(YEAR FROM dd.full_date) - ft.year_built AS building_age
          FROM 
            fact_transactions ft
            JOIN dim_date dd ON ft.date_id = dd.date_id
            JOIN dim_address da ON ft.address_id = da.address_id
            JOIN dim_suburb s ON da.suburb_id = s.suburb_id
          WHERE 
            dd.full_date BETWEEN $1 AND $2
            AND ft.price > 0  -- Exclude invalid prices
        )
        SELECT 
          s.suburb_id,
          ft.property_type_id,
          
          -- Aggregate calculations
          COUNT(ft.price) AS total_sales,
          AVG(ft.price) AS avg_price,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ft.price) AS median_price,
          AVG(ft.land_area) AS avg_landsize,
          
          -- Calculate price per sqm only for records with valid building area
          AVG(
            CASE WHEN ft.building_area > 0 
            THEN ft.price / ft.building_area 
            ELSE NULL END
          ) AS avg_price_per_sqm,
          
          -- Calculate land value metrics only for records with valid land area
          AVG(
            CASE WHEN ft.land_area > 0 
            THEN ft.price / ft.land_area 
            ELSE NULL END
          ) AS avg_land_area_value,
          
          -- Calculate building age only for records with valid year_built
          AVG(ft.building_age) AS avg_building_age
          
        FROM 
          transaction_data ft
          JOIN dim_suburb s ON ft.suburb_id = s.suburb_id
        GROUP BY 
          s.suburb_id, ft.property_type_id
        HAVING 
          COUNT(ft.price) >= 3  -- Ensure we have at least 3 transactions for meaningful stats
      `;
      
      const aggregateResult = await client.query(aggregateQuery, [startDate, endDate]);
      this.logger.log(`Calculated ${aggregateResult.rows.length} aggregate records for month ID: ${monthId}`);
      
      // Get existing records
      const existingQuery = `
        SELECT 
          performance_id, suburb_id, property_type_id
        FROM 
          fact_property_month_performance
        WHERE 
          month_id = $1
      `;
      
      const existingResult = await client.query(existingQuery, [monthId]);
      const existingMap = {};
      
      // Create a map of existing records for quick lookup
      existingResult.rows.forEach(row => {
        const key = `${row.suburb_id}_${row.property_type_id}`;
        existingMap[key] = row.performance_id;
      });
      
      // Process updates and inserts
      const toUpdate = [];
      const toInsert = [];
      
      for (const row of aggregateResult.rows) {
        const key = `${row.suburb_id}_${row.property_type_id}`;
        
        if (existingMap[key]) {
          // Update existing record
          toUpdate.push({
            performance_id: existingMap[key],
            ...row,
            month_id: monthId
          });
          
          // Remove from map so we can track what needs to be deleted
          delete existingMap[key];
        } else {
          // New record to insert
          toInsert.push({
            month_id: monthId,
            suburb_id: row.suburb_id,
            property_type_id: row.property_type_id,
            avg_price_per_sqm: row.avg_price_per_sqm,
            avg_land_area_value: row.avg_land_area_value,
            avg_building_age: row.avg_building_age,
            avg_price: row.avg_price,
            total_sales: row.total_sales,
            avg_landsize: row.avg_landsize,
            median_price: row.median_price
          });
        }
      }
      
      // Process records to delete (those that still remain in existingMap)
      const toDelete = Object.values(existingMap);
      
      // Process deletes
      if (toDelete.length > 0) {
        const deleteQuery = `
          DELETE FROM fact_property_month_performance
          WHERE performance_id = ANY($1)
        `;
        
        await client.query(deleteQuery, [toDelete]);
        this.logger.log(`Deleted ${toDelete.length} outdated aggregate records`);
      }
      
      // Process updates in batches
      let updatedCount = 0;
      for (const record of toUpdate) {
        const updateQuery = `
          UPDATE fact_property_month_performance
          SET 
            avg_price_per_sqm = $1,
            avg_land_area_value = $2,
            avg_building_age = $3, 
            avg_price = $4,
            total_sales = $5,
            avg_landsize = $6,
            median_price = $7
          WHERE 
            performance_id = $8
        `;
        
        await client.query(updateQuery, [
          record.avg_price_per_sqm,
          record.avg_land_area_value,
          record.avg_building_age,
          record.avg_price,
          record.total_sales,
          record.avg_landsize,
          record.median_price,
          record.performance_id
        ]);
        
        updatedCount++;
      }
      
      this.stats.updated += updatedCount;
      this.logger.log(`Updated ${updatedCount} existing aggregate records`);
      
      // Process inserts
      if (toInsert.length > 0) {
        const insertBatches = [];
        for (let i = 0; i < toInsert.length; i += config.batchSize) {
          insertBatches.push(toInsert.slice(i, i + config.batchSize));
        }
        
        for (const batch of insertBatches) {
          await this.insertBatch(batch);
        }
        
        this.stats.inserted += toInsert.length;
        this.logger.log(`Inserted ${toInsert.length} new aggregate records`);
      }
      
      await client.query('COMMIT');
      this.logger.log(`Successfully updated aggregates for month ID: ${monthId}`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error(`Error updating aggregates for month ID: ${monthId}`, error);
      this.errors.push({
        month_id: monthId,
        operation: 'update',
        error: error.message
      });
      this.stats.errors++;
    } finally {
      client.release();
    }
  }
  
  async insertBatch(records) {
    if (records.length === 0) return;
    
    const client = await olapPool.connect();
    try {
      // Build multi-row insert statement
      const columns = [
        'month_id', 'suburb_id', 'property_type_id', 
        'avg_price_per_sqm', 'avg_land_area_value', 'avg_building_age',
        'avg_price', 'total_sales', 'avg_landsize', 'median_price'
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
        INSERT INTO fact_property_month_performance (${columns.join(', ')})
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (month_id, suburb_id, property_type_id) DO UPDATE
        SET 
          avg_price_per_sqm = EXCLUDED.avg_price_per_sqm,
          avg_land_area_value = EXCLUDED.avg_land_area_value,
          avg_building_age = EXCLUDED.avg_building_age,
          avg_price = EXCLUDED.avg_price,
          total_sales = EXCLUDED.total_sales,
          avg_landsize = EXCLUDED.avg_landsize,
          median_price = EXCLUDED.median_price
      `;
      
      await client.query(query, values);
    } finally {
      client.release();
    }
  }
  
  saveErrors() {
    if (this.errors.length === 0) return;
    
    const errorLogPath = path.join(config.logDir, `${this.name}_errors.json`);
    fs.writeFileSync(errorLogPath, JSON.stringify(this.errors, null, 2), 'utf8');
    this.logger.log(`Errors saved to ${errorLogPath}`);
  }
}

// Main aggregation runner
async function runAggregationUpdate(startDate, endDate, processType = 'full') {
  try {
    console.log(`Starting property performance aggregation (${processType}) for period: ${startDate} to ${endDate}`);
    
    const aggregator = new PropertyPerformanceAggregator();
    const stats = await aggregator.calculateAggregates(startDate, endDate, processType);
    
    console.log('\nAggregation process complete!');
    console.log(`Processed ${stats.processed} months`);
    console.log(`Inserted: ${stats.inserted}, Updated: ${stats.updated}, Errors: ${stats.errors}`);
    console.log(`Total time: ${((stats.end - stats.start) / 1000).toFixed(2)} seconds`);
    
    return stats;
  } catch (error) {
    console.error('Error in aggregation process:', error);
  } finally {
    await olapPool.end();
  }
}

// Incremental update - processes only months that have new data
async function runIncrementalUpdate(startDate, endDate) {
  return runAggregationUpdate(startDate, endDate, 'incremental');
}

// Full update - recreates all aggregates for the specified period
async function runFullUpdate(startDate, endDate) {
  return runAggregationUpdate(startDate, endDate, 'full');
}

// Entry point
if (require.main === module) {
  // Example: Run aggregation for the previous year
  const today = new Date();
  const endDate = today.toISOString().split('T')[0];
  
  // Default to one year of data
  const startDate = new Date(today);
  startDate.setFullYear(today.getFullYear() - 1);
  const startDateStr = startDate.toISOString().split('T')[0];
  
  // Get command line arguments if provided
  const args = process.argv.slice(2);
  const mode = args[0] || 'full';
  const start = args[1] || startDateStr;
  const end = args[2] || endDate;
  
  if (mode === 'incremental') {
    runIncrementalUpdate(start, end).catch(err => {
      console.error('Fatal error:', err);
      process.exit(1);
    });
  } else {
    runFullUpdate(start, end).catch(err => {
      console.error('Fatal error:', err);
      process.exit(1);
    });
  }
}

module.exports = {
  PropertyPerformanceAggregator,
  runFullUpdate,
  runIncrementalUpdate
};