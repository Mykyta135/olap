// change-tracker.js
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Configuration
const config = {
  oltp: {
    host: 'localhost',
    database: 'OLTP',
    user: 'postgres',
    password: '1234', 
    port: 5432
  },
  tracking: {
    changelogDir: './changelog',
    checksumDir: './checksums',
    scanInterval: 3600000, 
    batchSize: 5000
  }
};

// Ensure directories exist
[config.tracking.changelogDir, config.tracking.checksumDir].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
});

// Initialize database connection
const pool = new Pool(config.oltp);

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

// Change detection methods
const CHANGE_DETECTION = {
  // Use timestamps if the table has them
  TIMESTAMP: 'timestamp',
  // Use primary key comparison for tables without timestamps
  PRIMARY_KEY: 'primary_key',
  // Use row-level checksums for comprehensive change detection
  CHECKSUM: 'checksum'
};

// Table definition for tracking
class TableTracker {
  constructor(tableName, primaryKeys, options = {}) {
    this.tableName = tableName;
    this.primaryKeys = Array.isArray(primaryKeys) ? primaryKeys : [primaryKeys];
    this.method = options.method || CHANGE_DETECTION.PRIMARY_KEY;
    this.timestampColumn = options.timestampColumn;
    this.excludeColumns = options.excludeColumns || [];
    this.logger = new Logger(`${tableName}_tracker.log`);
    this.checksumFile = path.join(config.tracking.checksumDir, `${tableName}_checksums.json`);
    
    // Validate configuration
    if (this.method === CHANGE_DETECTION.TIMESTAMP && !this.timestampColumn) {
      throw new Error(`Table ${tableName} uses timestamp method but no timestampColumn provided`);
    }
  }
  
  // Load previous checksums
  loadChecksums() {
    try {
      if (fs.existsSync(this.checksumFile)) {
        const data = fs.readFileSync(this.checksumFile, 'utf8');
        return JSON.parse(data);
      }
    } catch (error) {
      this.logger.error('Failed to load previous checksums', error);
    }
    return {};
  }
  
  // Save current checksums
  saveChecksums(checksums) {
    try {
      fs.writeFileSync(this.checksumFile, JSON.stringify(checksums, null, 2), 'utf8');
    } catch (error) {
      this.logger.error('Failed to save checksums', error);
    }
  }
  
  // Generate MD5 hash for a row
  generateChecksum(row) {
    const relevantData = { ...row };
    
    // Remove excluded columns
    this.excludeColumns.forEach(column => {
      delete relevantData[column];
    });
    
    // Create deterministic string representation
    const sortedKeys = Object.keys(relevantData).sort();
    const normalizedData = sortedKeys.map(key => `${key}:${relevantData[key]}`).join('|');
    
    // Generate MD5 hash
    return crypto.createHash('md5').update(normalizedData).digest('hex');
  }
  
  // Get primary key value as string (for composite keys)
  getPrimaryKeyValue(row) {
    return this.primaryKeys.map(key => row[key]).join('|');
  }
  
  // Get full list of columns
  async getTableColumns() {
    const client = await pool.connect();
    try {
      const query = `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1
      `;
      const result = await client.query(query, [this.tableName]);
      return result.rows.map(row => row.column_name);
    } finally {
      client.release();
    }
  }
  
  // Scan for changes based on the configured detection method
  async scanForChanges(lastScanTime) {
    const client = await pool.connect();
    this.logger.log(`Scanning table ${this.tableName} for changes since ${lastScanTime}`);
    
    try {
      // Get all columns for the table
      const allColumns = await this.getTableColumns();
      const columns = allColumns.filter(col => !this.excludeColumns.includes(col));
      
      let changes = {
        inserts: [],
        updates: [],
        deletes: [],
        scanTime: new Date().toISOString()
      };
      
      switch (this.method) {
        case CHANGE_DETECTION.TIMESTAMP:
          changes = await this.scanByTimestamp(client, columns, lastScanTime);
          break;
        case CHANGE_DETECTION.PRIMARY_KEY:
          changes = await this.scanByPrimaryKey(client, columns, lastScanTime);
          break;
        case CHANGE_DETECTION.CHECKSUM:
          changes = await this.scanByChecksum(client, columns);
          break;
        default:
          throw new Error(`Unknown change detection method: ${this.method}`);
      }
      
      this.logger.log(`Found changes in ${this.tableName}: ${changes.inserts.length} inserts, ${changes.updates.length} updates, ${changes.deletes.length} deletes`);
      return changes;
      
    } catch (error) {
      this.logger.error(`Error scanning table ${this.tableName}`, error);
      throw error;
    } finally {
      client.release();
    }
  }
  
  // Scan using timestamp column
  async scanByTimestamp(client, columns, lastScanTime) {
    const columnList = columns.join(', ');
    
    const query = `
      SELECT ${columnList}
      FROM ${this.tableName}
      WHERE ${this.timestampColumn} >= $1
      ORDER BY ${this.timestampColumn}
    `;
    
    const result = await client.query(query, [lastScanTime]);
    
    return {
      inserts: result.rows, // Simplified: treating all as inserts/updates
      updates: [],
      deletes: [],
      scanTime: new Date().toISOString()
    };
  }
  
  // Scan by comparing with previous scan
  async scanByPrimaryKey(client, columns, lastScanTime) {
    const columnList = columns.join(', ');
    const pkCondition = this.primaryKeys.map(pk => `${pk} = previous.${pk}`).join(' AND ');
    
    // Load previous scan results
    const previousScanFile = path.join(
      config.tracking.changelogDir,
      `${this.tableName}_previous_scan.json`
    );
    
    let previousRecords = [];
    if (fs.existsSync(previousScanFile)) {
      previousRecords = JSON.parse(fs.readFileSync(previousScanFile, 'utf8'));
    }
    
    // Get current records
    const query = `SELECT ${columnList} FROM ${this.tableName}`;
    const result = await client.query(query);
    const currentRecords = result.rows;
    
    // Index previous records by primary key
    const previousMap = new Map();
    previousRecords.forEach(record => {
      const keyValue = this.getPrimaryKeyValue(record);
      previousMap.set(keyValue, record);
    });
    
    // Find inserts and updates
    const currentMap = new Map();
    const inserts = [];
    const updates = [];
    
    currentRecords.forEach(record => {
      const keyValue = this.getPrimaryKeyValue(record);
      currentMap.set(keyValue, record);
      
      if (!previousMap.has(keyValue)) {
        // New record
        inserts.push(record);
      } else {
        // Check for updates by comparing checksums
        const previousChecksum = this.generateChecksum(previousMap.get(keyValue));
        const currentChecksum = this.generateChecksum(record);
        
        if (previousChecksum !== currentChecksum) {
          updates.push(record);
        }
      }
    });
    
    // Find deletes
    const deletes = [];
    previousMap.forEach((record, keyValue) => {
      if (!currentMap.has(keyValue)) {
        deletes.push(record);
      }
    });
    
    // Save current records for next comparison
    fs.writeFileSync(previousScanFile, JSON.stringify(currentRecords, null, 2), 'utf8');
    
    return {
      inserts,
      updates,
      deletes,
      scanTime: new Date().toISOString()
    };
  }
  
  // Scan by comparing checksums
  async scanByChecksum(client, columns) {
    const columnList = columns.join(', ');
    
    // Load previous checksums
    const previousChecksums = this.loadChecksums();
    
    // Get current records
    const query = `SELECT ${columnList} FROM ${this.tableName}`;
    const result = await client.query(query);
    const currentRecords = result.rows;
    
    // Calculate checksums and identify changes
    const currentChecksums = {};
    const inserts = [];
    const updates = [];
    
    currentRecords.forEach(record => {
      const keyValue = this.getPrimaryKeyValue(record);
      const checksum = this.generateChecksum(record);
      currentChecksums[keyValue] = checksum;
      
      if (!previousChecksums[keyValue]) {
        // New record
        inserts.push(record);
      } else if (previousChecksums[keyValue] !== checksum) {
        // Updated record
        updates.push(record);
      }
    });
    
    // Find deletes
    const deletes = [];
    Object.keys(previousChecksums).forEach(keyValue => {
      if (!currentChecksums[keyValue]) {
        // Record no longer exists
        // Note: We only have the key, not the full record data
        const keyParts = keyValue.split('|');
        const deleteRecord = {};
        this.primaryKeys.forEach((key, index) => {
          deleteRecord[key] = keyParts[index];
        });
        deletes.push(deleteRecord);
      }
    });
    
    // Save current checksums for next comparison
    this.saveChecksums(currentChecksums);
    
    return {
      inserts,
      updates,
      deletes,
      scanTime: new Date().toISOString()
    };
  }
  
  // Save changes to changelog file
  async saveChangelog(changes) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '_');
    const changelogFile = path.join(
      config.tracking.changelogDir,
      `${this.tableName}_changes_${timestamp}.json`
    );
    
    fs.writeFileSync(changelogFile, JSON.stringify(changes, null, 2), 'utf8');
    this.logger.log(`Changes saved to ${changelogFile}`);
    
    return changelogFile;
  }
}

// Change tracking service that manages multiple table trackers
class ChangeTrackingService {
  constructor() {
    this.trackers = [];
    this.logger = new Logger('change_tracking_service.log');
    this.stateFile = path.join(config.tracking.changelogDir, 'tracking_state.json');
  }
  
  // Register a table for tracking
  registerTable(tableName, primaryKeys, options = {}) {
    const tracker = new TableTracker(tableName, primaryKeys, options);
    this.trackers.push(tracker);
    this.logger.log(`Registered table ${tableName} for change tracking`);
    return this;
  }
  
  // Load service state
  loadState() {
    try {
      if (fs.existsSync(this.stateFile)) {
        const data = fs.readFileSync(this.stateFile, 'utf8');
        return JSON.parse(data);
      }
    } catch (error) {
      this.logger.error('Failed to load tracking state', error);
    }
    
    return {
      lastScanTime: new Date(0).toISOString(),
      tables: {}
    };
  }
  
  // Save service state
  saveState(state) {
    try {
      fs.writeFileSync(this.stateFile, JSON.stringify(state, null, 2), 'utf8');
    } catch (error) {
      this.logger.error('Failed to save tracking state', error);
    }
  }
  
  // Run a scan for all registered tables
  async runScan() {
    this.logger.log('Starting change tracking scan');
    
    // Load current state
    const state = this.loadState();
    const lastScanTime = state.lastScanTime;
    const currentScanTime = new Date().toISOString();
    
    // Track results for all tables
    const results = {};
    
    // Process each table
    for (const tracker of this.trackers) {
      try {
        const tableState = state.tables[tracker.tableName] || { lastScanTime };
        const changes = await tracker.scanForChanges(tableState.lastScanTime);
        
        // Save changelog if there are changes
        if (changes.inserts.length > 0 || changes.updates.length > 0 || changes.deletes.length > 0) {
          const changelogFile = await tracker.saveChangelog(changes);
          results[tracker.tableName] = {
            inserts: changes.inserts.length,
            updates: changes.updates.length,
            deletes: changes.deletes.length,
            changelogFile
          };
        } else {
          results[tracker.tableName] = {
            inserts: 0,
            updates: 0,
            deletes: 0,
            message: 'No changes detected'
          };
        }
        
        // Update table state
        state.tables[tracker.tableName] = {
          lastScanTime: currentScanTime
        };
      } catch (error) {
        this.logger.error(`Error processing table ${tracker.tableName}`, error);
        results[tracker.tableName] = {
          error: error.message
        };
      }
    }
    
    // Update state
    state.lastScanTime = currentScanTime;
    this.saveState(state);
    
    this.logger.log('Change tracking scan completed');
    return results;
  }
  
  // Start periodic scanning (for background service)
  startPeriodicScanning() {
    this.logger.log(`Starting periodic scanning at ${config.tracking.scanInterval}ms intervals`);
    
    // Run initial scan
    this.runScan().catch(error => {
      this.logger.error('Error during initial scan', error);
    });
    
    // Set interval for future scans
    this.scanInterval = setInterval(() => {
      this.runScan().catch(error => {
        this.logger.error('Error during periodic scan', error);
      });
    }, config.tracking.scanInterval);
    
    return this;
  }
  
  // Stop periodic scanning
  stopPeriodicScanning() {
    if (this.scanInterval) {
      clearInterval(this.scanInterval);
      this.scanInterval = null;
      this.logger.log('Stopped periodic scanning');
    }
    return this;
  }
}

// Example usage
async function main() {
  // Create and configure the tracking service
  const trackingService = new ChangeTrackingService();
  
  // Register tables with appropriate tracking methods
  trackingService
    .registerTable('property_location', 'id')
    .registerTable('property_features', 'id')
    .registerTable('transactions', 'id', {
      // Optionally exclude columns that change frequently but aren't relevant for OLAP
      excludeColumns: ['last_updated_by']
    });
  
  // Run a manual scan
  try {
    const result = await trackingService.runScan();
    console.log('Scan results:', result);
  } catch (error) {
    console.error('Error running scan:', error);
  }
  
  // Optional: Start periodic scanning (comment out if not needed)
  // trackingService.startPeriodicScanning();
  
  // To stop periodic scanning when needed
  // trackingService.stopPeriodicScanning();
  
  // Close database pool when done
  pool.end();
}

// Export for use in other modules
module.exports = {
  TableTracker,
  ChangeTrackingService,
  CHANGE_DETECTION
};

// Run the main function if this file is executed directly
if (require.main === module) {
  main().catch(console.error);
}