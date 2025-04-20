// test-incremental-updates.js
const { executeIncrementalUpdate} = require('./incrementalUpdate');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// Test configuration
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
    changelogDir: './test/changelog',
    stateFile: './test/changelog/incremental_state.json'
  }
};

// Create test directories
if (!fs.existsSync(config.tracking.changelogDir)) {
  fs.mkdirSync(config.tracking.changelogDir, { recursive: true });
}

// Initialize database connections
const oltpPool = new Pool(config.oltp);
const olapPool = new Pool(config.olap);

// Logger for tests
class TestLogger {
  constructor() {
    this.logs = [];
  }

  log(message) {
    console.log(`[TEST] ${message}`);
    this.logs.push({ type: 'info', message });
  }

  error(message, error) {
    console.error(`[TEST ERROR] ${message}`, error);
    this.logs.push({ type: 'error', message, error: error.toString() });
  }

  saveLogs(filename) {
    const logPath = path.join(config.tracking.changelogDir, filename);
    fs.writeFileSync(logPath, JSON.stringify(this.logs, null, 2), 'utf8');
  }
}

// Test case 1: Testing SCD Type 1 updates for property types
async function testPropertyTypeUpdates() {
  const logger = new TestLogger();
  logger.log('Starting Property Type Dimension Test');
  
  try {
    // 1. Set up test data
    await oltpPool.query('BEGIN');
    
    // Clean existing test data
    await oltpPool.query('DELETE FROM property_types WHERE id >= 1000');
    
    // Insert test property types
    await oltpPool.query(`
      INSERT INTO property_types (id, type) VALUES 
      (1000, 'h'), 
      (1001, 'u'),
      (1002, 't')
    `);
    
    await oltpPool.query('COMMIT');
    
    // 2. Run initial incremental update
    logger.log('Running initial incremental update');
    await executeIncrementalUpdate();
    
    // 3. Verify property types were inserted into data warehouse
    const initialResult = await olapPool.query(`
      SELECT * FROM dim_property_type 
      WHERE property_type_name IN ('House', 'Unit', 'Townhouse')
    `);
    
    logger.log(`Initial property types in DW: ${initialResult.rowCount}`);
    
    // 4. Modify property types in source system
    await oltpPool.query('BEGIN');
    
    // Update an existing property type
    await oltpPool.query(`
      UPDATE property_types SET type = 'a' WHERE id = 1001
    `);
    
    // Add a new property type
    await oltpPool.query(`
      INSERT INTO property_types (id, type) VALUES (1003, 'v')
    `);
    
    await oltpPool.query('COMMIT');
    
    // 5. Run incremental update again
    logger.log('Running incremental update after modifications');
    await executeIncrementalUpdate();
    
    // 6. Verify changes were propagated to data warehouse
    const afterChangeResult = await olapPool.query(`
      SELECT * FROM dim_property_type 
      WHERE property_type_name IN ('House', 'Apartment', 'Townhouse', 'Villa')
    `);
    
    logger.log(`Property types after update: ${afterChangeResult.rowCount}`);
    
    // Verify Unit was replaced with Apartment (SCD Type 1 behavior)
    const unitResult = await olapPool.query(`
      SELECT * FROM dim_property_type WHERE property_type_name = 'Unit'
    `);
    
    const apartmentResult = await olapPool.query(`
      SELECT * FROM dim_property_type WHERE property_type_name = 'Apartment'
    `);
    
    logger.log(`Units remaining: ${unitResult.rowCount}, Apartments: ${apartmentResult.rowCount}`);
    
    // 7. Clean up
    await oltpPool.query('DELETE FROM property_types WHERE id >= 1000');
    
    // Return test results
    return {
      initialCount: initialResult.rowCount,
      finalCount: afterChangeResult.rowCount,
      unitRemaining: unitResult.rowCount,
      apartmentCount: apartmentResult.rowCount,
      success: apartmentResult.rowCount > 0 && unitResult.rowCount === 0
    };
  } catch (error) {
    logger.error('Error in property type test', error);
    throw error;
  } finally {
    logger.saveLogs('property_type_test.json');
  }
}

// Test case 2: Testing SCD Type 2 updates for addresses
async function testAddressUpdates() {
  const logger = new TestLogger();
  logger.log('Starting Address Dimension Test (SCD Type 2)');
  
  try {
    // 1. Set up test data for hierarchy
    await oltpPool.query('BEGIN');
    
    // Clean existing test data
    await oltpPool.query('DELETE FROM addresses WHERE id >= 1000');
    await oltpPool.query('DELETE FROM suburbs WHERE id >= 1000');
    await oltpPool.query('DELETE FROM council_areas WHERE id >= 1000');
    await oltpPool.query('DELETE FROM metropolitan_areas WHERE id >= 1000');
    
    // Insert test hierarchy
    await oltpPool.query(`
      INSERT INTO metropolitan_areas (id, name, state, country) VALUES 
      (1000, 'Test Metro', 'Test State', 'Australia')
    `);
    
    await oltpPool.query(`
      INSERT INTO council_areas (id, name, metropolitan_id) VALUES 
      (1000, 'Test Council', 1000)
    `);
    
    await oltpPool.query(`
      INSERT INTO suburbs (id, name, council_id, postcode) VALUES 
      (1000, 'Test Suburb', 1000, '1234')
    `);
    
    // Insert test addresses
    await oltpPool.query(`
      INSERT INTO addresses (id, address, suburb_id) VALUES 
      (1000, '123 Test Street', 1000),
      (1001, '456 Sample Road', 1000)
    `);
    
    await oltpPool.query('COMMIT');
    
    // 2. Run initial incremental update
    logger.log('Running initial incremental update');
    await executeIncrementalUpdate();
    
    // 3. Verify addresses were inserted into data warehouse
    const initialResult = await olapPool.query(`
      SELECT a.*, s.suburb_name
      FROM dim_address a
      JOIN dim_suburb s ON a.suburb_id = s.suburb_id
      WHERE s.suburb_name = 'Test Suburb'
    `);
    
    logger.log(`Initial addresses in DW: ${initialResult.rowCount}`);
    
    // 4. Modify addresses in source system
    await oltpPool.query('BEGIN');
    
    // Update an existing address
    await oltpPool.query(`
      UPDATE addresses SET address = '123 Updated Street' WHERE id = 1000
    `);
    
    // Add a new address
    await oltpPool.query(`
      INSERT INTO addresses (id, address, suburb_id) VALUES (1002, '789 New Avenue', 1000)
    `);
    
    await oltpPool.query('COMMIT');
    
    // 5. Run incremental update again
    logger.log('Running incremental update after modifications');
    await executeIncrementalUpdate();
    
    // 6. Verify changes were propagated to data warehouse
    const afterChangeResult = await olapPool.query(`
      SELECT a.*, s.suburb_name
      FROM dim_address a
      JOIN dim_suburb s ON a.suburb_id = s.suburb_id
      WHERE s.suburb_name = 'Test Suburb'
    `);
    
    logger.log(`Addresses after update: ${afterChangeResult.rowCount}`);
    
    // Verify the old address version is marked as not current (SCD Type 2 behavior)
    const oldVersionResult = await olapPool.query(`
      SELECT * FROM dim_address
      WHERE street_number = '123' AND street_name = 'Test Street' AND is_current = FALSE
    `);
    
    // Verify the new address version is current
    const newVersionResult = await olapPool.query(`
      SELECT * FROM dim_address
      WHERE street_number = '123' AND street_name = 'Updated Street' AND is_current = TRUE
    `);
    
    logger.log(`Old versions: ${oldVersionResult.rowCount}, New versions: ${newVersionResult.rowCount}`);
    
    // 7. Clean up
    await oltpPool.query('DELETE FROM addresses WHERE id >= 1000');
    await oltpPool.query('DELETE FROM suburbs WHERE id >= 1000');
    await oltpPool.query('DELETE FROM council_areas WHERE id >= 1000');
    await oltpPool.query('DELETE FROM metropolitan_areas WHERE id >= 1000');
    
    // Return test results
    return {
      initialCount: initialResult.rowCount,
      finalCount: afterChangeResult.rowCount,
      oldVersionsCount: oldVersionResult.rowCount,
      newVersionsCount: newVersionResult.rowCount,
      success: oldVersionResult.rowCount > 0 && newVersionResult.rowCount > 0
    };
  } catch (error) {
    logger.error('Error in address test', error);
    throw error;
  } finally {
    logger.saveLogs('address_test.json');
  }
}

// Test case 3: Testing fact table updates
async function testTransactionFactUpdates() {
  const logger = new TestLogger();
  logger.log('Starting Transaction Fact Table Test');
  
  try {
    // 1. Set up test data for dimensions and facts
    await oltpPool.query('BEGIN');
    
    // Clean existing test data
    await oltpPool.query('DELETE FROM property_transactions WHERE id >= 1000');
    
    // Ensure dimensions exist
    await oltpPool.query(`
      INSERT INTO dates (date_id, full_date, day_of_week, month_num, year_num)
      VALUES (20250101, '2025-01-01', 3, 1, 2025)
      ON CONFLICT DO NOTHING
    `);
    
    // Insert test transactions
    await oltpPool.query(`
      INSERT INTO property_transactions (
        id, address, suburb, price, sale_date, type, method,
        year_built, building_area, landsize, bathroom, bedroom, car, distance
      ) VALUES 
      (1000, '123 Test Street', 'Test Suburb', 500000, '2025-01-01', 'h', 'Auction',
       2010, 150, 500, 2, 3, 1, 5),
      (1001, '456 Sample Road', 'Test Suburb', 600000, '2025-01-01', 'u', 'Private Sale',
       2015, 100, 0, 1, 2, 1, 4)
    `);
    
    await oltpPool.query('COMMIT');
    
    // 2. Run initial incremental update
    logger.log('Running initial incremental update');
    await executeIncrementalUpdate();
    
    // 3. Verify transactions were inserted into data warehouse
    const initialResult = await olapPool.query(`
      SELECT f.*, a.street_number, a.street_name, pt.property_type_name
      FROM fact_transactions f
      JOIN dim_address a ON f.address_id = a.address_id
      JOIN dim_property_type pt ON f.property_type_id = pt.property_type_id
      WHERE a.street_name IN ('Test Street', 'Sample Road')
    `);
    
    logger.log(`Initial transactions in DW: ${initialResult.rowCount}`);
    
    // 4. Verify monthly aggregates were created
    const initialAggregateResult = await olapPool.query(`
      SELECT f.*, s.suburb_name, pt.property_type_name
      FROM fact_property_month_performance f
      JOIN dim_suburb s ON f.suburb_id = s.suburb_id
      JOIN dim_property_type pt ON f.property_type_id = pt.property_type_id
      WHERE s.suburb_name = 'Test Suburb'
    `);
    
    logger.log(`Initial monthly aggregates in DW: ${initialAggregateResult.rowCount}`);
    
    // 5. Modify transactions in source system
    await oltpPool.query('BEGIN');
    
    // Update an existing transaction
    await oltpPool.query(`
      UPDATE property_transactions 
      SET price = 550000, bedroom = 4
      WHERE id = 1000
    `);
    
    // Add a new transaction
    await oltpPool.query(`
      INSERT INTO property_transactions (
        id, address, suburb, price, sale_date, type, method,
        year_built, building_area, landsize, bathroom, bedroom, car, distance
      ) VALUES 
      (1002, '789 New Avenue', 'Test Suburb', 700000, '2025-01-01', 't', 'Auction',
       2020, 200, 600, 2, 4, 2, 3)
    `);
    
    await oltpPool.query('COMMIT');
    
    // 6. Run incremental update again
    logger.log('Running incremental update after modifications');
    await executeIncrementalUpdate();
    
    // 7. Verify changes were propagated to fact table
    const afterChangeResult = await olapPool.query(`
      SELECT f.*, a.street_number, a.street_name, pt.property_type_name
      FROM fact_transactions f
      JOIN dim_address a ON f.address_id = a.address_id
      JOIN dim_property_type pt ON f.property_type_id = pt.property_type_id
      WHERE a.street_name IN ('Test Street', 'Sample Road', 'New Avenue')
    `);
    
    logger.log(`Transactions after update: ${afterChangeResult.rowCount}`);
    
    // 8. Verify monthly aggregates were updated
    const afterAggregateResult = await olapPool.query(`
      SELECT f.*, s.suburb_name, pt.property_type_name
      FROM fact_property_month_performance f
      JOIN dim_suburb s ON f.suburb_id = s.suburb_id
      JOIN dim_property_type pt ON f.property_type_id = pt.property_type_id
      WHERE s.suburb_name = 'Test Suburb'
    `);
    
    logger.log(`Monthly aggregates after update: ${afterAggregateResult.rowCount}`);
    
    // Check if aggregates reflect the updated data
    const houseAggregateResult = await olapPool.query(`
      SELECT avg_price, total_sales
      FROM fact_property_month_performance f
      JOIN dim_suburb s ON f.suburb_id = s.suburb_id
      JOIN dim_property_type pt ON f.property_type_id = pt.property_type_id
      WHERE s.suburb_name = 'Test Suburb' AND pt.property_type_name = 'House'
    `);
    
    const avgHousePrice = houseAggregateResult.rows[0]?.avg_price || 0;
    logger.log(`Updated average house price: ${avgHousePrice}`);
    
    // 9. Clean up
    await oltpPool.query('DELETE FROM property_transactions WHERE id >= 1000');
    
    // Return test results
    return {
      initialTransactions: initialResult.rowCount,
      finalTransactions: afterChangeResult.rowCount,
      initialAggregates: initialAggregateResult.rowCount,
      finalAggregates: afterAggregateResult.rowCount,
      updatedAvgHousePrice: avgHousePrice,
      success: afterChangeResult.rowCount > initialResult.rowCount && 
               Math.abs(avgHousePrice - 550000) < 0.01 // Check if house price was updated
    };
  } catch (error) {
    logger.error('Error in transaction fact test', error);
    throw error;
  } finally {
    logger.saveLogs('transaction_test.json');
  }
}

// Test runner
async function runTests() {
  const results = {
    propertyTypeTest: null,
    addressTest: null,
    transactionTest: null
  };
  
  try {
    console.log('===== Testing Incremental Updates =====');
    
    console.log('\n----- Testing Property Type Updates (SCD Type 1) -----');
    results.propertyTypeTest = await testPropertyTypeUpdates();
    console.log('Property Type Test:', results.propertyTypeTest.success ? 'PASSED' : 'FAILED');
    
    console.log('\n----- Testing Address Updates (SCD Type 2) -----');
    results.addressTest = await testAddressUpdates();
    console.log('Address Test:', results.addressTest.success ? 'PASSED' : 'FAILED');
    
    console.log('\n----- Testing Transaction Fact Updates -----');
    results.transactionTest = await testTransactionFactUpdates();
    console.log('Transaction Test:', results.transactionTest.success ? 'PASSED' : 'FAILED');
    
    console.log('\n===== Test Summary =====');
    console.log('Property Type Test (SCD Type 1):', results.propertyTypeTest.success ? 'PASSED' : 'FAILED');
    console.log('Address Test (SCD Type 2):', results.addressTest.success ? 'PASSED' : 'FAILED');
    console.log('Transaction Fact Test:', results.transactionTest.success ? 'PASSED' : 'FAILED');
    
    // Save results to file
    fs.writeFileSync(
      path.join(config.tracking.changelogDir, 'test_results.json'),
      JSON.stringify(results, null, 2),
      'utf8'
    );
    
    return results;
  } catch (error) {
    console.error('Error running tests:', error);
    throw error;
  } finally {
    // Close connections
    await oltpPool.end();
    await olapPool.end();
  }
}

// Run the tests
if (require.main === module) {
  runTests()
    .then(() => console.log('Tests completed.'))
    .catch(err => {
      console.error('Tests failed:', err);
      process.exit(1);
    });
}

module.exports = {
  runTests,
  testPropertyTypeUpdates,
  testAddressUpdates,
  testTransactionFactUpdates
};