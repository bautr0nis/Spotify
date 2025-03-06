import * as fs from 'fs';  // Import the 'fs' module for file system operations
import * as csv from 'fast-csv';  // Import the 'fast-csv' library for CSV parsing
import { pgClient, generateCreateTableSQL } from './pgUtils';  // Import PostgreSQL functions
import { detectColumnTypes } from './fileUtils';  // Import file handling functions

// Function to sanitize and handle empty strings for numeric columns
const sanitizeValues = (values: any[], columnTypes: { [key: string]: string }) => {
  return values.map((value, index) => {
    if (value === "" || value === null || value === undefined) {
      // Set empty or missing values to NULL for numeric columns
      if (columnTypes[Object.keys(columnTypes)[index]] === "FLOAT" || columnTypes[Object.keys(columnTypes)[index]] === "INT") {
        return null;
      }
    }
    return value;
  });
};

// Function to handle and fix invalid or incomplete dates
const fixDate = (dateStr: string) => {
  if (!dateStr) return null;

  const parts = dateStr.split("-");

  // If we have only year and month (e.g., "1956-03"), append the day (e.g., "1956-03-01")
  if (parts.length === 2) {
    return `${parts[0]}-${parts[1]}-01`;  // Add default day as 01
  }

  // If only the year is provided (e.g., "1956"), make it a full date "1956-01-01"
  if (parts.length === 1) {
    return `${parts[0]}-01-01`;
  }

  return dateStr;
};

// Function to check if the table already contains the required number of rows
const checkRowCount = async (tableName: string, csvRowsCount: number): Promise<boolean> => {
  try {
    const res = await pgClient.query(`SELECT COUNT(*) FROM ${tableName};`);
    const rowCount = parseInt(res.rows[0].count, 10);
    return rowCount === csvRowsCount; // If the row count matches, return true
  } catch (error) {
    console.error(`❌ Error checking row count for table "${tableName}":`, error);
    return false; // If there was an error, we proceed with the insert
  }
};

// Function to process a single CSV file and insert into PostgreSQL
export const processCSV = async (filePath: string, tableName: string, separator: string) => {
  const rows: any[] = [];

  // Read the CSV file
  await new Promise<void>((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv.parse({ headers: true, delimiter: separator }))
      .on("data", (row) => {
        rows.push(row);  // Collect all rows (not just 5)
      })
      .on("end", () => {
        resolve();  // Resolve when reading is complete
      })
      .on("error", reject);  // Reject on error
  });

  if (rows.length === 0) {
    console.error(`❌ No data found in ${filePath}`);
    return;
  }

  const columnTypes = detectColumnTypes(rows);
  const createTableSQL = generateCreateTableSQL(tableName, columnTypes);

  console.log(`🚀 Creating table for ${tableName}:\n${createTableSQL}`);

  try {
    await pgClient.query(createTableSQL);  // Use pgClient to run the SQL query
    console.log(`✅ Table "${tableName}" created or already exists!`);

    // Check if the table already has the correct number of rows
    const isRowCountMatch = await checkRowCount(tableName, rows.length);

    if (isRowCountMatch) {
      console.log(`✅ Skipping insertion for "${tableName}" as it already contains ${rows.length} rows.`);
      return;  // Skip the insertion if row count matches
    }

    // Insert all rows into PostgreSQL if row counts don't match
    for (const row of rows) {
      const columns = Object.keys(row);
      const placeholders = columns.map((_, index) => `$${index + 1}`).join(", ");

      // Fix dates before inserting
      const sanitizedRow = columns.map((column, index) => {
        if (column === "release_date") {
          return fixDate(row[column]);  // Fix invalid or incomplete dates
        }
        return row[column];
      });

      const values = sanitizeValues(sanitizedRow, columnTypes);  // Sanitize the values
      console.log(`Inserting values: ${JSON.stringify(values)}`); // Log values being inserted

      const insertSQL = `INSERT INTO ${tableName} (${columns.map(c => `"${c}"`).join(", ")}) 
                         VALUES (${placeholders}) ON CONFLICT DO NOTHING;`;

      await pgClient.query(insertSQL, values);  // Use pgClient to run the insert query
    }

    console.log(`✅ Data from ${filePath} inserted successfully!`);
  } catch (error) {
    console.error(`❌ Error inserting data from ${filePath}:`, error);
  }
};