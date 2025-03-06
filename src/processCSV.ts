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

// Function to process a single CSV file and insert into PostgreSQL
export const processCSV = async (filePath: string, tableName: string, separator: string) => {
  const rows: any[] = [];
  fs.createReadStream(filePath)
    .pipe(csv.parse({ headers: true, delimiter: separator }))
    .on("data", (row) => {
      rows.push(row);  // Collect all rows (not just 5)
    })
    .on("end", async () => {
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

        // Insert all rows into PostgreSQL
        for (const row of rows) {
          const columns = Object.keys(row);
          const placeholders = columns.map((_, index) => `$${index + 1}`).join(", ");
          const values = sanitizeValues(Object.values(row), columnTypes);  // Sanitize the values

          console.log(`Inserting values: ${JSON.stringify(values)}`); // Log values being inserted

          const insertSQL = `INSERT INTO ${tableName} (${columns.map(c => `"${c}"`).join(", ")}) 
                             VALUES (${placeholders}) ON CONFLICT DO NOTHING;`;

          await pgClient.query(insertSQL, values);  // Use pgClient to run the insert query
        }

        console.log(`✅ Data from ${filePath} inserted successfully!`);
      } catch (error) {
        console.error(`❌ Error inserting data from ${filePath}:`, error);
      }
    });
};