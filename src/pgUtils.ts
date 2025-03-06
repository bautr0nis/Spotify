import { Client } from "pg";

// PostgreSQL Configuration
export const pgClient = new Client({
  host: process.env.PG_HOST!,
  user: process.env.PG_USER!,
  password: process.env.PG_PASSWORD!,
  database: process.env.PG_DATABASE!,
  port: Number(process.env.PG_PORT),
});

pgClient.connect();

// Function to create table in PostgreSQL
export const generateCreateTableSQL = (tableName: string, columnTypes: { [key: string]: string }) => {
  const columnsSQL = Object.entries(columnTypes)
    .map(([columnName, columnType]) => `"${columnName}" ${columnType}`)
    .join(",\n  ");

  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n  ${columnsSQL}\n);`;
};

// Function to create the table in PostgreSQL if it doesn't exist
export const createTableIfNotExists = async (tableName: string, columnTypes: { [key: string]: string }) => {
  const createTableSQL = generateCreateTableSQL(tableName, columnTypes);
  try {
    await pgClient.query(createTableSQL);
    console.log(`✅ Table "${tableName}" created or already exists!`);
  } catch (error) {
    console.error(`❌ Error creating table ${tableName}:`, error);
  }
};

// Function to insert data into PostgreSQL
export const insertDataIntoDB = async (tableName: string, rows: any[]) => {
  try {
    for (const row of rows) {
      const columns = Object.keys(row);
      const placeholders = columns.map((_, index) => `$${index + 1}`).join(", ");
      const values = Object.values(row);

      const insertSQL = `INSERT INTO ${tableName} (${columns.map(c => `"${c}"`).join(", ")}) 
                         VALUES (${placeholders}) ON CONFLICT DO NOTHING;`;

      await pgClient.query(insertSQL, values);
    }
    console.log(`✅ Data inserted into ${tableName} successfully!`);
  } catch (error) {
    console.error(`❌ Error inserting data into ${tableName}:`, error);
  }
};

export const closePgClient = () => {
  pgClient.end();
};
