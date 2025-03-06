import { listS3Files, checkAndDownloadFiles } from './s3Utils';  // Import functions from s3Utils
import { createTableIfNotExists, insertDataIntoDB, closePgClient, pgClient } from './pgUtils';  // Import PostgreSQL functions including pgClient
import { detectSeparator } from './fileUtils';  // Import file handling functions
import { processCSV } from './processCSV';  // Import processCSV from the new file
import * as path from 'path';  // Path module for file handling
import dotenv from 'dotenv';

dotenv.config();

// Main function to process all files
const processAllFiles = async () => {
  try {
    // Get all CSV files from S3
    const files = await listS3Files();

    if (files.length === 0) {
      console.log("❌ No CSV files found in the S3 bucket.");
      return;
    }

    // Ensure files exist locally or download them
    await checkAndDownloadFiles(files);

    // Process each file dynamically
    for (const file of files) {
      const filePath = path.join(process.env.CSV_DIRECTORY!, path.basename(file));
      const tableName = path.basename(file, ".csv");

      console.log(`🔍 Detecting separator for ${file}...`);
      const detectedSeparator = await detectSeparator(filePath);
      console.log(`✅ Detected separator for ${file}: "${detectedSeparator}"`);

      // Process the CSV file and wait for it to finish
      await processCSV(filePath, tableName, detectedSeparator);
    }

    console.log("🎉 All files processed successfully!");
  } catch (error) {
    console.error("❌ Error processing files:", error);
  } finally {
    // Ensure the client is closed after all operations are completed
    console.log("Closing PostgreSQL client...");
    // Ensure that pgClient.end() is called after all insertions are completed
    await pgClient.end();  // Wait for pgClient to finish before ending
  }
};

// Run the script
processAllFiles();