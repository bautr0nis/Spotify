import { Client } from 'pg';
import AWS from 'aws-sdk';
import { write } from 'fast-csv';
import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

// Initialize AWS S3
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});

// PostgreSQL connection
const pgClient = new Client({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: 'spotifydb',  // Database name
  password: process.env.PG_PASSWORD,
  port: parseInt(process.env.PG_PORT || '5432'),
});

// Function to upload CSV to S3
export const uploadToS3 = async (filePath: string, fileName: string) => {
  const fileStream = fs.createReadStream(filePath);
  const params = {
    Bucket: process.env.AWS_BUCKET_NAME!,
    Key: `1 - Data/transformed/${fileName}`,
    Body: fileStream,
    ContentType: 'text/csv',
  };

  try {
    const result = await s3.upload(params).promise();
    console.log(`File uploaded successfully at ${result.Location}`);
  } catch (error) {
    console.error('Error uploading file to S3:', error);
  }
};

// Function to fetch data from PostgreSQL and convert to CSV
export const fetchDataAndUpload = async () => {
  await pgClient.connect();
  console.log('Connected to PostgreSQL');

  try {
    // Query for transform_artists
    const artistsQuery = 'SELECT * FROM transform_artists';
    const tracksQuery = 'SELECT * FROM transform_tracks';

    // Fetch data for transform_artists and transform_tracks
    const artistsResult = await pgClient.query(artistsQuery);
    const tracksResult = await pgClient.query(tracksQuery);

    // Define paths for CSV files
    const artistsCsvPath = path.join(__dirname, 'transform_artists.csv');
    const tracksCsvPath = path.join(__dirname, 'transform_tracks.csv');

    // Write transform_artists data to CSV
    const artistsCsvStream = fs.createWriteStream(artistsCsvPath);
    write(artistsResult.rows, { headers: true }).pipe(artistsCsvStream);

    // Write transform_tracks data to CSV
    const tracksCsvStream = fs.createWriteStream(tracksCsvPath);
    write(tracksResult.rows, { headers: true }).pipe(tracksCsvStream);

    // Wait for CSV files to be written to disk
    await new Promise<void>((resolve) => {
      artistsCsvStream.on('finish', () => resolve());  // Resolving with no arguments
    });

    await new Promise<void>((resolve) => {
      tracksCsvStream.on('finish', () => resolve());  // Resolving with no arguments
    });

    // Upload the CSV files to S3
    await uploadToS3(artistsCsvPath, 'transform_artists.csv');
    await uploadToS3(tracksCsvPath, 'transform_tracks.csv');

  } catch (error) {
    console.error('Error fetching data from PostgreSQL:', error);
  } finally {
    await pgClient.end();
    console.log('PostgreSQL client closed');
  }
};

// Run the function to fetch data and upload it to S3
fetchDataAndUpload();