import * as AWS from "aws-sdk";
import * as path from "path";
import * as fs from "fs";
import dotenv from "dotenv";

dotenv.config();

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  region: process.env.AWS_REGION!,
});

const BUCKET_NAME = process.env.AWS_BUCKET_NAME!;
const S3_RAW_PATH = process.env.S3_RAW_PATH!;

// Function to list CSV files in the S3 directory
export const listS3Files = async (): Promise<string[]> => {
  const params = {
    Bucket: BUCKET_NAME,
    Prefix: S3_RAW_PATH, // Directory in S3
  };

  try {
    const data = await s3.listObjectsV2(params).promise();
    return data.Contents?.filter(item => item.Key?.endsWith(".csv")).map(item => item.Key!) || [];
  } catch (error) {
    console.error("❌ Error listing files in S3:", error);
    return [];
  }
};

// Function to download files from S3 if not already downloaded
export const checkAndDownloadFiles = async (files: string[]) => {
  if (!fs.existsSync(process.env.CSV_DIRECTORY!)) {
    fs.mkdirSync(process.env.CSV_DIRECTORY!);
  }

  for (const file of files) {
    const filePath = path.join(process.env.CSV_DIRECTORY!, path.basename(file));
    if (!fs.existsSync(filePath)) {
      console.log(`⚠️ File ${file} not found locally. Downloading from S3...`);
      try {
        const params = { Bucket: BUCKET_NAME, Key: file };
        const data = await s3.getObject(params).promise();
        fs.writeFileSync(filePath, data.Body as Buffer);
        console.log(`✅ Downloaded ${file} from S3`);
      } catch (error) {
        console.error(`❌ Error downloading ${file} from S3:`, error);
      }
    } else {
      console.log(`✅ Found ${file} locally.`);
    }
  }
};