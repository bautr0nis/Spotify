# Spotify Data Transformation and Upload to S3

This project processes Spotify data using Node.js, dbt for transformations, and uploads the transformed data to AWS S3.

## Prerequisites

Before running this solution, you need to ensure you have the following installed and configured:

### 1. **Node.js** (v14 or above)
   - [Download Node.js](https://nodejs.org/en/)

### 2. **PostgreSQL** (for local PostgreSQL setup, or Dockerized version)
   - [Install PostgreSQL](https://www.postgresql.org/download/)

### 3. **AWS Account with S3 Bucket**
   - You will need to set up an S3 bucket to store the transformed data.

### 4. **Python 3.8 or later** (For DBT)
   - Install dbt by following the instructions [here](https://docs.getdbt.com/dbt-cli/installation).

### 5. **Docker** (Optional, for PostgreSQL)
   - If you're using Docker to run PostgreSQL, make sure Docker is installed and running.

## Setup Instructions

### 1. Clone the repository

Clone this repository to your local machine.

### 2. Install dependencies
Install the necessary Node.js packages:
```bash
npm install
```
### 3. Set up environment variables
Create a .env file in the root directory of the project with the following values:
```bash
PG_HOST=localhost
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_DATABASE=spotifydb
PG_PORT=5432

AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_REGION=your_aws_region
AWS_BUCKET_NAME=your_s3_bucket_name
```
### 4. Running the solution
#### Step 1: Data extraction
The data extraction is done by processing files from S3 and inserting them into the PostgreSQL database.

Run the following command to process all CSV files from S3, detect their separators, and store the data in the appropriate tables inside PostgreSQL:
```bash
npx ts-node 1-Extract/processDirectory.ts
```
This script will:

- Fetch CSV files from the configured S3 bucket.
- Download the files locally if they are not already present.
- Process the files by detecting the separators.
- Insert the data into PostgreSQL tables.

#### Step 2: Data Transformation
After the data is loaded into PostgreSQL, run the following command to transform the data using DBT:
```bash
npx ts-node 2-Transformation/runDbt.ts
```
This script will:

- Activate the DBT environment.
- Run the DBT models located in the spotify_dbt folder to transform the data.

#### Attention
- Transformation can be found in spotify_dbt/models/transformation
- Views can be found in spotify_dbt/models/views


#### Step 3: Data Upload to S3
Once the data is transformed, you can upload the CSV files to S3. Run the following command:
```bash
npx ts-node 3-Load/LoadToS3.ts
```

### 5. Testing the solution
To ensure that the functions are working correctly, we have unit tests defined using Jest. To run the tests, use the following command:
```bash
npx jest
```

