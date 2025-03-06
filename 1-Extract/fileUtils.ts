import * as fs from "fs";
import * as csv from "fast-csv";

// Function to detect CSV separator dynamically
export const detectSeparator = (filePath: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { encoding: "utf8" });
    let detectedSeparator = ",";
    let linesRead = 0;
    let dataChunk = "";

    stream.on("data", (chunk) => {
      dataChunk += chunk.toString(); // Convert Buffer to string
      const lines = dataChunk.split("\n").slice(0, 5); // Read first 5 lines

      for (const line of lines) {
        if (line.includes(";")) detectedSeparator = ";";
        if (line.includes("\t")) detectedSeparator = "\t";
      }

      linesRead += lines.length;
      if (linesRead >= 5) {
        stream.destroy(); // Stop reading after detecting the separator
      }
    });

    stream.on("close", () => resolve(detectedSeparator));
    stream.on("error", (error) => reject(error));
  });
};

// Function to collect first few rows to infer column types
export const collectSampleRows = (filePath: string, separator: string): Promise<any[]> => {
  return new Promise((resolve, reject) => {
    const sampleRows: any[] = [];
    fs.createReadStream(filePath)
      .pipe(csv.parse({ headers: true, delimiter: separator }))
      .on("data", (row) => {
        if (sampleRows.length < 5) sampleRows.push(row); // Collect only first 5 rows
      })
      .on("end", () => resolve(sampleRows))
      .on("error", (error) => reject(error));
  });
};

// Function to detect column types dynamically
export const detectColumnTypes = (sampleRows: any[]): { [key: string]: string } => {
  const columnTypes: { [key: string]: string } = {};

  for (const row of sampleRows) {
    for (const [key, value] of Object.entries(row)) {
      const val = String(value).trim();

      // If the value is empty or missing, treat it as text
      if (!val || val === "") {
        columnTypes[key] = "TEXT";
      }
      // If it's a number, check if it's an integer or float
      else if (!isNaN(Number(val))) {
        // Check if the value contains a decimal point or is not an integer
        if (val.includes('.') || Number(val) !== Math.floor(Number(val))) {
          columnTypes[key] = "FLOAT";  // Use FLOAT for decimal numbers
        } else {
          columnTypes[key] = "INT";  // Use INT for whole numbers
        }
      }
      // If it's a valid date, treat it as DATE
      else if (!isNaN(Date.parse(val))) {
        columnTypes[key] = "DATE";
      } else {
        columnTypes[key] = "TEXT";
      }
    }
  }

  return columnTypes;
};