require("dotenv").config();
const fs = require("fs");
const fyersModel = require("fyers-api-v3").fyersModel;
const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");
const winston = require("winston");
// const { createClient } = require("redis");
const _ = require("lodash");

const s3client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});
const fyers = new fyersModel({
  path: "./logs",
  enableLogging: false,
});
fyers.setAppId(process.env.FYERS_APP_ID);
fyers.setRedirectUrl(process.env.FYERS_REDIRECT_URL);
fyers.setAccessToken(process.env.FYERS_ACCESS_TOKEN);
// const client = createClient();
const logger = winston.createLogger({
  level: "error",
  format: winston.format.json(),
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
  ],
});

const FYERS_RATE_LIMIT = {
  SECOND: 10,
  MINUTE: 200,
  DAY: 10000,
};
const dates = [
  { startDate: "2017-01-01", endDate: "2017-12-31" },
  { startDate: "2018-01-01", endDate: "2018-12-31" },
  { startDate: "2019-01-01", endDate: "2019-12-31" },
  { startDate: "2020-01-01", endDate: "2020-12-31" },
  { startDate: "2021-01-01", endDate: "2021-12-31" },
  { startDate: "2022-01-01", endDate: "2022-12-31" },
  { startDate: "2023-01-01", endDate: "2023-12-31" },
  { startDate: "2024-01-01", endDate: "2024-12-31" },
];

let requestCount = 0;
let count = 1;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const applyRateLimitDelay = async () => {
  if (requestCount % FYERS_RATE_LIMIT.SECOND === 0) {
    await delay(1000);
  }
  if (requestCount % FYERS_RATE_LIMIT.MINUTE === 0) {
    await delay(60000);
  }
  if (requestCount % FYERS_RATE_LIMIT.DAY === 0) {
    throw new Error(
      "Daily Rate Limit Reached. Please run the script tomorrow with update list of stocks"
    );
  }
};

const streamToString = (stream) => new Promise((resolve, reject) => {
  const chunks = [];
  stream.on("data", (chunk) => chunks.push(chunk));
  stream.on("error", reject);
  stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
});

const updateS3Object = async (key, currentData) => {
  try {
    const s3GetObjectParams = {
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: key,
    };
    let currentStockData;
    try {
      currentStockData = await s3client.send(new GetObjectCommand(s3GetObjectParams));
    } catch (error) {
      currentStockData = error.name === 'NoSuchKey' ? { Body: JSON.stringify([]) } : (() => { throw error })();
    }
    let finalStockData = JSON.parse(typeof currentStockData.Body === "string" ? currentStockData.Body : await streamToString(currentStockData.Body));
    let finalStockDataObject = finalStockData.reduce((obj, item) => (obj[item[0]] = item, obj), {});
    currentData.forEach(value => value && value.length > 0 && !finalStockDataObject[value[0]] && finalStockData.push(value));
    finalStockData.sort((a, b) => a[0] - b[0]);
    const s3PutObjectParams = {
      Body: JSON.stringify(finalStockData),
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: key,
      ContentType: "application/json",
    };
    let res = await s3client.send(new PutObjectCommand(s3PutObjectParams));
    // await client.set(key, "UPDATED");
    console.log(`${count++}. Data updated for ${key}. Status: `, res["$metadata"].httpStatusCode);
  } catch (err) {
    console.error(err);
    logger.log({
      level: "error",
      message: `${key} data not uploaded. Status: ${JSON.stringify(res)}`,
    });
  }
};

const main = async () => {
  // await client.connect();
  const stocksMetaData = fs.readFileSync("../../equity.json", "utf8");
  const stocks = Object.keys(JSON.parse(stocksMetaData));
  for (let stock of stocks) {
    // if ((await client.get(stock)) === "UPDATED") {
    //   console.log(`${stock} data already updated`);
    //   continue;
    // }
    try {
      let stockData = [];
      for (let date of dates) {
        const input = {
          symbol: stock,
          resolution: "1D",
          date_format: "1",
          range_from: date.startDate,
          range_to: date.endDate,
          cont_flag: "1",
        };
        const data = await fyers.getHistory(input);
        if (data.s !== "ok" && data.s !== "no_data") {
          console.log(`Error in fetching data for ${stock}. Error: ${JSON.stringify(data)}`);
          logger.log({
            level: "error",
            message: `Error in fetching data for ${stock} for date range ${
              date.startDate
            } to ${date.endDate}. Error: ${JSON.stringify(data)}`,
          });
        }
        requestCount++;
        await applyRateLimitDelay();
        stockData.push(data.candles);
      }
      stockData = _.chain(stockData).flatten().compact().value();
      await updateS3Object(stock, stockData);
    } catch (err) {
      logger.log({
        level: "error",
        error: err,
        message: `Error in fetching data for ${stock}`,
      });
    }
  }
  // await client.quit();
};

main();
