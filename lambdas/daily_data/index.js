require("dotenv").config(); //! Remove and uninstall when uploading to lambda
const fs = require("fs");
const fyersModel = require("fyers-api-v3").fyersModel;
const sgMail = require("@sendgrid/mail");
const dayjs = require("dayjs");
const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");
const axios = require("axios");

const s3client = new S3Client({ region: process.env.AWS_REGION });
const fyers = new fyersModel({
  path: "./logs",
  enableLogging: false,
});
fyers.setAppId(process.env.FYERS_APP_ID);
fyers.setRedirectUrl(process.env.FYERS_REDIRECT_URL);
fyers.setAccessToken(process.env.FYERS_ACCESS_TOKEN);
sgMail.setApiKey(process.env.SENDGRID_API_KEY);

const VALIDATE_REFRESH_TOKEN =
  "https://api-t1.fyers.in/api/v3/validate-refresh-token";
const appIdHash =
  "995629a1f1d5ef3b14f2794adcd5fe46d8f59faf254981ee39e424f7ac8e3af4";
const FYERS_RATE_LIMIT = {
  SECOND: 10,
  MINUTE: 200,
  DAY: 10000,
};

let requestCount = 0;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const applyRateLimitDelay = async () => {
  if (requestCount % FYERS_RATE_LIMIT.SECOND === 0) {
    // console.log("Rate Limit for each second reached. Waiting for 1000ms");
    await delay(1000);
  }
  if (requestCount % FYERS_RATE_LIMIT.MINUTE === 0) {
    // console.log("Rate Limit for each second reached. Waiting for 60000ms");
    await delay(60000);
  }
  if (requestCount % FYERS_RATE_LIMIT.DAY === 0) {
    throw new Error(
      "Dailt Rate Limit Reached. Please run the script tomorrow with update list of stocks"
    );
  }
};

const sendEmail = async (mail) => {
  const msg = {
    to: "vishnuvardhanganji@gmail.com",
    from: "ganji19241a0571@grietcollege.com",
    subject: mail.subject,
    html: mail.html,
  };
  try {
    await sgMail.send(msg);
    console.log("Email sent");
  } catch (err) {
    console.error("Error sending email: ", err);
  }
};

const updateS3Object = async (key, currentData) => {
  try {
    const s3GetObjectParams = {
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: key,
    };
    let currentStockData = await s3client.send(
      new GetObjectCommand(s3GetObjectParams)
    );
    let finalStockData = JSON.parse(
      await currentStockData.Body.transformToString()
    );
    let finalStockDataObject = finalStockData.reduce((obj, item) => {
      obj[item[0]] = item;
      return obj;
    }, {});
    for (let value of currentData) {
      if (value && value.length > 0 && !finalStockDataObject[value[0]]) {
        finalStockData.push(value);
      }
    }
    finalStockData.sort((a, b) => a[0] - b[0]);
    const s3PutObjectParams = {
      Body: JSON.stringify(finalStockData),
      Bucket: process.env.AWS_BUCKET_NAME,
      Key: key,
      ContentType: "application/json",
    };
    let res = await s3client.send(new PutObjectCommand(s3PutObjectParams));
    console.log(
      `Data updated for ${key}. Status: `,
      res["$metadata"].httpStatusCode
    );
  } catch (err) {
    console.log("Error in updateS3Object: ", err);
  }
};

const getTodayData = async (symbol) => {
  const todayDate = dayjs().format("YYYY-MM-DD");
  const input = {
    symbol: symbol,
    resolution: "15",
    date_format: "1",
    range_from: todayDate,
    range_to: todayDate,
    cont_flag: "1",
  };
  try {
    const res = await fyers.getHistory(input);
    console.log(`Data fetched for: ${symbol}`);
    return res.candles;
  } catch (err) {
    console.error(`Error fetching data for: ${symbol}`, err);
  }
};

const updateData = async (stock) => {
  try {
    let stockData = await getTodayData(stock);
    requestCount++;
    await applyRateLimitDelay();
    if (stockData && stockData.length > 0) {
      await updateS3Object(stock, stockData);
    }
  } catch (err) {
    console.error(`Error during updating data for ${stock}`, err);
  }
};

const main = async () => {
  try {
    const res = await axios.post(VALIDATE_REFRESH_TOKEN, {
      grant_type: "refresh_token",
      appIdHash: appIdHash,
      refresh_token: process.env.FYERS_REFRESH_TOKEN, // Need to manually update this every 15 days
      pin: process.env.FYERS_PIN,
    });
    console.log(res.data);
    if (res.data.s === "ok" && res.data.code === 200) {
      fyers.setAccessToken(res.data.access_token);
      const stocksMetaData = fs.readFileSync("equity.json", "utf8");
      const stocks = Object.keys(JSON.parse(stocksMetaData));
      for (let stock of stocks) {
        try {
          await updateData(stock);
        } catch (err) {
          console.error(`Error updating data for ${stock}`, err);
        }
      }
      await sendEmail({
        subject: `Daily Data Updated Successfully on ${dayjs().format(
          "DD/MM/YYYY"
        )}`,
        html: `<strong>Updated data for ${stocks.length} stocks</strong>`,
      });
      return;
    }
  } catch (err) {
    // Stop code execution if refresh token is invalid or any other error occurs
    console.error("Error in main:", err);
    await sendEmail({
      subject: `Error in updaing daily data on ${dayjs().format("DD/MM/YYYY")}`,
      html: `<strong>Error in updating data</strong><br> Error: ${err}`,
    });
    return;
  }
};

// main();

exports.handler = async (event, context) => {
  try {
    await main();
  } catch (err) {
    console.error("Error in handler: ", err);
  }
};
