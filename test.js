require("dotenv").config();
const { S3Client, ListObjectsV2Command } = require("@aws-sdk/client-s3");
const fs = require("fs");
const s3client = new S3Client({ region: process.env.AWS_REGION });

const listObjects = async (params) => {
  try {
    const data = await s3client.send(new ListObjectsV2Command(params));
    return data;
  } catch (err) {
    console.error(err);
  }
};

const main = async () => {
  const list = await listObjects({
    Bucket: process.env.AWS_BUCKET_NAME,
  });
  const stocksMetaData = fs.readFileSync("equity.json", "utf8");
  const stocksInJson = Object.keys(JSON.parse(stocksMetaData));
  const stocksInS3 = list.Contents.map((stock) => stock.Key);
  const missingStocks = stocksInJson.filter(
    (stock) => !stocksInS3.includes(stock)
  );
  console.log(missingStocks);
};

main();
