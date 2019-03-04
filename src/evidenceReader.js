require('dotenv-flow').config();
const MongoClient = require('mongodb').MongoClient;
const log = require("debug")("sprinthive:mongodb");

const user = process.env.MONGODB_USERNAME || 'root';
const password = process.env.MONGODB_PASSWORD;
const authenticationDb = process.env.MONGODB_AUTHENTICATION_DATABASE;
const host = process.env.MONGODB_HOST || "mongodb";
const dbName = process.env.MONGODB_DB_NAME || "evidence";
const port = 27017;

let auth = '';
if (user && password) auth = `${user}:${password}@`;
const url = `mongodb://${auth}${host}:${port}/${authenticationDb}`;
log(`Connecting to Mongo with the url ${url}`);

async function exec(fn) {
  const client = new MongoClient(url, { useNewUrlParser: true });
  try {
    await client.connect();
    const db = client.db(dbName);
    return await fn(db);
  } finally {
    client.close();
  }
}

async function countEvidenceQuery() {
  return await exec(async db => db.collection("evidence").countDocuments());
}

async function getEvidence() {
  return await exec(async db => db.collection("evidence")
    .find({})
    // .limit(3)
    .toArray());
}


module.exports = {countEvidenceQuery, getEvidence};