const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;
const ReadPreference = require('mongodb').ReadPreference;
const fs = require('fs-extra');
const jsonToCSV = require('json-to-csv');

const createConnection = (state) => {
  return Rx.Observable.fromPromise(
      MongoClient.connect(state.url, {readPreference: ReadPreference.NEAREST}))
  .map(db => {
    state.db = db;
    return state;
  })
};

function countResults(state) {
  const {db} = state;
  return Rx.Observable.fromPromise(
    db.collection("AddressVerificationRequested")
      .find({"creationDate": {"$gte": new Date("2017-10-13T12:10:40.178Z")}})
      .count())
    .map(d => {
      state.count = d
      return state
    })
}

function findAddressVerificationRequested(state) {
  const {db} = state;
  return Rx.Observable.fromPromise(
      db.collection("AddressVerificationRequested")
      .find({"creationDate": {"$gte": new Date("2017-10-13T12:10:40.178Z")}}, {
        sort: {creationDate: -1}
      })
      // .limit(60)
      .toArray())
  .mergeMap(d => Rx.Observable.from(d))
  .reduce((acc, r) => {
    if (!acc.results[r._id]) {
      acc.results[r._id] = {};
    }
    acc.results[r._id].addressVerificationRequested = r;
    return acc;
  }, state)
}

function findAddressProvided(state) {
  const {db} = state;
  const keys = Object.keys(state.results);
  return Rx.Observable.fromPromise(
    db.collection("AddressProvided")
      .find({addressVerificationId: { $in: keys}})
      .toArray())
    .mergeMap(Rx.Observable.from)
    // .do(r => !state.results[r.addressVerificationId] && console.log(`Request not found for addressVerificationId: ${r.addressVerificationId}`))
    // .filter(r => state.results[r.addressVerificationId])
    .reduce((acc, r) => {
      if (!acc.results[r.addressVerificationId].addressProvided) {
        acc.results[r.addressVerificationId].addressProvided = [];
      }
      acc.results[r.addressVerificationId].addressProvided.push({
        addressVerificationResult: r
      });
      return acc;
    }, state)
}

function findAddressVerificationResult(state) {
  const {db} = state;
  const keys = Object.keys(state.results);
  return Rx.Observable.fromPromise(
      db.collection("AddressVerificationResult")
      .find({addressVerificationId: { $in: keys}})
      .toArray())
    .mergeMap(Rx.Observable.from)
    // .do(r => !state.results[r.addressVerificationId] && console.log(`Request not found for addressVerificationId: ${r.addressVerificationId}`))
    // .filter(r => state.results[r.addressVerificationId])
    .reduce((acc, r) => {
      if (!acc.results[r.addressVerificationId].addressVerificationResult) {
        acc.results[r.addressVerificationId].addressVerificationResult = [];
      }
      acc.results[r.addressVerificationId].addressVerificationResult.push({
        addressVerificationResult: r
      });
      return acc;
    }, state)
}

function convertRequested(d) {
  const matchingLines = d.addressVerificationRequested.address.addressLines
  return {type: "addressVerificationRequested",
    traceId: d.addressVerificationRequested.traceId,
    addressLine1: matchingLines[0],
    addressLine2: matchingLines[1],
    addressLine3: matchingLines[2],
    addressLine4: d.addressVerificationRequested.address.suburb,
    addressLine5: d.addressVerificationRequested.address.city,
    postCode: d.addressVerificationRequested.address.postCode,
  }
}

function convertProvided(d) {
  const ans = []
  d.addressProvided.forEach(a => {
    const traceId = a.addressVerificationResult.traceId
    a.addressVerificationResult.addressList.forEach(b => {
      ans.push({
        type: "addressProvided",
        traceId: traceId,
        addressLine1: b.line1,
        addressLine2: b.line2,
        addressLine3: b.line3,
        addressLine4: b.line4,
        addressLine5: undefined,
        postCode: b.postCode,
        originalSource: b.originalSource
      });
    })
  })
  return ans;
}

function convertResultsToCSV(state) {
  return Rx.Observable.from(Object.keys(state.results))
    .map(key => {
      return state.results[key];
    })
    .filter(d => d.addressProvided)
    .reduce((acc, d) => {
      acc.resultsCSV.push(convertRequested(d))
      acc.resultsCSV = acc.resultsCSV.concat(convertProvided(d))
      return acc;
    }, state)
}

function save(state) {
  return Rx.Observable.fromPromise(
      fs.writeJson(`./address-final-${state.version}.json`, state.results))
}

function saveCSV(state) {
  return Rx.Observable.fromPromise(jsonToCSV(state.resultsCSV, `address-final-${state.version}.csv`))
}


const eventStream = Rx.Observable.of({
  url: 'mongodb://localhost:27047/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {},
  resultsCSV: [],
  version: 2
}).mergeMap(createConnection)
// .mergeMap(countResults)
.mergeMap(findAddressVerificationRequested)
.mergeMap(findAddressProvided)
.mergeMap(findAddressVerificationResult)
.mergeMap(convertResultsToCSV)

eventStream
  .do(d => console.log(d.count))
  // .do(d => console.log(JSON.stringify(d.resultsCSV, null, 2)))
.do(save)
// .do(saveCSV)
.subscribe(ans => {
  const count = Object.keys(ans.results).length;
  console.log(`Found ${count} records`);
  ans.db.close();
}, err => console.log(err), () => console.log("Completed"));


