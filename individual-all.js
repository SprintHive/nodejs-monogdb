const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;
const ReadPreference = require('mongodb').ReadPreference;
const fs = require('fs-extra');

const createConnection = (state) => {
  return Rx.Observable.fromPromise(
      MongoClient.connect(state.url, {readPreference: ReadPreference.NEAREST}))
    .map(db => {
      state.db = db;
      return state;
    })
};

function findAllIndividualRequests(state) {
  const {db} = state;
  const query = {"creationDate": {"$gte": new Date("2017-10-13T12:10:40.178Z")}};
  return Rx.Observable.fromPromise(
    db.collection("IndividualVerificationRequested")
      .find(query, { sort: {creationDate: -1}})
      .toArray())
    .map(d => {
      if (d.length) {
        d.forEach(r => {
          state.results[r._id] = {
            individualVerificationRequested: r
          }
        });
      }
      return state;
    })
}                                                                                                                                    ``

function queryCollection(db, key, id, collection) {
  const query = {};
  query[key] = id
  return Rx.Observable.fromPromise(db.collection(collection)
    .find(query)
    .toArray())
    .map(data => {
      const ans = {data}
      ans[key] = id
      return ans
    })
}

function findIndividualProvided(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => {
      return queryCollection(db, "individualVerificationId", state.results[key].individualVerificationRequested._id, "IndividualVerificationProvided")
    })
    .reduce((acc, r) => {
      if (acc.results[r.individualVerificationId]) acc.results[r.individualVerificationId].individualVerificationProvided = r.data;
      return acc;
    }, state)
}


function findIndividualVerificationResult(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => {
      return queryCollection(db, "individualVerificationId", state.results[key].individualVerificationRequested._id, "IndividualVerificationResult")
    })
    .reduce((acc, r) => {
      if (acc.results[r.individualVerificationId]) acc.results[r.individualVerificationId].individualVerificationResult = r.data;
      return acc;
    }, state)
}

function findAddressVerificationRequested(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => {
      return queryCollection(db, "_id", state.results[key].individualVerificationRequested._id, "AddressVerificationRequested")
    })
    .reduce((acc, r) => {
      if (acc.results[r._id]) acc.results[r._id].addressVerificationRequested = r.data;
      return acc;
    }, state)
}

function findAddressProvided(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => {
      return queryCollection(db, "addressVerificationId", state.results[key].individualVerificationRequested._id, "AddressProvided")
    })
    .reduce((acc, r) => {
      if (acc.results[r.addressVerificationId]) acc.results[r.addressVerificationId].addressProvided = r.data;
      return acc;
    }, state)
}

function findAddressVerificationResult(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => {
      return queryCollection(db, "addressVerificationId", state.results[key].individualVerificationRequested._id, "AddressVerificationResult")
    })
    .reduce((acc, r) => {
      if (acc.results[r.addressVerificationId]) acc.results[r.addressVerificationId].addressVerificationResult = r.data;
      return acc;
    }, state)
}

function mapIndividualVerificationRequested(d) {
  const ans = {}
  ans.type = 'IndividualVerificationRequested'
  ans.traceId = d.traceId
  ans.identifyingNumber = d.identifyingNumber
  ans.firstName = d.firstName
  ans.middleNames = d.middleNames
  ans.lastName = d.lastName
  ans.dateOfBirth = d.dateOfBirth
  ans.deceased = d.deceased

  return ans
}

function mapIndividualVerificationProvided(d) {
  const ans = {}
  ans.type = 'IndividualVerificationProvided'
  ans.traceId = d.traceId
  ans.providerIdentityNumber = d.providerIdentityNumber
  ans.firstName = d.firstName
  ans.middleNames = d.middleNames
  ans.lastName = d.lastName
  ans.dateOfBirth = d.dateOfBirth
  ans.deceased = d.deceased

  return ans
}

function convertToCSV(state) {
  return Rx.Observable.from(state.results)
    .map(d => {
      state.csvResults.push(mapIndividualVerificationRequested(d.individualVerificationRequested))
      state.csvResults.push(mapIndividualVerificationProvided(d.individualVerificationProvided))
      return state
    })
}

function saveCSV(state) {
  return Rx.Observable.fromPromise(jsonToCSV(state.csvResults, "identity.csv"))
}

function saveJson(state) {
  return Rx.Observable.fromPromise(fs.writeJson('./out-all.json', state.results))
}

Rx.Observable.of({
  url: 'mongodb://localhost:27017/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {},
  csvResults: []
}).mergeMap(createConnection)
  .mergeMap(findAllIndividualRequests)
  .mergeMap(findIndividualProvided)
  .mergeMap(findIndividualVerificationResult)
  .mergeMap(findAddressVerificationRequested)
  .mergeMap(findAddressProvided)
  .mergeMap(findAddressVerificationResult)
  // .mergeMap(convertToCSV)
  .do(saveJson)
  // .do(saveCSV)
  .subscribe(ans => {
    const count = Object.keys(ans.results).length;
    console.log(`Found ${count} records`);
    ans.db.close();
  }, err => console.log(err), () => console.log("Completed"));
