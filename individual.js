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

function findFailedIndividualResults(state) {
  const {db} = state;
  const query = {"creationDate": {"$gte": new Date("2017-10-13T12:10:40.178Z")}};
  return Rx.Observable.fromPromise(
    db.collection("IndividualVerificationResult")
      .find(query, {sort: {creationDate: -1}})
      .toArray())
    .map(d => {
      if (d.length) {
        d.forEach(r => {
          state.results[r.individualVerificationId] = {
            individualVerificationResult: r
          }
        });
      }
      return state;
    })
}                                                                                                                                    ``

function findById(db, _id, collection) {
  const query = {_id};
  return Rx.Observable.fromPromise(db.collection(collection).findOne(query))
}

function findIndividualVerificationProvided(db, individualVerificationId, collection) {
  const query = {individualVerificationId};
  return Rx.Observable.fromPromise(db.collection(collection).findOne(query))
}

function findIndividualRequests(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => findById(db, state.results[key].individualVerificationResult.individualVerificationId, "IndividualVerificationRequested"))
    .reduce((acc, r) => {
      if (acc.results[r._id]) acc.results[r._id].individualVerificationRequested = r;
      return acc;
    }, state)
    .map(() => state)
}

function findProvidedIndividualProfiles(state) {
  const {db} = state;
  return Rx.Observable.from(Object.keys(state.results))
    .mergeMap(key => findIndividualVerificationProvided(db, state.results[key].individualVerificationResult.individualVerificationId, "IndividualVerificationProvided"))
    .reduce((acc, r) => {
      if (acc.results[r.individualVerificationId]) acc.results[r.individualVerificationId].individualVerificationProvided = r;
      return acc;
    }, state)
    .map(() => state)
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
  return Rx.Observable.fromPromise(fs.writeJson('./out_identity.json', state.results))
}

Rx.Observable.of({
  url: 'mongodb://localhost:27047/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {},
  csvResults: []
}).mergeMap(createConnection)
  .mergeMap(findFailedIndividualResults)
  .mergeMap(findIndividualRequests)
  .mergeMap(findProvidedIndividualProfiles)
  // .mergeMap(convertToCSV)
  .do(saveJson)
  // .do(saveCSV)
  .subscribe(ans => {
    const count = Object.keys(ans.results).length;
    console.log(`Found ${count} records`);
    ans.db.close();
  }, err => console.log(err), () => console.log("Completed"));
