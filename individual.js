const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs-extra');

const createConnection = (state) => {
  return Rx.Observable.fromPromise(MongoClient.connect(state.url))
    .map(db => {
      state.db = db;
      return state;
    })
};

function findFailedIndividualResults(state) {
  const {db} = state;
  const query = {profileMatches: false};
  return Rx.Observable.fromPromise(
    db.collection("IndividualVerificationResult")
      .find(query, {limit: 60})
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
}

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
    .do(console.log)
    .reduce((acc, r) => {
      if (acc.results[r.individualVerificationId]) acc.results[r.individualVerificationId].individualVerificationProvided = r;
      return acc;
    }, state)
    .map(() => state)
}

function save(state) {
  return Rx.Observable.fromPromise(fs.writeJson('./out.json', state.results))
}


Rx.Observable.of({
  url: 'mongodb://localhost:27037/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {}
}).mergeMap(createConnection)
  .mergeMap(findFailedIndividualResults)
  .mergeMap(findIndividualRequests)
  .mergeMap(findProvidedIndividualProfiles)
  .do(save)
  .subscribe(ans => {
    // console.log(JSON.stringify(ans.results));
    ans.db.close();
  }, err => console.log(err), () => console.log("Completed"));
