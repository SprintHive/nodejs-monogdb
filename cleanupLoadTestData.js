const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;
const ReadPreference = require('mongodb').ReadPreference;

const createConnection = (state) => {
  return Rx.Observable.fromPromise(
    MongoClient.connect(state.url, {readPreference: ReadPreference.NEAREST}))
    .map(db => {
      state.db = db;
      return state;
    })
};

function cleanup(state, collection) {
  const {db} = state;
  const query = {"creationDate": {"$lt": new Date("2017-10-13T00:00:00.000Z")}}
  return Rx.Observable.fromPromise(db.collection(collection).deleteMany(query))
    .map(d => {
      state.results[`${collection}Deleted`] = d.deletedCount || 0
      return state
    })
}

Rx.Observable.of({
  url: 'mongodb://localhost:27017/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {},
  resultsCSV: [],
  version: 2
}).mergeMap(createConnection)
  .mergeMap(state => cleanup(state, "IndividualVerificationRequested"))
  .mergeMap(state => cleanup(state, "IndividualVerificationProvided"))
  .mergeMap(state => cleanup(state, "IndividualProfileNoDataProvided"))
  .mergeMap(state => cleanup(state, "IndividualProfileErrorProvided"))
  .mergeMap(state => cleanup(state, "IndividualVerificationResult"))
  .mergeMap(state => cleanup(state, "AddressVerificationRequested"))
  .mergeMap(state => cleanup(state, "AddressProvided"))
  .mergeMap(state => cleanup(state, "AddressNoDataProvided"))
  .mergeMap(state => cleanup(state, "AddressErrorProvided"))
  .mergeMap(state => cleanup(state, "AddressVerificationResult"))
  .subscribe(ans => {
    console.log(JSON.stringify(ans.results, null, 2))
    ans.db.close();
  }, err => console.log(err), () => console.log("Completed"));



