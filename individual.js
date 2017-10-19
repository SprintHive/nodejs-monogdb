const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;

const createConnection = (state) => {
  return Rx.Observable.fromPromise(MongoClient.connect(state.url))
    .map(db => {
      state.db = db;
      return state;
    })
};

function findFailedIndividualResults({state}) {
  const {db} = state;
  const query = {profileMatches: false};
  return Rx.Observable.fromPromise(
    db.collection("IndividualVerificationResult")
      .find(query)
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

function findIndividualRequests({state}) {
  const {db} = state;
  const query = {profileMatches: true};
  return Rx.Observable.fromPromise(
    db.collection("IndividualVerificationResult")
      .find(query)
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

const eventStream = Rx.Observable.of({
  url: 'mongodb://localhost:27017/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {}
}).mergeMap(createConnection)
  .mergeMap(state => findFailedIndividualResults({state}))
  .mergeMap(state => findIndividualRequests({state}));


eventStream
  .subscribe(ans => {
    console.log(JSON.stringify(ans.results));
    ans.db.close();
  }, err => console.log(err), () => console.log("Completed"));


