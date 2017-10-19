const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;

const createConnection = (state) => {
  return Rx.Observable.fromPromise(MongoClient.connect(state.url))
  .map(db => {
    state.db = db;
    return state;
  })
};

function findAddressVerificationRequested({state}) {
  const {db} = state;
  return Rx.Observable.fromPromise(
      db.collection("AddressVerificationRequested")
      .find({}, {
        fields: {
          identifyingNumber: false,
          identityType: false,
          lastName: false
        }
      })
      .toArray())
  .map(d => {
    if (d.length) {
      d.forEach(r => {
        if (!state.results.hasOwnProperty[r._id]) {
          state.results[r._id] = {};
        }
        Object.assign(state.results[r._id], {
          addressVerificationRequested: r
        });
      });
    }
    return state;
  })
}

function findAddressVerificationResult({state}) {
  const {db} = state;
  return Rx.Observable.fromPromise(
      db.collection("AddressVerificationResult")
      .find()
      .toArray())
  .map(d => {
    if (d.length) {
      d.forEach(r => {
        if (!state.results[r.addressVerificationId].hasOwnProperty("results")) {
          console.log("ID " + r.addressVerificationId + " has no results"
              + " array, creating");
          state.results[r.addressVerificationId].results = [];
        }
        state.results[r.addressVerificationId].results.push({
          addressVerificationResult: r
        });
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
.mergeMap(state => findAddressVerificationRequested({state}))
.mergeMap(state => findAddressVerificationResult({state}));

eventStream
.subscribe(ans => {
  console.log(JSON.stringify(ans.results));
  ans.db.close();
}, err => console.log(err), () => console.log("Completed"));


