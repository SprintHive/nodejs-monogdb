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
    .mergeMap(results => Rx.Observable.from(results))
    .do(r => !state.results[r.addressVerificationId] && console.log(`Request not found for addressVerificationId: ${r.addressVerificationId}`))
    .filter(r => state.results[r.addressVerificationId])
    .reduce((state, r) => {
      if (!state.results[r.addressVerificationId].hasOwnProperty("addressVerificationResult")) {
        state.results[r.addressVerificationId].addressVerificationResult = [];
      }
      state.results[r.addressVerificationId].addressVerificationResult.push({
        addressVerificationResult: r
      });
      return state;
    }, state)
}

function findAddressProvided({state}) {
  const {db} = state;
  return Rx.Observable.fromPromise(
      db.collection("AddressProvided")
      .find()
      .toArray())
    .mergeMap(results => Rx.Observable.from(results))
    .do(r => !state.results[r.addressVerificationId] && console.log(`Request not found for addressVerificationId: ${r.addressVerificationId}`))
    .filter(r => state.results[r.addressVerificationId])
    .reduce((state, r) => {
      if (!state.results[r.addressVerificationId].hasOwnProperty("addressProvided")) {
        state.results[r.addressVerificationId].addressProvided = [];
      }
      state.results[r.addressVerificationId].addressProvided.push({
        addressVerificationResult: r
      });
      return state;
    }, state)
}

function save(state) {
  return Rx.Observable.fromPromise(
      fs.writeJson('./out_address.json', state.results))
}

const eventStream = Rx.Observable.of({
  url: 'mongodb://localhost:27017/dkyc-core',
  db: undefined, // used to store a reference to the db.
  results: {}
}).mergeMap(createConnection)
.mergeMap(state => findAddressVerificationRequested({state}))
.mergeMap(state => findAddressProvided({state}))
.mergeMap(state => findAddressVerificationResult({state}));

eventStream
.do(save)
.subscribe(ans => {
  const count = Object.keys(ans.results).length;
  console.log(`Found ${count} records`);
  ans.db.close();
}, err => console.log(err), () => console.log("Completed"));


