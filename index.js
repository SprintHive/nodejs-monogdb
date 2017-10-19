const Rx = require("rxjs/Rx");
const MongoClient = require('mongodb').MongoClient;

const createConnection = (state) => {
  return Rx.Observable.fromPromise(MongoClient.connect(state.url))
    .map(db => {
      state.db = db;
      return state;
    })
}

function findByIdNumber({state, collection}) {
  const {db, idNumber} = state;
  return Rx.Observable.fromPromise(
    db.collection(collection)
      .find({identifyingNumber: idNumber})
      .toArray())
    .map(d => {
      if (d.length) {
        d[0].type = collection;
        state.individualVerificationId = d[0]._id
        state.events.push(d[0]);
      }
      return state;
    })
}

function find({state, collection}) {
  const {db, individualVerificationId} = state;
  const query = {individualVerificationId}
  return Rx.Observable.fromPromise(
    db.collection(collection)
      .find(query)
      .toArray())
    .map(d => {
      if (d.length) {
        d[0].type = collection;
        state.events.push(d[0]);
      }
      return state;
    })
}

Rx.Observable.of({
  url: 'mongodb://localhost:27017/dkyc-core',
  db: undefined, // used to store a reference to the db.
  idNumber: "some-id-number", // the id number you are looking for
  individualVerificationId: undefined, // used to store the individualVerificationId
  events: [] // this is the event stream
}).flatMap(createConnection)
  .flatMap(state => findByIdNumber({state, collection: "IndividualVerificationRequested"}))
  .flatMap(state => find({state, collection: "IndividualVerificationProvided"}))
  .flatMap(state => find({state, collection: "IndividualProfileNoDataProvided"}))
  .flatMap(state => find({state, collection: "IndividualProfileErrorProvided"}))
  .flatMap(state => find({state, collection: "IndividualVerificationResult"}))
  .flatMap(state => findByIdNumber({state, collection: "AddressVerificationRequested"}))
  .flatMap(state => find({state, collection: "AddressProvided"}))
  .flatMap(state => find({state, collection: "AddressNoDataProvided"}))
  .flatMap(state => find({state, collection: "AddressErrorProvided"}))
  .flatMap(state => find({state, collection: "AddressVerificationCompleted"}))
  .subscribe(ans => {
    console.log(ans)
    ans.db.close();
  }, err => console.log(err), () => console.log("Completed"))
