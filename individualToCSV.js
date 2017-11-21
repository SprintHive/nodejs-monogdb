const Rx = require("rxjs/Rx");
const fs = require('fs-extra');
const jsonToCSV = require('json-to-csv');

const data$ = Rx.Observable.fromPromise(fs.readJson('./out-all.json'));

function fetchData(state) {
  return data$.map(ans => {
    state.data = ans
    return state
  })
}

function setKeys(state) {
  return Rx.Observable.from(Object.keys(state.data))
    // .take(1)
    .reduce((state, key) => {
      state.out.push(state.data[key])
      return state
    }, state)
}

function mapIndividualVerificationRequested(d, matched) {
  const ans = {}
  ans.type = 'IndividualVerificationRequested'
  ans.traceId = d.traceId
  ans.identifyingNumber = d.identifyingNumber
  ans.firstName = d.firstName
  ans.middleNames = d.middleNames
  ans.lastName = d.lastName
  ans.dateOfBirth = d.dateOfBirth
  ans.deceased = d.deceased
  ans.matched = matched

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
  ans.matched = undefined
  return ans
}

function convertToCSV(state) {
  return Rx.Observable.from(state.out)
    .reduce((acc, d) => {
      const matches = d.individualVerificationResult[0] && d.individualVerificationResult[0].profileMatches
      state.out2.push(
        mapIndividualVerificationRequested(d.individualVerificationRequested, matches)
      )
      d.individualVerificationProvided.forEach(r => state.out2.push(mapIndividualVerificationProvided(r)))
      return state
    }, state)
}

function saveCSV(state) {
  return Rx.Observable.fromPromise(jsonToCSV(state.out2, "identity.csv"))
}

Rx.Observable.of({data: undefined, keys: [], out: [], out2: []})
  .mergeMap(fetchData)
  .mergeMap(setKeys)
  .mergeMap(convertToCSV)
  // .do(state => console.log(JSON.stringify(state.data, null, 2)))
  // .do(state => console.log(JSON.stringify(state.out2, null, 2)))
  .do(saveCSV)
  .subscribe(ans => console.log(ans.out.length), e => console.log(e), console.log("Complete"));