const Rx = require("rxjs/Rx");
const fs = require('fs-extra');
const jsonToCSV = require('json-to-csv');

const data = require('./address-final-2.json');

function saveCSV(data) {
  return Rx.Observable.fromPromise(jsonToCSV(data, `address-final-test.csv`))
}

function convertRequested(d) {
  const matchingLines = d.addressVerificationRequested.address.addressLines
  return {type: "addressVerificationRequested",
    traceId: d.addressVerificationRequested.traceId,
    addressLine1: matchingLines[0],
    addressLine2: matchingLines[1],
    addressLine3: matchingLines[2],
    addressLine4: undefined,
    suburb: d.addressVerificationRequested.address.suburb,
    city: d.addressVerificationRequested.address.city,
    postCode: d.addressVerificationRequested.address.postalCode,
    originalSource: undefined
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
        city: undefined,
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
    fs.writeJson('./out_address_10.json', state))
}


Rx.Observable.from(Object.keys(data))
  // .skip(1)
  // .take(1)
  .map(key => data[key])
  .filter(d => d.addressVerificationResult && d.addressVerificationResult.length)
  .take(60)
  .reduce((acc, d) => {
    acc.push(convertRequested(d));
    acc = acc.concat(convertProvided(d));
    return acc;
  }, [])
  .do(saveCSV)
  .subscribe(ans => {
    console.log(JSON.stringify(ans, null, 2));
    const count = ans.length;
    console.log(`Found ${count} records`);
  }, err => console.log(err), () => console.log("Completed"));
