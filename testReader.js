const Rx = require("rxjs/Rx");

const data = require('./out-all.json');

/*
  A good stating point to process the out-all.json payload.
*/

function hasVerificationRestult(d) {
  return d.addressVerificationResult.length
}

Rx.Observable.from(Object.keys(data))
  .map(key => data[key])
  .filter(hasVerificationRestult)
  .take(3)
  .reduce((acc, d) => {
    acc.push(d)
    return acc;
  }, [])
  .subscribe(ans => {
    console.log(JSON.stringify(ans, null, 2));
    const count = ans.length;
    console.log(`Found ${count} records`);
  }, err => console.log(err), () => console.log("Completed"));
