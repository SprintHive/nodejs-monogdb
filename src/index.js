const fs = require("fs");
const {countEvidenceQuery, getEvidence} = require("./evidenceReader");

getEvidence()
  .then(ans => {
    const whiteList = ["7811145141084","7604275732180","8406040187081"];
    const key = "za-id-data";
    const data = ans
      .filter(e => e.enrichment[key])
      .map(e => JSON.parse(e.enrichment[key]))
      .filter(zaIdData => whiteList.indexOf(zaIdData.idNumber !== -1));

    fs.writeFileSync("./out/forjz.json", JSON.stringify(data))
  }).catch(err => console.error(err));


const a = [
  {id: 1},
  {id: 2},
  {id: 3},
  {id: 4},
  {id: 5},
  {id: 6},
  {id: undefined},
  {id: null},
];

function addIndex({id}, i) {
  return id + i
}
console.log(a);

const b = a
  .filter(a => a.id)
  .map(addIndex);
console.log(b);