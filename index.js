const fs = require("fs");
const { parse } = require("csv-parse");
const ObjectsToCsv = require("objects-to-csv");

const buythedip = [];
const capital = [];

const csvToObject = (data, headers) => {
  return data.map((item) => {
    const obj = {};
    headers.forEach((h, index) => (obj[h] = item.at(index)));
    return obj;
  });
};

fs.createReadStream("./buythedip.csv")
  .pipe(parse({ delimiter: ",", from_line: 1 }))
  .on("headers", (headers) => {
    console.log(`First header: ${headers[0]}`);
  })
  .on("data", function (row) {
    buythedip.push(row);
  })
  .on("end", function () {
    const buythedip_headers = buythedip[0];
    buythedip.shift();

    const buythedip_objects = csvToObject(buythedip, buythedip_headers);

    fs.createReadStream("./capital.csv")
      .pipe(parse({ delimiter: ",", from_line: 1 }))
      .on("data", function (row) {
        capital.push(row);
      })
      .on("end", function () {
        const capital_headers = capital[0];
        capital.shift();
        const capital_objects = csvToObject(capital, capital_headers);

        console.log("capital_objs", capital_objects[0]);
        console.log("buythedip_objects", buythedip_objects[0]);
        const finalCSV = [];
        for (let i = 0; i < buythedip_objects.length; i++) {
          const bdd = buythedip_objects[i];
          const capital = capital_objects.find(
            (c) =>
              c.date == bdd.date && c.symbol == bdd.symbol && c.days == bdd.days
          );
          let result = {
            symbol: bdd.symbol,
            days: bdd.days,
            bdd_ret: bdd.ret,
            bdd_price: bdd.price,
            bdd_max_dd: bdd.max_dd,
            bdd_recovery: bdd.recovery,
            bdd_std: bdd.std,
            bdd_ma20: bdd.ma20,
            bdd_return_adjusted_ranking: bdd.return_adjusted_ranking,
            bdd_decile: bdd.decile,
          };

          if (capital) {
            const newCapital = {
              capital_ret: capital.ret,
              capital_max_dd: capital.max_dd,
              capital_std: capital.std,
              capital_price: capital.price,
              capital_ma20: capital.ma20,
              capital_ma50: capital.ma50,
              capital_std_rank: capital.std_rank,
              capital_max_dd_rank: capital.max_dd_rank,
              capital_avg_rank_std_max_dd: capital.avg_rank_std_max_dd,
              capital_return_rank: capital.return_rank,
              capital_avg_rank_return_std_max_dd:
                capital.avg_rank_return_std_max_dd,
              capital_decile: capital.decile,
            };

            result = { ...result, ...newCapital };
          } else {
            result = {
              ...result,
              capital_ret: "N/A",
              capital_max_dd: "N/A",
              capital_std: "N/A",
              capital_price: "N/A",
              capital_ma20: "N/A",
              capital_ma50: "N/A",
              capital_std_rank: "N/A",
              capital_max_dd_rank: "N/A",
              capital_avg_rank_std_max_dd: "N/A",
              capital_return_rank: "N/A",
              capital_avg_rank_return_std_max_dd: "N/A",
              capital_decile: "N/A",
            };
          }

          finalCSV.push(result);
        }

        const csv = new ObjectsToCsv(finalCSV);

        csv.toDisk("./res.csv");
      });
  });
