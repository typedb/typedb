// the Node.js client for Grakn
// https://github.com/graknlabs/grakn/tree/master/client-nodejs
const Grakn = require("grakn");

// used for creating a stream to read the data files
// https://nodejs.org/api/fs.html#fs_fs_createreadstream_path_options
const fs = require("fs");

// for creating custom JSON processing pipelines with a minimal memory footprint
// https://github.com/uhop/stream-json
const { parser } = require("stream-json");
// to stream out assembles objects, assuming an array of objects in the JSON file
// https://github.com/uhop/stream-json/wiki/StreamArray
const { streamArray } = require("stream-json/streamers/StreamArray");
// used in conjunction with stream-json to simplify data processing
// https://github.com/uhop/stream-chain
const { chain } = require("stream-chain");

const inputs = [
  { dataPath: "./data/companies", template: companyTemplate },
  { dataPath: "./data/people", template: personTemplate },
  { dataPath: "./data/contracts", template: contractTemplate },
  { dataPath: "./data/calls", template: callTemplate }
];

// Go
buildPhoneCallGraph(inputs);

/**
 * gets the job done:
 * 1. creates a Grakn instance
 * 2. creates a session to the targeted keyspace
 * 3. loads csv to Grakn for each file
 * 4. closes the session
 */
async function buildPhoneCallGraph() {
  const grakn = new Grakn("localhost:48555"); // 1
  const session = grakn.session("phone_calls"); // 2

  for (input of inputs) {
    console.log("Loading from [" + input.dataPath + "] into Grakn ...");
    await loadDataIntoGrakn(input, session); // 3
  }

  session.close(); // 4
}

/**
 * loads the csv data into our Grakn phone_calls keyspace
 * @param {string} input.dataPath path to the data file, minus the file format
 * @param {function} input.template accepts a js object and returns a constructed Graql insert query
 * @param {object} session a Grakn session, off of which a tx will be created
 */
async function loadDataIntoGrakn(input, session) {
  const items = await parseDataToObjects(input);

  for (item of items) {
    let tx;
    tx = await session.transaction(Grakn.txType.WRITE);

    const graqlInsertQuery = input.template(item);
    console.log("Executing Graql Query: " + graqlInsertQuery);
    tx.query(graqlInsertQuery);
    tx.commit();
  }

  console.log(
    `\nInserted ${items.length} items from [${input.dataPath}] into Grakn.\n`
  );
}

function companyTemplate(company) {
  return `insert $company isa company has name "${company.name}";`;
}

function personTemplate(person) {
  const { first_name, last_name, phone_number, city, age } = person;
  // insert person
  let graqlInsertQuery = `insert $person isa person has phone-number "${phone_number}"`;
  const isCustomer = first_name;
  if (isCustomer) {
    // person is a customer
    graqlInsertQuery += ` has first-name "${first_name}"`;
    graqlInsertQuery += ` has last-name "${last_name}"`;
    graqlInsertQuery += ` has city "${city}"`;
    graqlInsertQuery += ` has age ${age}`;
  }
  graqlInsertQuery += ";";
  return graqlInsertQuery;
}

function contractTemplate(contract) {
  const { company_name, person_id } = contract;
  // match company
  let graqlInsertQuery = `match $company isa company has name "${company_name}"; `;
  // match person
  graqlInsertQuery += `$customer isa person has phone-number "${person_id}"; `;
  // insert contract
  graqlInsertQuery +=
    "insert (provider: $company, customer: $customer) isa contract;";
  return graqlInsertQuery;
}

function callTemplate(call) {
  const { caller_id, callee_id, started_at, duration } = call;
  // match caller
  let graqlInsertQuery = `match $caller isa person has phone-number "${caller_id}"; `;
  // match callee
  graqlInsertQuery += `$callee isa person has phone-number "'${callee_id}"; `;
  // insert call
  graqlInsertQuery += `insert $call(caller: $caller, callee: $callee) isa call; $call has started-at ${started_at}; $call has duration ${duration};`;
  return graqlInsertQuery;
}

/**
 * 1. reads the file through a stream,
 * 2. adds the  object to the list of items
 * @param {string} dataPath path to the data file
 * @returns items that is, a list of objects each representing a data item
 */
function parseDataToObjects(input) {
  const items = [];
  return new Promise(function(resolve, reject) {
    const pipeline = chain([
      fs.createReadStream(input.dataPath + ".json"), // 1
      parser(),
      streamArray()
    ]);

    // 2
    pipeline.on("data", function(result) {
      items.push(result.value); // 2
    });

    pipeline.on("end", function() {
      resolve(items);
    });
  });
}
