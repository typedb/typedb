// a Node.js client for Grakn
// https://github.com/graknlabs/grakn/tree/master/client-nodejs
const Grakn = require("grakn");

// used for creating a stream to read the data files
// https://nodejs.org/api/fs.html#fs_fs_createreadstream_path_options
const fs = require("fs");

// CSV (or delimited text) parser
// https://github.com/mholt/PapaParse
const papa = require("papaparse");

// template functions that construct a Graql insert query using the given data object
const customerTemplate = require("./templates/customer");
const personTemplate = require("./templates/person");
const callTemplate = require("./templates/call");

// input required to parse the data
// the objects are passed to constructGraqlInsertQueries(input) via a loop
const parsingInput = [
  {
    fileName: "customers",
    template: customerTemplate
  },
  {
    fileName: "people",
    template: personTemplate
  },
  {
    fileName: "calls",
    template: callTemplate
  }
];
// the Grakn keyspace into which the data is loaded
const keyspace = "phone_calls";

// Go!
loadCsvIntoGrakn(parsingInput, keyspace);

/**
 * loads the csv data into the Grakn keyspace
 * 1. gets the constructGraqlInsertQueries()
 * 2. passes it to runGraqlInsertQueries()
 * @param {Object[]} parsingInput
 * @param {string} keyspace
 * exists after completion or terminates in case of failure
 */
async function loadCsvIntoGrakn(parsingInput, keyspace) {
  const transaction = await createGraknTransaction(keyspace);

  // for a check later, to determine a commit should be made
  let isInterrupted = false;

  for (input of parsingInput) {
    let graqlInsertQueries;
    try {
      graqlInsertQueries = await constructGraqlInsertQueries(input); // 1
      await runGraqlInsertQueries(
        transaction,
        graqlInsertQueries,
        input.fileName // this one's not important. it's just for the console.log()
      ); // 2
      console.log(
        "\nSuccessfully loaded [" + input.fileName + "] data into Grakn\n"
      );
    } catch (error) {
      isInterrupted = true;
      console.log(
        "\nLoading the csv data into Grakn was interrupted:\n",
        error
      );
      // close the transaction and exit with error
      transaction.close();
      process.exit(1);
    }
  }

  // no commit, unless all queries have run successfully
  if (!isInterrupted) {
    await transaction.commit();
    process.exit();
  }
}

/**
 * for every item in graqlInsertQueries, it runs the query
 * @param {object} transaction created in createGraknTransaction(), used here and will close on error
 * @param {string[]} graqlInsertQueries put together in constructGraqlInsertQueries()
 * @param {string} fileName // just for console.log()
 */
async function runGraqlInsertQueries(
  transaction,
  graqlInsertQueries,
  fileName
) {
  console.log("Loading [" + fileName + "] data into Grakn ...");
  for (const graqlQuery of graqlInsertQueries) {
    console.log("Executing Graql query: " + graqlQuery);
    await transaction.query(graqlQuery);
  }
}

/**
 * 1. reads the file through a stream,
 * 2. parses the csv line to an object
 * 3. passes the object to parsingInput.template(lineData)
 * 4. gets the constrcuted Graql insert query in return
 * 5. adds it to graqlInsertQueries
 * @param {object} parsingInput parsingInput.template(lineData)
 * @param {string} parsingInput.fileName name of the data file, minus the path and format
 * @param {object[]} parsingInput.template a function that returns the Graql insert query,
 * based on data passed to it
 * @returns graqlInsertQueries that is, the list of Graql insert queries to run on
 * the Grakn keyspace
 */
function constructGraqlInsertQueries(parsingInput) {
  const graqlInsertQueries = [];
  return new Promise(function(resolve, reject) {
    papa.parse(
      fs.createReadStream("./data/" + parsingInput.fileName + ".csv"), // 1
      {
        header: true, // a Paparse config option
        // 2
        step: function(results, parser) {
          if (results.errors.length) {
            // in case of an error, the parset pauses and
            // the promise rejects with the error
            parser.pause();
            reject(
              "Failed to parse csv line: \n" + JSON.stringify(results.errors[0])
            );
          }
          const graqlQuery = parsingInput.template(results.data[0]); // 3 and 4
          graqlInsertQueries.push(graqlQuery); // 5
        },
        complete: function() {
          resolve(graqlInsertQueries);
        },
        error: function(error) {
          reject(
            "Failed to read file for [" + parsingInput.fileName + "]: " + error
          );
        }
      }
    );
  });
}

/**
 * creates a transaction of a session made to connect to the Grakn keyspace
 * @param keyspace {string}
 * @returns a transaction that will be used for executing the Graql insert queries
 */
async function createGraknTransaction(keyspace) {
  const grakn = new Grakn("localhost:48555");
  const session = grakn.session(keyspace);
  let transaction;
  try {
    transaction = await session.transaction(Grakn.txType.WRITE);
    console.log("Transaction was created successfullly \n");
  } catch (error) {
    console.log("Failed to create transaction: \n", error);
  }
  return transaction;
}
