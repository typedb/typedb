# Grakn Client Node.js

## Dependencies
Before installing the `grakn` node module, make sure the following dependencies are installed.

- [Grakn >= 1.3.0](https://github.com/graknlabs/grakn/releases)
- [Node >= 6.5.0](https://nodejs.org/en/download/)

## Installation
```
npm install grakn
```

## Quickstart
First make sure, the [Grakn server](http://dev.grakn.ai/docs/running-grakn/install-and-run#start-the-grakn-server) is running..

In your source, require `grakn`.

```javascript
const Grakn = require("grakn");
```

Instantiate a client and open a session.

```javascript
const client = new Grakn("localhost:48555");
const session = client.session("keyspace");
```

We can also pass the credentials, as specified when [configuring authentication via Grakn Console](http://dev.grakn.ai/docs/management/users), into the initial constructor as a Javascript object.

```javascript
const client = new Grakn("localhost:48555", { "username": "<username>", "password": "<password>" });
```

Create transactions to use for reading and writing data.

```javascript
const client = new Grakn("localhost:48555");
const session = client.session("keyspace");

// creating a write transaction
const writeTx = await session.transaction(Grakn.txType.WRITE); // write transaction is open
// to persist changes, write transaction must always be committed/closed
writeTx.commit();

// creating a read transaction
const readTx = await session.transaction(Grakn.txType.READ); // read transaction is open
// read transaction must always be closed
readTx.close();
```

Running basic retrieval and insertion queries.

```javascript
const Grakn = require("grakn");
const client = new Grakn("localhost:48555");
const session = client.session("keyspace");

async function runBasicQueries() {
  // Insert a person using a WRITE transaction
  const writeTransaction = await session.transaction(Grakn.txType.WRITE);
  const insertIterator = await writeTransaction.query("insert $x isa person;");
  const concepts = await insertIterator.collectConcepts()
  console.log("Inserted a person with ID: " + concepts[0].id);
  // to persist changes, a write transaction must always be committed (closed)
  await writeTransaction.commit();

  // Retrieve persons using a READ only transaction
  const readTransaction = await session.transaction(Grakn.txType.READ);

  // We can either query and consume the iterator lazily
  let answerIterator = await readTransaction.query("match $x isa person; limit 10; get;");
  let aConceptMapAnswer = await answerIterator.next();

  while ( aConceptMapAnswer != null) {
    // get the next `x`
    const person = aConceptMapAnswer.map().get("x");
    console.log("Retrieved person with id "+ person.id);
    aConceptMapAnswer = await answerIterator.next();
  }

  // Or query and consume the iterator immediately collecting all the results
  // - consume it all immediately
  answerIterator = await readTransaction.query("match $x isa person; limit 10; get;");
  const persons = await answerIterator.collectConcepts();
  persons.forEach( person => { console.log("Retrieved person with id "+ person.id) });

  // a read transaction must always be closed
  await readTransaction.close();

  // a session must always be closed
  await session.close();
}

runBasicQueries();
```

**Remember that transactions always need to be closed. Committing a write transaction closes it. A read transaction, however, must be explicitly closed by calling the `close()` method on it.**

To view examples of running various Graql queries using the Grakn Client Node.js, head over to their dedicated documentation pages as listed below.

- [Insert](http://dev.grakn.ai/docs/query/insert-query)
- [Get](http://dev.grakn.ai/docs/query/get-query)
- [Delete](http://dev.grakn.ai/docs/query/delete-query)
- [Aggregate](http://dev.grakn.ai/docs/query/aggregate-query)
- [Compute](http://dev.grakn.ai/docs/query/compute-query)

## Client Architecture
To learn about the mechanism that a Grakn Client uses to set up communication with keyspaces running on the Grakn Server, refer to [Grakn > Client API > Overview]((http://dev.grakn.ai/docs/client-api/overview).

## API Reference
To learn about the methods available for executing queries and retrieving their answers using Client Node.js, refer to [Grakn > Client API > Node.js > API Reference](http://dev.grakn.ai/docs/client-api/nodejs#api-reference).

## Concept API
To learn about the methods available on the concepts retrieved as the answers to Graql queries, refer to [Grakn > Concept API > Overview](http://dev.grakn.ai/docs/concept-api/overview)