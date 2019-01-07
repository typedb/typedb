# Grakn Client Python

## Dependencies
Before installing the Python `grakn` package, make sure the following dependencies are installed.

- [Grakn >= 1.3.0](https://github.com/graknlabs/grakn/releases)
- [Python >= 2.7](https://www.python.org/downloads/)
- [PIP package manager](https://pip.pypa.io/en/stable/installing/)

## Installation
```
pip install grakn
```
If multiple Python versions are available, you may wish to use
```
pip3 install grakn
```

## Quickstart
First make sure, the [Grakn server](http://dev.grakn.ai/docs/running-grakn/install-and-run#start-the-grakn-server) is running.

In the interpreter or in your source, import `grakn`.

```python
import grakn
```

Instantiate a client and open a session.

```python
client = grakn.Grakn(uri="localhost:48555")
with client.session(keyspace="mykeyspace") as session:
  ## session is open
  pass
## session is closed
```

We can also pass the credentials, as specified when [configuring authentication via Grakn Console](http://dev.grakn.ai/docs/management/users), into the client constructor as a dictionary.

```python
client = grakn.Grakn(uri="localhost:48555", credentials={"username": "<username>", "password": "<password>"})
```

Create transactions to use for reading and writing data.

```python
client = grakn.Grakn(uri="localhost:48555")

with client.session(keyspace="mykeyspace") as session:
  ## creating a write transaction
  with session.transaction(grakn.TxType.WRITE) as write_transaction:
    ## write transaction is open
    ## write transaction must always be committed (closed)
    write_transaction.commit()

  ## creating a read transaction
  with session.transaction(grakn.TxType.READ) as read_transaction:
    ## read transaction is open
    ## if not using a `with` statement, we must always close the read transaction like so
    # read_transaction.close()
```

Running basic retrieval and insertion queries.

```python
client = grakn.Grakn(uri="localhost:48555")

with client.session(keyspace="mykeyspace") as session:
  ## creating a write transaction
  with session.transaction(grakn.TxType.WRITE) as write_transaction:
    insert_iterator = write_transaction.query("insert $x isa person has birth-date 2018-08-06;")
    concepts = insert_iterator.collect_concepts()
    print("Inserted a person with ID: {0}".format(concepts[0].id))
    ## write transaction must be committed (closed)
    write_transaction.commit()

  ## creating a read transaction
  with session.transaction(grakn.TxType.READ) as read_transaction:
    answer_iterator = read_transaction.query("match $x isa person; limit 10; get;")

    ## retrieve the first answer
    a_concept_map_answer = next(answer_iterator)
    ## get the dictionary of variables : concepts, retrieve variable 'x'
    person = a_concept_map_answer.map().get("x")

    ## we can also iterate using a `for` loop
    some_people = []

    for answer in answer_iterator:
      some_people.append(answer.map().get("x"))
      break ## skip the iteration, we are going to try something else

    ## extract the rest of the people in one go
    remaining_people = answer_iterator.collect_concepts()

    ## if not using a `with` statement, then we must always close the read transaction
    # read_transaction.close()
```
**Remember that transactions always need to be closed. The safest way is to use the `with ...` syntax which auto-closes at the end of the `with` block. Otherwise, remember to call `transaction.close()` explicitly.**

To view examples of running various Graql queries using the Grakn Client Python, head over to their dedicated documentation pages as listed below.

- [Insert](http://dev.grakn.ai/docs/query/insert-query)
- [Get](http://dev.grakn.ai/docs/query/get-query)
- [Delete](http://dev.grakn.ai/docs/query/delete-query)
- [Aggregate](http://dev.grakn.ai/docs/query/aggregate-query)
- [Compute](http://dev.grakn.ai/docs/query/compute-query)

## Client Architecture
To learn about the mechanism that a Grakn Client uses to set up communication with keyspaces running on the Grakn Server, refer to [Grakn > Client API > Overview]((http://dev.grakn.ai/docs/client-api/overview).

## API Reference
To learn about the methods available for executing queries and retrieving their answers using Client Python, refer to [Grakn > Client API > Python > API Reference](http://dev.grakn.ai/docs/client-api/python#api-reference).

## Concept API
To learn about the methods available on the concepts retrieved as the answers to Graql queries, refer to [Grakn > Concept API > Overview](http://dev.grakn.ai/docs/concept-api/overview)