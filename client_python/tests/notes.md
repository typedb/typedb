pip install grakn

import grakn

g = grakn.Grakn.Grakn(...)

import grakn

g = grakn.Grakn('localhost:48555')

to test:
GraknTest():
* invalid keyspace name
* valid keyspace valid URI
* all combinations of valid/invalid URI/keyspace
* try without a server running

SessionTest():
* dont provide right enum
* correct enum with READ, WRITE, BATCH (?TODO unused?)


-- setup --
* define simple schema
* insert a couple of entities/attributes/relationships


TransactionTest():
* query (match...) -> test a query with a result, a query without a result, invalid syntax, query after tx closed

* commit -> doesn't return error
* commit-test:
  * insert something
  * commit
  * read from DB and matches expected (which we just inserted)
* commit -> check is closed

* close -> check is closed

* 


s = g.session('test')


