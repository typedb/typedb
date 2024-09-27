### Install

**Download from TypeDB Package Repository:**

[Distributions for 3.0.0-alpha-4](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.0-alpha-4)

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.0-alpha-4```

## New Features
- **Introduce reduce stages for pipelines**
  Introduce reduce stages for pipelines to enable grouped-aggregates.
  Aggregate operators available: count, sum max, min, mean, median, std.

  Example to count persons, grouped by name
  ```text
    match $p isa person, has name $name;
    reduce $count_with_variable = count($p) within $name; 
  ```

- **Add support for ISO timezones; finalise datetime & duration support**


## Bugs Fixed
- **Fix greedy planner: variables are produced immediately**
- **Fix QueryStreamTransmitter to not lose Continue signals and hang streaming operations**
- **Fix reboot bugs, update logging**

## Other Improvements
- **Fix datetime-tz encode: send datetime as naive_utc instead of naive_local**
- **Fix datetimes: encode in seconds instead of millis**
- **Add query type to the query rows header. Implement connection/database bdd steps**
  We update the server to server the [updated protocol](https://github.com/typedb/typedb-protocol/pull/209) and return the type of the executed query as a part of the query row stream answer.
  We also fix the server's database manipulation code and implement additional bdd steps to pass the `connection/database` bdd tests.
- **Include console in 3.0 distribution**
  Includes console in 3.0 distribution. It can perform database management & run queries.
- **Fix within-transaction query concurrency**

  We update the in-transaction query behaviour to result in a predictable and well-defined behaviour. In any applicable transaction, the following rules hold:

  1. Read queries are able to run concurrency and lazily, without limitation
  2. Write queries always execute eagerly, but answers are sent back lazily
  3. Schema queries and write queries interrupt all running read queries and finished write queries that are lazily sending answers

  As a user, we see the following - reads after writes or schema queries are just fine:
  ```
  let transaction = transaction(...);
  let writes_iter = transaction.query("insert $x isa person;").await.unwrap().into_rows();
  let reads_iter = transaction.query("match $x isa person;").await.unwrap().into_rows();
  
  // both are readable:
  writes_iter.next().await.unwrap();
  reads_iter.next().await.unwrap();
  ```
  In the other order, we will get an interrupt:
  ```
  let transaction = transaction(...);
  let reads_iter = transaction.query("match $x isa person;").await.unwrap().into_rows();
  let writes_iter = transaction.query("insert $x isa person;").await.unwrap().into_rows();
  
  // only writes are still available
  writes_iter.next().await.unwrap();
  assert!(reads_iter.next().await.is_err());
  ```
