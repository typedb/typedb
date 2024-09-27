### Install

**Download from TypeDB Package Repository:**

Distributions for 3.0.0-alpha-2

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.0-alpha-2```

## New Features
- **Introduce reduce stages for pipelines**
  Introduce reduce stages for pipelines to enable grouped aggregates.
  Currently implemented: count, sum max, min, mean, median, std.

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
