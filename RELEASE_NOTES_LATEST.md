**Download from TypeDB Package Repository:**

[Distributions for 3.8.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.8.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.8.0```


## New Features
- **Implement date/time and string binary operators**
  
  We implement addition and subtraction operators for `date`, `datetime`, `datetime_tz`, and `duration`.
  
  We also implement string concatenation, also performedd with the `+` operator.
  
  
- **Implement built-in functions length(), iid(), and label()**
  
  - `len()`: string length in codepoints / unicode scalar values (USVs): `len("TypeDB") == 6`, `len("こんにちは") == 5`, `len("⭐") == 1`, `len("❤️‍🔥") == 4`;
  - `iid()`: an instance's internal ID as a string, to be used in an `iid` constraint in following queries;
  - `label()`: a type's label as a string, for convenience in fetch queries.
  
  

## Bugs Fixed
- **Tag parameters with value type**
  
  We tag extracted parameters in queries with their value type. Previously the plan cache would not be able to distinguish queries with integer literals from ones with datetime literals, e.g., which would cause it to retrieve a wrong compiled plan from the cache, causing a server crash.
  
  
- **Always regenerate indices for modified relations on schema commit**
  #7594  changed relation-index behaviour so schema transactions avoid reading or writing to relation-indices - and generate indices at commit time for newly inserted relations, or relations of types that became eligible for indices as part of schema modifications. 
  
  
- **Fix division error checking so a zero result is not considered an error**
  Division relied on `f64::is_normal`. Now, only `FpCategory::Inf` and `FpCategory::Nan` are considered errors.
  This is checked by `f64::is_finite`
  
- **Fix Sentry crash reporting by updating dependencies**
  Update dependencies https://github.com/typedb/typedb-dependencies/pull/595 to use the `rustls` feature of `sentry`, allowing it to always send reports to the protected sentry endpoint. Fixes https://github.com/typedb/typedb/issues/7650.
  
  The ureq HTTP transport used by Sentry requires a TLS backend to make HTTPS requests to sentry.io. Our sentry dependency was configured with `default-features = false` and the ureq feature, which, following the description on the [official docs page](https://crates.io/crates/sentry/0.36.0), enabled `rustls` by default. If built using Bazel, this indeed works correctly. However, when built through Cargo, no actual reports could be sent to the Sentry endpoint, as `rustls` [is not enabled](https://github.com/getsentry/sentry-rust/blob/0.36.0/sentry/Cargo.toml#L81) for `ureq` by default (default features, including `rustls`, are turned off).
  
  
- **Annotations can use losslessy castable arguments**
  
  We fix two panics that arose when using castable values (eg. integers range annotations for decimal value types) in parameters to annotations, such as a `decimal` that used `integer` arguments: `person owns salary @values(10)` or `person owns salary @range(0..1000000);`.
  
  We make consistent the behaviour that:
  
  - Integers are losslessly castable to Decimal and Double value types
  - Date is lossless castable to DateTime
  
  We have also updated BDD tests to reflect that doubles are no longer losslessly castable to decimals, since they have different precision.
  
  

## Code Refactors


## Other Improvements
- **Introduce bazel ci config setting**
  
  We add a bazel config profile `ci`, which does not use a local disk cache. Local disk caches can help speed up builds, especially when switching branches or creating new worktrees. However, they can be very large and cause CI systems to run out of disk space, while also not providing any benefits (roughly building once per CI job, and already using the global networked cache).
  
  
- **Add bazel disk cache to speed up local builds when switching branches**

- **Change "newsletter" to "blog" in README**

    
