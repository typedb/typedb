**Download from TypeDB Package Repository:**

[Distributions for 3.10.4](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.10.4)

**Pull the Docker image:**

```docker pull typedb/typedb:3.10.4```

## New Features


## Bugs Fixed
- **Poll for server readiness in assembly fail point tests**
  
  Fix nondeterministic failures in the failpoints test.
  
  
- **Fix flaky statistics underflow**
  
  We fix a flaky (occasional `concept` bdd failures) test due to panics in statistics counting. In particular, it occurred in schema transactions that wrote both deleted types and their instances. By doing this in the wrong order, we got an underflow panic.
  
  We also make the server resilient to underflow panics/crashes by logging an error to the server log and sentry (if enabled) with a new macro. We consider this a 'tolerable' error which shouldn't get you into a crash situation.
  
  

## Code Refactors
- **Add tests for statistics synchronization**
  
  Add more statistics integration tests, as well as a new "load" test, which does a small concurrent, batched load and does a reboot, verifying that all statistics are counted correctly.
  
  
- **Reject query pipelines exceeding 1000 stages**
  
  Add MAX_PIPELINE_STAGES limit enforced in translate_pipeline, covering read, write, and analyse paths. New QueryError::PipelineStagesLimitExceeded variant reports the offending stage count.
  
  
- **Checkpoint statistics after startup recovery**
  
  Improve subsequent boot time by checkpointing statistics on reboot. This in particular alleviates slow bootup caused by large commit records in the WAL that may be scanned as part of the initial synchroniziation process. Without this change, the server pays the same cost of scanning data records on every reboot, until a "large enough" modification of the data occurs.
  
  
- **Optimize CI build job by reducing cached artifacts download**
  
  We use the `--remote_download_minimal ` in the `ci` config in Bazel, which reduces the number of artifacts downloaded during a build job in FactoryCI.
  
  In particular, this speeds up jobs where little/nothing has changed by hugely reducing the amount of data downloaded. It could also speed up test jobs by reducing number of downloaded artifacts, but the effects seem to be less dramatic.
  
  Have observed a build job of around 8-12 minutes now, instead of up to 20 or 30.
  
  

## Other Improvements
- **Update Rust dependencies**
  
  
- **Remove parallelism and memory options from .bazelrc**

- **Bazel java runtime is hermitic (remote) - used for notes creation**
