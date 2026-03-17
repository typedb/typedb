# Fail point

Fail points are used for testing resilience of TypeDB.
They are a mechanism to inject failures at specific points in the code.
They are only available in debug builds.

The design and implementation are based on the [fail](https://docs.rs/fail/latest/fail/) crate,
but without relying on cargo features which are poorly supported in Bazel.

To enable fail points, set the `FAILPOINTS` environment variable.
The value is a semicolon-separated list of fail point directives.

## Fail point directive format

`fail_point_name=actions`

`actions` is a sequence of actions separated by `->`.
Each action is in the format: `[frequency%][count*]task`

`frequency` is a number between 0 and 100, indicating the chance of the fail point being triggered.
Default is 100%.

`count` is a number indicating how many times the fail point can be triggered.
Default is unlimited.

`task` is one of:
- `panic` - panic
- `print` - print a message to stderr (useful for debugging)
- `off` - do nothing (only relevant in chains, provided as a silent alternative to `print`)

## Example

`RECOVERY_PARTIAL_WRITE=90%5*print->panic`: for the first 5 times `RECOVERY_PARTIAL_WRITE` is encountered,
it has a 90% chance of printing a message to stderr and 10% (the remainder) to panic.
On the sixth time, it is guaranteed to panic.
