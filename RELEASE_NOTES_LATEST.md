Install & Run: https://typedb.com/docs/home/install

Download from TypeDB Package Repository: 

Server only: [Distributions for 2.28.3-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-server+version:2.28.3-rc0)

Server + Console: [Distributions for 2.28.3-rc0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name:^typedb-all+version:2.28.3-rc0)


## New Features


## Bugs Fixed


## Code Refactors


## Other Improvements
- **Add development-mode for local and CI set ups**

We add a new server's config parameter called `development-mode.enable`. This parameter can be used to separate real "release" application runs from our testing.

This parameter can influence many different sides of the server (logging settings, networking behavior?.., anything we want).`development-mode.enable` is an optional parameter, and the absence of it in the config is equivalent to `development-mode.enable=false`. It is expected to be set to `true` for all the `config.yml` files used by local environments, tests, and snapshots.

We add a new Bazel build flag `--//server:config=[development/release]` that helps choose the server configuration for all the build jobs, addressing the newly added `development-mode.enable` option.

Example of a release configuration build:
```
bazel build --//server:config=release //server:assemble-mac-x86_64-zip
```
Example of a development configuration build:
```
bazel build //server:assemble-mac-x86_64-zip
bazel build --//server:config=development //server:assemble-mac-x86_64-zip
```

- **We remove `deployment-id` from the `TypeDB` config as it makes sense only for `Cloud` servers.**
