# Grakn's intro guide to [Bazel](https://bazel.build)

This guide serves as a _"quick crash course"_ on the primary concepts of Bazel, stitched together to form _"a high-level picture of how everything comes together"_. This guide is not a replacement of [Bazel's comprehensive documentation](https://docs.bazel.build/versions/master/bazel-overview.html).


## Setup
First things first, install Bazel as instructed in [Bazel installation docs](https://docs.bazel.build/versions/master/install.html). At Grakn, we recommend [installing using Homebrew](https://docs.bazel.build/versions/master/install.html).

### Working with an IDE

Bazel supports various IDEs, which you can find out more in [their docs](https://docs.bazel.build/versions/master/ide.html). At Grakn, we recommend using IntelliJ, and setting up [Bazel's IntelliJ Plugin](https://plugins.jetbrains.com/plugin/8609-bazel) is quite simple. Once you have a Bazel project (i.e. a project built with Bazel `WORKSPACE` and `BUILD` files explained below, such as [graknlabs/grakn](https://github.com/graknlabs/grakn)), then:
1. Install Bazel's Intellij plugin by going to `Settings > Plugins > Install > Browse Repositories`, search for 'Bazel', install the plugin, and restart.
2. Set the correct binary location for Bazel by going to `Preferences > Bazel Settings` and set `Bazel binary location` to the value you get when you run `which bazel` in your terminal (e.g. `/usr/local/bin/bazel`)
3. Import Bazel project from the welcome page, or go to  `File > Import Bazel project`.
4. Select `Create from scratch`

---

## Bazel builds a "workspace" of "packages"
Bazel builds software from source code organised in a directory, called a _"workspace"_. A "workspace" is equivalent to a "repository". Source files in a workspace are organised in a tree/nested hierarchy of "packages", i.e. **a workspace contains many packages, nested in each other**.

A workspace is defined by a `WORKSPACE` file, and package is defined by a `BUILD` file. Where the `WORKSPACE` file sits denotes the root of the workspace directory, and where the `BUILD` file sits denotes the root of the package directory.

---

## The Language: Starlark

Both `WORKSPACE` and `BUILD` files are written in an imperative programming language: [Starlark (renamed from Skylark)](https://docs.bazel.build/versions/master/skylark/language.html). The syntax is a subset of Python, but designed to be small, simple, and thread-safe. 

Everything that you write in `WORKSPACE` and `BUILD` files are just functions in Starlark. However, Bazel may refer to them as either _"rules"_ or _"functions"_.
- Bazel _"rules"_ are Starlark functions to instruct the Bazel build system on how to build a set of input to a set of outputs, e.g. sources to jars. There are [Workspace Rules](https://docs.bazel.build/versions/master/be/workspace.html) for the `WORKSPACE` file, and there are [Build Rules](https://docs.bazel.build/versions/master/be/overview.html) for the `BUILD` file.
- [Bazel _"functions"_](https://docs.bazel.build/versions/master/be/functions.html) are also Starlark functions, that serves the role as "helper functions" for programmatic tasks in `WORKSPACE` or `BUILD` files.

> In general, like any imperative language (e.g. Java), the order of statements matter. However, `BUILD` files often consist only of declarations of build rules, and their relative order does not matter. What rules are declared when package evaluation completes matters.

---

## The Workspace: repository with a `WORKSPACE` file

A _"workspace"_ is defined by a `WORKSPACE` file sitting at the root directory of the repository. In the `WORKSPACE` file, we declare **ALL OF THE** _"external dependencies"_ of the repository - explicit and transitive dependencies. You can think of the `WORKSPACE` file as the _"gateway"_ for all external dependencies to be declared once across your entire workspace, and with one version only.

> _For projects with a large number of dependencies, declaring all the dependencies explicitly might appear to be a nightmare. We will show how to automate this (using [bazel-deps](https://github.com/johnynek/bazel-deps)). Managing your dependencies explicitly, which is how it's done with Bazel, is the best way to mantain a lean and robust set of dependencies. This is one of the many reasons we fell in love with Bazel._

The first thing you need to do in a `WORKSPACE` file, is to declare your workspace name with the [`workspace(...)` function](https://docs.bazel.build/versions/master/be/functions.html#workspace):
```
workspace(name = `grakn_core`)
```

Then you declare external dependencies, explained in the following section.

You can check the [`WORKSPACE`](https://github.com/graknlabs/grakn/blob/master/WORKSPACE) file at [graknlabs/grakn](https://github.com/graknlabs/grakn) to see the full example.

### Workspace Rules: to declare external dependencies

Bazel allows you to declare external dependencies, which are treated as a special type of workspace: a _"remote workspace"_. There are various formats of dependencies. You can find out more on how to declare them in the [Workspace rules docs](https://docs.bazel.build/versions/master/be/workspace.html). We will just briefly cover 2 examples here to introduce you to the concept, but we will dive deeper into external dependency management in a later section. 

> _Note: Workspace names cannot accept hyphens (`-`). Thus, use underscores (`_`) instead._

Git repository dependency: `git_repository`
```
git_repository(
    name = "rules_antlr",
    remote = "https://github.com/marcohu/rules_antlr",
    tag = "0.1.0"
)
```
As simple as that, we've imported `marcohu/rules_antlr` github project as an external dependency.

Maven artifact dependency: `maven_jar`
```
maven_jar(
    name = "com_google_guava_guava",
    artifact = "com.google.guava:guava:18.0"
)
```
Declaring a maven dependency is also simple.

### Labels: to refer to workspaces

A label is defined to be: `@` + `workspace_name` + `//` + `package-name` + `:` + `target`.

We will discuss `package-name:target` in _"The Packages"_ section below. For now, all you need to know is that both ANTLR and Guava is be available to use in your `BUILD` files, and you can refer to their packages as `@rules_antlr//package-name` and `@com_google_guava_guava//package-name`. 

If a label does not contain `@workspace_name`, e.g. just `//package-name`, that means the label refers to a package within the current workspace/repository.

### Macros: to manage workspace rules

As your list of dependencies grows, you will want to manage them in a more modular way (encapsulated and reusable). This can be done by creating _"macros"_, which are Starlark functions, written in a separate `.bzl` file which you can `load` into the `WORKSPACE` file (and `BUILD` file for that matter).

For example, in `graknlabs/grakn`, we encapsulate the dependency declaration for `@com_github_grpc_grpc` and `@org_pubref_rules_proto` as `grpc_dependencies()` in a separate file [`dependencies/compilers/dependencies.bzl`](https://github.com/graknlabs/grakn/blob/master/dependencies/compilers/dependencies.bzl):
```
def grpc_dependencies():
    native.git_repository(
        name = "com_github_grpc_grpc",
        remote = "https://github.com/graknlabs/grpc",
        commit = "da829a5ac902ab99eef14e6aad1d8e0cd173ec64"
    )

    native.git_repository(
        name = "org_pubref_rules_proto",
        remote = "https://github.com/pubref/rules_proto",
        commit = "f493ce70027f353cd6964339018163207393ba93",
    )
```

Then in the [`WORKSPACE`](https://github.com/graknlabs/grakn/blob/master/WORKSPACE) file can be as simple as:
```
workspace(name = `grakn_core`)

load("//dependencies/compilers:dependencies.bzl", "grpc_dependencies")
grpc_dependencies()
```

_(The syntax of `//dependencies/compilers:dependencies.bzl` will be clear when we discuss package labels in the section below.)_

At this point, the workspace (which you can see in [`graknlabs/grakn`](https://github.com/graknlabs/grakn)) should look like:
```
grakn_core/
├── WORKSPACE
├── dependencies/
│   └── compilers/
│       └── dependencies.bzl
├── ...
```

> _Even with macros, managing transitive dependencies explicitly will still be painful, especially for large projects. We'll show you how to automate this using [bazel-deps](https://github.com/johnynek/bazel-deps) while still maintain full/explicit control over transitive dependencies._

### Output directories: `bazel-*`

When you run Bazel, it outputs binaries/generated files to several different directories, all sitting at the root of your workspace. Each of these directories are `symlinks` to a target-specific directory in your filesystem (`/private/var/tmp/...`), which serves as the cache to the Bazel server.
```
grakn_core/
├── WORKSPACE
├── bazel-bin -> ...
├── bazel-genfiles -> ...
├── bazel-my_workspace -> ...
├── bazel-out -> ...
├── bazel-testlogs -> ...
└── ...
```

You can learn more about them on [Bazel output directory docs](https://docs.bazel.build/versions/master/output_directories.html).

> _Note: Make sure to add `bazel-*` to your `.gitignore`._
> _Note: We will ignore these output directories in further examples._

---

## The Packages: directories with a `BUILD` file

In Bazel, the primary unit of code organisation in a workspace is a "package", which is a directory within the workspace that contains a `BUILD` file. The package includes all files in its directories and sub-directories, except for directories that have their own `BUILD` file.
```
grakn_core/
├── WORKSPACE
└── app/
    ├── BUILD
    ├── app.cc
    ├── data/
    │   └── input.txt
    └── test/
        ├── BUILD
        └── test.cc
```

In the example above, `app` and `app/tests` are the 2 packages in the workspace, because they have a `BUILD` file in their directory. `app/data` is not a package because it does not have `BUILD` file, and thus, it belongs to the `app` package.

### Targets: contents of a package

A _"package"_ is comprised of 3 things: _files_, _rules_, and _package groups_. We refer to them as _"targets"_.

- **Files** could either be _"source files"_ or _"generated files"_. _Source files_ are files that are manually written, and _generated files_ are automatically generated by a program (also referred to as _"derived files"_).
- **Rules** are declared in the `BUILD` file (explained more below). Rules are instructions to the build process, by specifying the input and output files, and what to do with them.
- **Package groups** are also declared in the `BUILD` file (explained more below). Package groups are sets of packages whose purpose is to limit accessibility of certain rules.

### Labels: to refer to packages and targets

A label is defined to be: `@` + `workspace_name` + `//` + `package-name` + `:` + `target`

`//` marks the workspace root directory.

When `@workspace_name` is not provided, that means the label refers to a package and target within the current workspace/repository.

`package-name` is the name of the directory containing a BUILD file, relative to the root directory of the workspace. I.e. `package-name` is `path/to/package` from the repository root directory where `WORKSPACE` sits in. A `package-name` can never start with `//`, nor can they end with `/`.

`target` is the name of a target in package, i.e. a file, rule or package-group. By default, we refer to a target by writing `//package-name:target`. But we can also refer to targets without `//path/to/package`, i.e. just `:target` (called "relative label"), from within the same package. If the target is a file (rather than a rule), we refer to it as just `target` (without `:`) from within the same package, e.g. `testdata/input.txt`.

Labels can also be used to refer to directories in a package, such as `data = [""//data/regression:testdata/."]"` or `data = ["testdata/."]"` (relative label). **However, this should NOT be used** (it's possible for exceptional cases, but we'll just skip them altogether). When making rules depend on directories in such a way, the Bazel build server could not detect the edits of the individual files in this directory (it would only detect addition/deletion of files to/from the directory), which will lead to incorrect behaviour.

To provide a _"set of files"_ as an input to a rule, we should use the `glob(..)` function, such as `(data = glob(["testdata/**"]))`, which _"enumerate the set of files"_ contained in the directory. _"Functions"_, such as `glob(..)`, are further explained below.

The lexical specification of labels are also documented on Bazel's [concept and terminology docs](https://docs.bazel.build/versions/master/build-ref.html#lexi).

#### Example

In the [`console/BUILD`](https://github.com/graknlabs/grakn/blob/master/console/BUILD) file in [`graknlabs/grakn`](https://github.com/graknlabs/grakn), we can see examples of labels being used:
```
java_library(
    name = "console",
    srcs = glob(["src/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//client-java:client-java",
        "//grakn-graql:grakn-graql",
        ...
    ],
    ...
)
```

In the above example, we can see how `console` depends on `//client-java:client-java`. It means that `console` depends on the `:client-java` target, from `//client-java` package. We also see an example of the `glob()` function used to enumerate the set of all file targets to provide as input to the `srcs` argument.


### The `BUILD` file

There are 2 things you can declare in a build file:

- **Functions** to declare static information about the package build, or to help declarations of rules.
- **Rules** to provide instruction for the build process.

#### Types of build functions

Functions are generally used to declare static information about the code, such as `package`, `package_group`, `license`, `workspace`, and so on. Functions are also used to help the declaration of _"rules"_, such as the `glob(..)` function. These functions are documented in [Bazel function docs](https://docs.bazel.build/versions/master/be/functions.html).

#### Types of build rule

The majority of build rules come in families, grouped together by language. For example, `java_binary`, `java_library` and `java_test` are the build rules for Java binaries, libraries, and tests, respectively. Other languages use the same naming scheme, with a different prefix, e.g. `py_*` for Python. These rules are all documented in the [Bazel's build encyclopedia](https://docs.bazel.build/versions/master/be/overview.html).

- `*_library` rules specify separately-compiled modules in the given programming language. Libraries can depend on other libraries.

- `*_binary` rules build and produce executables for the package, and store them in `bazel-bin/path/to/package`. Binaries can depend on other libraries.

- `*_test` rules are a specialisation of a `*_binary` rule, used for automated testing. Test can depend on binaries and libraries.



Rules are defined by various inputs as their "dependencies". Most build rules have 3 main arguments for specifying different kinds of generic input: `srcs` (source files), `deps` (dependency modules) and `data` (e.g. for tests). These attributes are documented in Bazel's [Common definitions](https://docs.bazel.build/versions/master/be/common-definitions.html) and [Build Encyclopedia](https://docs.bazel.build/versions/master/be/overview.html).

#### The dependency graph

A target `X` _"depends upon"_ a target `Y` if `Y` is needed by `X` at build or execution time. The _depends upon_ relation induces a directed acyclic graph (DAG) over targets, and we call this a _"dependency graph"_.

There are 2 types of dependencies: _"direct dependencies"_ and _"transitive dependencies"_. There are also 2 types of _dependency graphs_: a graph of _"actual dependencies"_ and _"declared dependencies"_. The graph of actual dependencies must be a subset of the graph of declared dependencies. However, redundant declared dependencies make builds slower and binaries bigger.

**Thus, every rule must explicitly declare all of its "actual" and "direct" dependencies to the build system, and no more.** I.e. Do not forget to declare dependencies that are directly used by a package, even if they are made available through transitive dependencies. And do not declare dependencies that are not used directly by a package, even if it doesn't break the build.

#### Example

In the [`console/BUILD`](https://github.com/graknlabs/grakn/blob/master/console/BUILD) file in [`graknlabs/grakn`](https://github.com/graknlabs/grakn), we can see examples of build rules for library and binary targets:
```
java_library(
    name = "console",
    srcs = glob(["src/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//client-java:client-java",
        "//grakn-graql:grakn-graql",
        ...
    ],
    ...
)
java_binary(
    name = "console-binary",
    main_class = "ai.grakn.core.console.Graql",
    runtime_deps = ["//console:console"],
    ...
)
```

In the above example, we can see how `console` is built as a java library (`.jar`), and `console-binary` takes the java library and turns it into an executable with `ai.grkan.core.console.Graql` as the main class.

In the [`client-java/BUILD`](https://github.com/graknlabs/grakn/blob/master/client-java/BUILD) file in [`graknlabs/grakn`](https://github.com/graknlabs/grakn), we can see examples of build rules for test targets:
```
java_test(
    name = "transaction-test",
    srcs = glob(["test/**/*.java"]),
    test_class = "ai.grakn.client.TransactionTest",
    deps = [
        ":client-java",
        "//grakn-graql",
        ...
    ],
)
```
In the above example, we can see how `client-java` has a test called `transaction-test` with `ai.grakn.client.TransactionTest` as the main class. The test rule depends on `:client-java` which produces the jar for `client-java`, and `grakn-graql` which contains the language API which the tests use directly.

---

## Dependency Management

Given the multi-language nature of Bazel, we are now able to manage dependencies for various languages through various types of repositories, all at once. In `graknlabs/grakn` we use 3 main software repositories:
- [Maven](https://maven.apache.org) for Java
- [PyPI](https://pypi.org) for Python
- [NPM](https://www.npmjs.com) for Node.js

We will manage dependencies from all the above repositories through Bazel.

### Managing Maven Dependencies with [`bazel-deps`](https://github.com/johnynek/bazel-deps)

Managing the delcaration of all of your direct and transitive dependency is not easy, especially if you're migrating from Maven for the first time. To solve this problem, we use [`bazel-deps`](https://github.com/johnynek/bazel-deps): an bazel extension that allows us to declare our direct dependencies, and produces bazel rules to declare the transitive dependencies. 

The setup and configurations for `bazel-deps` are documented at [github.com/johnynek/bazel-deps](https://github.com/johnynek/bazel-deps) repository, and we will just introduce the high-level concepts here and how we use them at Grakn.

> _Note: this is not a tool to get away from the responsibility of keeping your dependencies lean, but it does help you to get started with managing transitive dependencies explicitly_.

#### 1. Add `bazel-deps` to your workspace

First, we declare `bazel-deps` itself as a dependency in our workspace. At Grakn, we declare the dependency macro in [`dependencies/tools/dependencies.bzl`](https://github.com/graknlabs/grakn/blob/master/dependencies/tools/dependencies.bzl):
```
def tools_dependencies():
    native.http_file(
        name = "bazel_deps",
        executable = True,
        sha256 = "43278a0042e253384543c4700021504019c1f51f3673907a1b25bb1045461c0c",
        urls = ["https://github.com/graknlabs/bazel-deps/releases/download/v0.2/grakn-bazel-deps-v0.2.jar"],
    )
```

> _Note: [`graknlabs/bazel-deps`](https://github.com/graknlabs/bazel-deps) is a fork of [`johnynek/bazel-deps`](https://github.com/johnynek/bazel-deps) that modifies the naming style of directories, files and rules to use hyphens (instead of underscores) as the delimiter character._

We then load `tools_dependencies` in [`WORKSPACE`](https://github.com/graknlabs/grakn/blob/master/WORKSPACE):
```
load("//dependencies/tools:dependencies.bzl", "tools_dependencies")
tools_dependencies()
```

#### 2. Declare your direct dependencies

Before we run `bazel-deps`, we first need to declare our direct dependencies in a `.yaml` file. At Grakn, we declare this in [`dependencies/maven/dependencies.yaml`](https://github.com/graknlabs/grakn/blob/master/dependencies/maven/dependencies.yaml):
```
options:
  thirdPartyDirectory: dependencies/maven/artifacts
  ...

dependencies:
  org.apache.cassandra:
    cassandra-all:
      version: "3.11.3"
      lang: java
      exclude:
      - "ch.qos.logback:logback-classic"
```

Notice how there is a set `options` for you to configure your `bazel-deps`. You can learn more about all the `options` in [`bazel-deps`'s docs](https://github.com/johnynek/bazel-deps#options), but what's important for us to highlight right now is that we set [`dependencies/maven/artifacts/`](https://github.com/graknlabs/grakn/tree/master/dependencies/maven/artifacts) as the directory which will contain all the generated (transitive) dependency rules.

#### 3. Generate the (transitive) dependency rules and macro

We now run `bazel-deps` in our workspace by calling:
```
bazel run //dependencies/tools:bazel-deps -- generate -r $GRAKN_CORE_HOME -d dependencies/maven/dependencies.yaml -s dependencies/maven/dependencies.bzl
```
The 3 arguments that `bazel-deps` require are:
- `-r $GRAKN_CORE_HOME`: the full path to the workspace in which the dependencies need to be delcared
- `-d dependencies/maven/dependencies.yaml`: input file name (and path relative from workspace) that contains the list of direct dependencies
- `-s dependencies/maven/dependencies.bzl`: output file name (and path relative from workspace) for the transitive dependency list to be generated in a macro called `maven_dependencies`

In `graknlabs/grakn` we've wrapped the above command in a simple scipt, [`dependencies/maven/update.sh`](https://github.com/graknlabs/grakn/blob/master/dependencies/maven/update.sh), so that everytime you make a change to Maven's `dependencies.yaml` you can simply run:
```
./dependencies/maven/update.sh
```

Which will run `bazel-deps` and (re)generate:
- [`dependencies/maven/artifacts/...`](https://github.com/graknlabs/grakn/tree/master/dependencies/maven/artifacts), and 
- [`dependencies/maven/dependencies.bzl`](https://github.com/graknlabs/grakn/blob/master/dependencies/maven/dependencies.bzl).

We then load `maven_dependencies()` from `dependencies.bzl` in [`WORKSPACE`](https://github.com/graknlabs/grakn/blob/master/WORKSPACE):
```
load("//dependencies/maven:dependencies.bzl", "maven_dependencies")
maven_dependencies()
```

---
_At this point, you can see in [`graknlabs/grakn`](https://github.com/graknlabs/grakn/blob/master) that we manage our external dependencies in packages that look like:_
```
grakn_core/
├── WORKSPACE
├── dependencies/
│   ├── maven/
│   │   ├── artifacts/
│   │   │   └── ...           # Generated rules for each dependency
│   │   ├── BUILD
│   │   ├── dependencies.bzl  # Generated macro for (transitive) dependencies
│   │   ├── dependencies.yaml # Where we declare direct dependency list
│   │   └── update.sh         # Script to generate dependencies.bzl
│   │
│   ├── tools/
│   │   ├── BUILD
│   │   └── dependencies.bzl  # Where we declare bazel-deps dependency
│   │
│   ├── ...
│
├── ...
```
---

#### 4. Using external dependencies

Now that the external dependency rules are loaded in `dependencies/maven/artifacts` and we have declared them in `WORKSPACE` through the macro in `dependencies/maven/dependencies.bzl`, we can start using the external dependencies. Let's look at [`console/BUILD`](https://github.com/graknlabs/grakn/blob/master/console/BUILD) again:
```
java_library(
    name = "console",
    srcs = glob(["src/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//client-java:client-java",
        "//grakn-graql:grakn-graql",

        # External dependencies
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        ...
    ],
    ...
)
```

In the example above, we can see that `console` depends on rules declared in the `BUILD` file of packages contained inside [`//dependencies/maven/artifacts/...`](https://github.com/graknlabs/grakn/tree/master/dependencies/maven/artifacts):
- the `:annotations` rule from [`.../com/google/code/findbugs/BUILD`](https://github.com/graknlabs/grakn/blob/master/dependencies/maven/artifacts/com/google/code/findbugs/BUILD)
- the `:guava` rule from [`.../com/google/guava/BUILD`](https://github.com/graknlabs/grakn/blob/master/dependencies/maven/artifacts/com/google/guava/BUILD)

Both `:annotations` and `:guava` are `bazel-deps` generated rules, that exports the jars of the dependency artifacts.

#### 5. Updating external dependencies

1. Update the dependency list in [`dependencies/maven/dependencies.yaml`](https://github.com/graknlabs/grakn/blob/master/dependencies/maven/dependencies.yaml) described [step #2](#2-Declare-your-direct-dependencies). You can add/update/delete a dependency in this list.
2. Run `./dependencies/maven/update.sh` as shown in [step #3](#3-Generate-the-transitive-dependency-rules-and-macro), which will regenerate the dependency rules and macro.
3. If you added a new external dependency, you can now use that dependency rule in your `BUILD` file as shown in [step #4](#4-Using-external-dependencies)

> _Note: you DO NOT need to update the `WORKSPACE` file. Once the `maven_dependencies()` macro has been declared (in [step #1](#1-Add-bazel-deps-to-your-workspace)), it will always be loaded during build time whenever Bazel sees a change in the dependency._

### Managing PyPI Dependency

>> TODO

### Managing NPM Dependency

>> TODO

---

## Build tools

>> TODO

- fix_deps, buildozer and buildifer
- https://github.com/google/startup-os/blob/master/tools/BUILD

---

## Special Compilers (source code generators)

>> TODO

### ANTLR4

>> TODO
- importing repository
- build rules
- https://github.com/marcohu/rules_antlr

### Protobuf and GRPC

>> TODO
- how to write proto_library rules: 
- https://github.com/pubref/rules_proto
- https://blog.bazel.build/2017/02/27/protocol-buffers.html