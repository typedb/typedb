# GRAKN.AI Documentation

## TL;DR

**How to update and build the docs**

* Install `bundler` and `rake` (one time only task)
* Pull down documentation repo
* Make changes to markdown
* `rake serve` in terminal and browse to [http://127.0.0.1:4005](http://127.0.0.1:4005)
* If you need to make some extra changes to the markdown after you have the server running, just change the files and save them. `rake` will pick up the changes and rebuild the docs so you don't need to do anything.
* When you're happy, push to the docs repo
* Go to the graknlabs website repo when you're ready to deploy...

## Dependencies

You need to install the following dependencies to be able to build HTML pages in the documentation repository. **NOTE:** this is *not* necessary for updating the documentation itself.

1. Bundle; you will need to install `bundler` through your package manager of choice.

    **Arch Linux**
    ```
    $ yaourt -S ruby-bundler
    ```

    **OSX**
    ```
    $ brew install bundler
    ```

    **Ruby Gems (generic)**
    ```
    $ gem install bundler
    ```

2. Rake; this is used to automate the rest of the site building process.
    ```
    $ gem install rake
    ```

With `rake` installed you can now install all other dependencies:
```
$ rake dependencies
```

## Environment variables

There is an environment variable that needs to be set depending on the build you wish to commence: `urlprefix`. If you only wish to preview the generated documentation itself; as server from this repository rather than integrated into the GRAKN.AI website you do not need to do anything.

##### `urlprefix`

For local builds (local web server with documentation only), do not set this variable, or make sure it has been cleared with:
```
$ export urlprefix=""
```

For integrating to GRAKN.AI website, or elsewhere set as needed. For example; `www.grakn.ai/docs/`
```
$ export urlprefix=/docs
```

## Building

You can generate the documentation HTML by running the following in the repository top level.
```
$ rake build
      Generating...
                    done in 1.503 seconds.
 Auto-regeneration: disabled. Use --watch to enable.
$
```

This will build the documentation site in `_jekyll` and create a symlink `_site` in the repository top level directory which will contain all the generated content.

## Cleaning

Clean by running the following command in the repository top level:
```
$ rake clean
```

This will remove all generated files.

## Serving

You can also build and server the generated HTML files in one command. A web
server will be started listening on `localhost` (127.0.0.1) on port 4005

```
$ rake serve
    Server address: http://127.0.0.1:4005/
  Server running... press ctrl-c to stop.
$
```

You can now view the documentation by navigating your web browser to `http://127.0.0.1:4005/`

## Tests

There are a few tests we run against docs:

- `html-proofer`
- `GraqlDocsTest`
- `JavaDocsTest`

`html-proofer` can be executed with `rake test`. It will check all the links in the docs to make sure they actually go
somewhere.

`GraqlDocsTest` and `JavaDocsTest` will test the Graql and Java code blocks respectively. Blocks are identified by
whether they begin with `graql` or `java`. Each page is tested on its own by executing the code blocks sequentially.

By default, the code blocks are executed against the genealogy knowledge base. If you want to use a different knowledge
base, then add e.g.
```
KB: pokemon
```
to the header of the markdown file. The valid knowledge bases can be found in `DocTestUtils`.

Java code blocks are actually tested with Groovy (because it is an interpreted language). There are some differences
between Java and Groovy syntax, so we recommend writing code that is valid in both languages.

If a code block should not be executed (e.g. because it is deliberately invalid or does something dangerous), then mark
it `graq-test-ignore` or `java-test-ignore` instead of `graql` or `java`.
