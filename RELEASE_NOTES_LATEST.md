### Install

**Download from TypeDB Package Repository:**

[Distributions for 3.0.0-alpha-5](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.0.0-alpha-5)

**Pull the Docker image:**

```docker pull vaticle/typedb:3.0.0-alpha-5```

## New Features

- **Negation**

  We add support for negations in match queries:
  ```php
  match $x isa person, has name $a; not { $a == "Jeff"; };
  ```

  Note: unlike in 2.x, TypeDB 3.x supports nesting negations, which leads to a form of "for all" operator:
  ```php
  match
      $super isa set;
      $sub isa set;
      not { # all elements of $sub are also elements of $super:
          (item: $element, set: $sub) isa set-membership;  # there is no element in $sub...
          not { (item: $element, set: $super) isa set-membership; }; # ... that isn't also an element of $super
      };
  ```

- **Require implementation**

  Implement the 'require' clause:
  ```
  match
  ...
  require $x, $y, $z;
  ```
  Will filter the match output stream to ensure that the variable `$x, $y,
  $z` are all non-empty variables (if they are optional).

- **Let match stages accept inputs**

  We now support `match` stages in the middle of a pipeline, which enables queries like this:
  ```
  insert
      $org isa organisation;
  match
      $p isa person, has age 10;
  insert
      (group: $org, member: $p) isa membership;
  ```


## Other Improvements
- **Add console script as assembly test**

- **Add BDD test for pipeline modifiers**\
  Add BDD test for pipeline modifiers; Fixes some bugs uncovered in the process.

- **Fix failing type-inference tests**

- **Fetch part I**
  
  We implement the initial architecture and refactoring required for the Fetch implementation, including new IR data structures and translation steps.
  
- **TypeDB 3 - Specification**
  
  Added specification of the TypeDB 3.0 database system behaviour.

