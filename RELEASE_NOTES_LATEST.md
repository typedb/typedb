Install & Run: http://docs.vaticle.com/docs/running-typedb/install-and-run


## New Features
- **Relax rule conclusion validation to facilitate rules over abstract types**
  
  A follow on from #6801 , we finish the rule validation relaxation implemented there. We further relax the 'then' of the rule to operate over concrete  variables, as long as all the type variables that define _which_ instances to create during materialisation are concrete.
  
  For example:
  ```
  define
  abstract-person sub entity, abstract, plays friendship:friend; #abstract
  friendship sub relation, relates friend;  #non-abstract
  
  rule concrete-relation-over-abstract-players: 
  when { 
     $x isa abstract-person;
  } then {
     (friend: $x) isa friendship;
  };
  ```
  
  Is now a legal rule to write since the 'friendship' that would be created is an instance of a concrete type.
  
  
  
- **Introduce protocol versioning**
  
  Introduce a new top-level RPC to open a connection, which validates the protocol versioning is compatible and throws an exception to the clients if it is incompatible.
  
  
- **Introduce expressions and computed value variables**
  
  Implement support for expressions and value variables. The user-facing changes that drive this change are outlined in https://github.com/vaticle/typeql/pull/260.
  
  In particular TypeDB now supports the evaluation and return of Values, which are distinct from the previously existing data instances Entity/Relation/Attribute. Values are never materialised permanently, and only used within the scope of a particular query or rule. They are defined as an actual inner value, plus one of the defined TypeDB value types: `long`, `double`, `boolean`, `string` or `datetime`.
  
  Rules also now allow using of value variables to infer new attribute values:
  ```
  define
  rule computation-cost-dollars-to-pounds: when {
    $c isa computation, has cost-dollars $c;
    $_ isa exchange-rate, has name "dollar-pound", has rate $rate;
    ?pounds = $rate * $c;
  } then {
    $c has cost-pounds ?pounds;
  };
  ```
  
  Querying for an attribute `match $x isa thing;` will then also trigger this inference. Note that accidentally creating infinite reasoning is much easier now: if implement `n = n + 1` in a rule, **it will run as long as the transaction is alive** - and likely cause an OOM. Future work will offer more advanced validation to protect against these cases.
  
  
  Closes #6654.
  
  
- **Check BDD value equality using native types**
  
  In BDD steps implementation we checked the equality of all values through the conversion to Strings. It might be incorrect when comparing Doubles especially if one of these values is a result of arithmetics.
  Now we compare Doubles by their absolute error.
  For consistency we compare all values using their native types equality.
  
  
- **Introduce unique attribute ownership**
  
  We introduce the `@unique` annotation for attribute ownerships. The `@unique` annotation allows users to declare that an attribute instance may not be owned more than once by the owner type.
  
  For example:
  ```
  define
  person owns email @unique;
  ```
  
  Will require that any email owned by a person is unique - multiple people owning the same email will generate an exception conforming to the standard Snapshot Isolation requirements.
  
  Note that this allowed a person to own any number of emails - no cardinality restriction is generated from the `@unique` annotation.
  
  Since this functionality overlaps with the `@key` annotation for attribute ownerships, some behaviours are enforced:
  1. An ownership can't have both `@unique` and `@key` at the same time.
  2. A child type can specialised a `@unique` attribute ownership that is inherited by redeclaring the ownership with `@key` - however the reverse is a loosening and will throw a useful exception
  3. Annotations are inherited and cannot be simply redeclared - they must be specialised or left out. Note that this applies even when using the override mechanism to specialise an attribute ownership:
  
  ```
  define
  organisation sub entity, abstract, owns organisation-id @unique;
  organisation-id sub attribute, abstract, value string;
  non-profit sub organisation, owns nonprofit-id as organisation-id;
  nonprofit-id sub organisation-id, value string;
  ```
  Here, ownership by `nonprofit` of `non-profit-id` will inherit the `@unique` annotation from the parent ownership.
  
  This feature can be used to implement a sound client-side unique key scheme (such as auto-increment/UUID) that may not be wanted as `@key`s. This is possible since concurrent creation of unique attributes will throw isolation violations exceptions according to TypeDB's isolation guarantees.
  
  

## Bugs Fixed
- **Enable reasoner perf counters for reasoner benchmark**
  Enable reasoner perf counters in reasoner benchmark, fixing the failing tests.
  
  
- **Fix concurrency bug with ByteArray hashCode**
  Fix concurrency bug with the lazily evaluated hashCode of the ByteArray class
  
  
- **Fix assertions in Data Importer**
  Remove one overly-restrictive assert and fix a bug which was triggering another assert.
  This bug would not have affected correctness or completeness of the import.
  
  
- **Reasoner planner model bypassed reasoning**
  Model the fact that the reasoner will bypass reasoning when it is safe to do so.
  
  
- **Fix condition for cyclic-scaling factor**
  Fix a condition which determined the value for the cyclic scaling factor. This was leading to NaNs in the computation cost when the answer estimate was 0.


- **Relax rule validation to allow unanswerable but coherent rules**

  We allow rules to be written in a way that is semantically sensible in the schema, but may not produce any answers (specifically, if `when` types can't have any instances because they are abstract).

  We therefore match the current behaviour of `match` queries: `match $r isa relation;` does not throw any exceptions even if there are no concrete subtypes of `relation`. As a result, after this change, it will be possible to write rules that will never trigger any inferences, since never match any concrete types.

  This change allows writing and sharing 'general' rules that work over an abstract schema, only to be specialised at a later part of the development cycle.

  Note that a rule `then` must still be written using concrete, insertable types.

## Code Refactors
- **Update release notes workflow**
  
  We integrate the new release notes tooling. The release notes are now to be written by a person and committed to the repo.
  
  
- **Optimise queries by introducing concept-level schema cache**
  
  Many data operations rely on the schema easily and performantly available. However, we only cached the schema at the lower "graph" layer - which has no knowledge of semantic constructs like ownerships. 
  
  As of #6775, caching at the Concept layer becomes particularly useful since it introduces constructs to represent ownerships. To avoid re-computing type ownerships, we had already introduced caching in the owner Type. However, the Type itself was repeatedly constructed, negating the usage of this cache. 
  
  This change introducing a schema cache in the Concept Manager, and redirects all constructions of Type concepts via the Concept Manager to leverage the cache. This cache is only active when the schema is read-only.
  
  With this change, we can see heavy write performance of improve by up to 35%, in particular when writing attribute ownerships.
  
  

## Other Improvements
- **Fix rule unindexing on rule undefine**
  
  
- **Fix release pipeline in automation yaml**
  
  
- **Update error messages during uniqueness violations**
  
- **Reasoner benchmarking based on the demo IAM schema**
  
  Introduce tests based on queries from the TypeDB IAM example to measure reasoner performance and flag regressions. We include two types of tests:
  
  1. basic tests: these are simple baselines for the reasoner such as inferring relations, attribute ownerships, etc.
  2. complex tests: these are designed to target particularly difficult spots of the reasoning engine, such as query planning, large state spaces, complex rule graphs, and more.
  
  The 'test' portion of these benchmarks are designed to pass/fail based on the amount of state & work created in the execution of the benchmark, rather than time, as these should be stable across machine types.
  
  
- **Set up bazel remote cache**
  
  
- **Prune inferred types of variables in then having variabilised 'isa' constraints**
  Improve type-inference for in rules by pruning inferred-types of variables with variabilised `isa` constraints. 
  
  