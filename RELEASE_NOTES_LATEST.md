**Download from TypeDB Package Repository:**

[Distributions for 3.5.0](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.5.0)

**Pull the Docker image:**

```docker pull typedb/typedb:3.5.0```


## New Features
- **Analyze endpoint**
  Introduce the analyze endpoint for the HTTP API, meant for compiling a query (without running it) and inspecting it - Currently, the structure & type-annotations are returned.
  The endpoint is at: `/<version>/transactions/<transaction-id>/analyze`
  
  Sample request:
  ```
  { "query": " 
    with fun title($book: book) -> string: 
    match $
        book has title $title;
        let $as_string = $title;
    return first $as_string;
  
    match
        $book isa book;
    fetch { 
        \"title\": title($book) 
    };"
  }
  ```
  Sample response: (Or see the end of [#7537](https://github.com/typedb/typedb/pull/7537) for a full example)
  ```
  {
      "structure":  {
          "query": PipelineStructure, // As returned in any query
          "preamble": [ FunctionStructure, ...],
       },
       annotations: {
            "query":  PipelineAnnotations,
            "preamble": [ FunctionAnnotations, ... ],
            "fetch": FetchAnnotations,
          }
       }
  }
  ```
  where,
  ```
  PipelineStructure = { blocks: [...],  "pipeline": [...], "variables":{ ... }, "outputs": [...] }
  PipelineAnnotations = {
    "annotationsByBlock": [
        {
            "variableAnnotations": { "0": VariableAnnotations, ... }
        }
  }
  
  FunctionStructure = { 
    "body": PipelineStructure 
    "arguments":  [...],
    "return": FunctionReturnStructure,
  }
  
  FunctionReturnStructure =   { "tag": "check"}  | 
    { "tag": "stream" | "single",    "variables": [...] }  |
    { "tag": "reduce",  "reducers": [ { "reducer": "count"|"sum"|..., [variable: ]  ] }  |
  
  
  FunctionAnnotations = { 
    "arguments": [VariableAnnotations, ... ], 
    "returned": [VariableAnnotations, ...],
    "body": PipelineAnnotations
  }
  
  FetchAnnotations = {
     "<key>": ValueTypeAnnotations | FetchAnnotations
  }
  
  ValueTypeAnnotations = ["string", ...] // etc
  
  VariableAnnotations = EITHER
    {
        "tag": "concept",
        "annotations": [
            {
                "kind": "attributeType",
                "label": "title",
                "valueType": "string"
            }, 
            ...
        ]
    }
  OR 
    {
      "tag": "value",
      "value_types": [
          "string", "long", ....
      ]
  }
  ```
  
  
  

## Bugs Fixed
- **Fix crash when functions sorts empty stream**
  Fixes a crash when a functions tried to sort an empty stream.
  
  
- **Fix development mode configuration in config file**
  Fixes development mode configuration in config file
  
  
- **Support inserting attributes from values of other attributes**
  Support inserting attributes from values of other attributes 
  
  ```typeql
  match $x isa src-attr;
  insert $y isa dst-attr == $x;
  ```
  
  fixes #7551 
  
- **Enforce stratification through single return functions**
  Single return functions may not call themselves recursively.  Addresses #7550 
  
  This will now fail:
  ```typeql
    define
      fun last_number() -> integer:
      match
        let $number = last_number() + 1;
      return last $number;
  ```
  
  
- **Store answers in a hash table as well to avoid slow inserts in large table**
  Answer tables additionally store answers in a HashSet for constant time lookup, making constructing large tables linear instead of quadratic.
  
  
- **Variables referenced in only some branches of a disjunction are required inputs**
  Updates the planner to treat variables which are referenced in "some but not all" branches of a disjunction as "required inputs". This means a pattern binding the variable must be bound before the disjunction can be scheduled.

  
- **Lock attributes on updating connections**

  We fix an Isolation bug that is exposed under concurrent update (adding an ownership) + concurrent delete transactions. This is a relatively uncommon conflict, and is fixed  by locking attributes that are edited in order to conflict with concurrent deletes of the same attribute. Under highly concurrent operations that include `delete`s, this change might manifest itself as more transaction conflicts `STC2` (Storage Commit 2) errors, which can be resolved with a retry.


## Code Refactors
- **Add cases for reduce enum variants**
  Adds the remaining ReduceInstruction cases to a `match` introduced in the previous commit.
  
  
- **Rename the command-line argument for authentication token expiration**
  Rename the command-line argument `server.authentication.token_ttl_seconds` to `token-expiration-seconds` to make it consistent with the config field
  
  

## Other Improvements
- **Fix missing word in isolation error message**
  
  The isolation conflict message when deleting a required key reads "Transaction data a concurrent commit requires."
  
  
- **Refactor bazel build dependencies**
  
  We've identified some redundant dependencies in bazel build scripts, which are harmful to incremental/parallel build. 
  We refactored them as part of a research project on dependency reduction.


- **Revise response format of the analyze endpoint**
  We revise the response format for the `analyze` endpoint of the HTTP API, to align it better with TypeDB's representation, and simplify parsing & reconstructing the structure.
  
  Major changes include:
  * `Or`, `Not` and `Try` blocks are now included in the block constraints. Hence the conjunction tree structure is represented within the blocks. This avoids a relatively convoluted interplay between `pipeline.structure.conjunctions` and `pipeline.structure.blocks` in reconstructing the conjunction tree.
  * A `match` stage under a `pipeline` structure now only needs to hold a single `blockId` instead of a tree representing the conjunction.
  * Fetch annotations are more structured, for easier parsing in all languages. Previously, the dynamic keys required a map with a union of values to parse properly. Now it's simply a union of static types, and provides the user a schema for the returned JSON object.
  ```
  export type FetchAnnotations =
      { tag:  "list", elements: FetchAnnotations } |
      { tag : "object", possibleFields: FetchAnnotationFieldEntry[] } |
      { tag : "value", valueTypes: ValueType[] };
  
  export type FetchAnnotationFieldEntry = FetchAnnotations & { key: string };
  
  ```
  For a more complete spec, see https://github.com/typedb/typedb-driver/pull/783
  
  
- **Allow empty define**
  
  Since https://github.com/typedb/typeql/pull/412 is implemented, empty define queries are allowed. This means that schema export that used to return an empty string for empty schemas can now return an empty `define` query, which is much easier to work with on the client side.
  
  This addresses https://github.com/typedb/typedb/issues/7531
  
  
- **Work around brew install python@3.9 breaking our CI on mac**
  Works around `brew install python@3.9` breaking CircleCI on mac.
  
  
- **Re-enable @typeql release dependency validation**

- **Update README links**

- **Skip decompressing WAL records unnecessarily**
  
    
