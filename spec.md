# TypeQL 3.0 Spec (Math BDD)

## Notation and terminology

* **TT** — transaction time
* **CT** — commit time
* $`a : A`$ — $a$ is of type $A$ (note capitalization of types). 
  * _Example_: $p$ is of type $\mathsf{Person}$
* $`a(x, y, ...) : A(I,J,...)`$ — $a$ is an element in type "$`A`$ of $`x`$ (as $`I`$), and $`y`$ (as $`J`$), and ..." (i.e. $a$ depends on $x$ through interface $I$). Alternative notation: $`a : A(x\to I,y\to J,...)`$. 
  * _Example_: $m$ is of type $\mathsf{Marriage}$ of $p$ (as $\mathsf{Spouse}$) and $q$ (as $\mathsf{Spouse}$).
* $`A(I, J, ...) : \mathbf{Type}`$ —  $A$ is a type with associated (interface) types $`I, J, ...`$. More specifically, may replace $\mathbf{Type}$ by $\mathbf{Ent}, \mathbf{Rel}, \mathbf{Att}$. Set $\mathbf{Obj} = \mathbf{Ent} + \mathbf{Att}$. 
  * _Example_: $`\mathsf{Person}`$ is a type.
  * _Example_: $`\mathsf{Marriage(Spouse)}`$ is a type with associated type $`\mathsf{Spouse}`$.
* $`A < B`$ — $A$ subtypes $B$ (this is transitive). Variations (both imply $`<`$):
  * $`<_!`$ ("direct subtype"). _Example_: $`\mathsf{Child} <_! \mathsf{Person}`$
  * $`<_I`$ ("interface implementation"). _Example_: $`\mathsf{Person} <_I \mathsf{Spouse}`$
* $`|A|`$ — Cardinality of $A$
* $`[A]`$ — Listtype of $A$ (contains lists $`[a_0, a_1, ...]`$ of $`a_i : A`$)
* $`[\![\texttt{code}]\!] = \alpha`$ — mathematical meaning of "code" is "$`\alpha`$"


## Schema

### Types

Topics: kinds, sub, dependencies, as, lists, 

* $`[\![\texttt{entity A}]\!] = A : \mathbf{Ent}`$
* $`[\![\texttt{entity A sub B}]\!] = A : \mathbf{Ent}, A < B`$
* $`[\![\texttt{A sub B}]\!] = A : \mathbf{Ent}`$

### Values

Topics: primitives, structs, ...

### Constraints

Topics: cardinality, functionality (key, unique), abstractness, ...

## Data

### Matching

Topics: match semantics

### Manipulating

Topics: insert, delete, update, put

### Functions

Topics: definition, calling, recursion semantics

## Pipelines

### Clauses

Topics: Match, Manipulation, Fetch, ...

### Operators

Topics: Select, Limit, Offset, ...

## Concurrency


