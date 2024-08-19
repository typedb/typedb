# TypeQL 3.0 Spec (Math BDD)

## Notation and terminology

* **TT** — transaction time
  * _Interpretation_: any time within transaction
* **CT** — commit time
  * _Interpretation_: time of committing a transaction to DB
* **tvar** — type variable
* **dvar** - data variables
  * _Note_: **tvar**s and **dvar**s are uniquely distinguish everywhere in TypeQL
* **Typing**. $`a : A`$ — $a$ is of type $A$ (types always capitalized). 
  * _Example_: $p$ is of type $\mathsf{Person}$
   _Special case_: $a :! A$ ("user-defined type" a.k.a. direct type).
* **Dependent typing**. $`a(x, y, ...) : A(I,J,...)`$ — $a$ is an element in type "$`A`$ of $`x`$ (as $`I`$), and $`y`$ (as $`J`$), and ..." (i.e. $a$ depends on $x$ through interface $I$).
  * _Alternative notation_: $`a : A(x\to I,y\to J,...)`$. 
  * _Example_: $m$ is of type $\mathsf{Marriage}$ of $p$ (as $\mathsf{Spouse}$) and $q$ (as $\mathsf{Spouse}$).
  * _Unfilled role slots_: $`a(\emptyset,z,...) : A(I,J)`$ (or $`m : A(\emptyset \to I, z \to J, ...)`$ using alt. notn.)
  * _Role cardinality_: $|a|_I$ counts $a \to I$ with $a \neq \emptyset$
* **Dependent types**. $`A(I, J, ...) : \mathbf{Type}`$ —  $A$ is a type with associated (interface) types $`I, J, ...`$. 
  * _Alternative notation_: $`A : \mathbf{Type}(I,J,...)`$. 
  * _Example_: $`\mathsf{Person}`$ is a type.
  * _Example_: $`\mathsf{Marriage(Spouse)}`$ is a type with associated type $`\mathsf{Spouse}`$.
  * _Variations_: may replace $\textbf{Type}$ by $\mathbf{Ent}, \mathbf{Rel}, \mathbf{Att}$, or $\mathbf{Obj} = \mathbf{Ent} + \mathbf{Att}$.
  * _Key properties_
    * _Compositionality_: $A(I) : \mathbf{Type}$ and $A(J) : \mathbf{Type}$ imply $A(I,J) : \mathbf{Type}$
    * _Forgetting dependencies_: $A(\vec I,J) <_\bot A(\vec I)$
* **Casting**. $`A < B`$ — $A$ casts into $B$ (this is transitive). Variations (both imply $`<`$):
  * _Direct_: $`<_!`$ ("user-defined cast" a.k.a. direct cast).
    *  _Example_: $`\mathsf{Child} <_! \mathsf{Person}`$
    *  _Example_: $`\mathsf{Child} <_! \mathsf{Nameowner}`$
    *  _Example_: $`\mathsf{Person} <_! \mathsf{Spouse}`$
  * _Dependency_: $`A(I_1, ..., I_k,J) <_\bot A(I_1, ..., I_k)`$
* **Type cardinality**.$`|A|`$ — Cardinality of $A$
* **List types**. $`[A]`$ — List type of $A$ (contains lists $`[a_0, a_1, ...]`$ of $`a_i : A`$)
* **Sum types**. $`A + B`$ — Sum type
* **Product types**. $`A \times B`$ — Product type

(Complete aside for the nerds: list types are neither sums, nor products, nor polynomials ... they are so-called _inductive_ types!)


## Schema

_Pertaining to defining and manipulating TypeDB schemas_

### Define

* `define` statement fall into four categories:
  * schema type defs
  * constraints
  * value type defs
  * function defs
* `define` statements can be chained
* **TBD**: preceding `define` with `match`, and using results for **tvar**s 

#### Type defs

* $`[\![\texttt{entity A}]\!] = A : \mathbf{Ent}`$
* $`[\![\texttt{entity A sub B}]\!] = A : \mathbf{Ent}, A < B`$

#### Constraints

#### Struct defs

#### Functions defs

kinds of functions, function dependency

### Undefine

#### Type defs

#### Constraints

#### Struct defs

#### Functions defs

### Redefine

#### Type defs

#### Constraints

#### Struct defs

#### Functions defs

## Data languages

_Pertaining to reading and writing data_

### Match

Topics: match semantics

### Insert

### Delete

### Update

### Put

### Function semantics

## Pipelines

### Clauses vs Operators

Topics: Match, Manipulation, Fetch, ...

## Concurrency

## Sharding

(TBD)
