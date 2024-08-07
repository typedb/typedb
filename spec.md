# TypeQL 3.0 Spec (Math BDD)

## Notation

* $`a : A`$ — $a$ is an instance of $A$ (note capitalization)
* $`a(x) : A`$ — dependent term, read "$a$ of $x$" (i.e. $a$ depends on $x$)
* $`A < B`$ — $A$ subtypes $B$. Transitive relations. Variations (both imply $`<`$):
  * $`<_!`$ ("direct subtype"), 
  * $`<_+`$ ("impl").
* $`|A|`$ — Cardinality of $A$
* $`[A]`$ — Listtype of $A$ (contains lists of $a$ for $a : A$)
* $`A : \mathbf{Type}`$ — $A$ is a type. Similarly for $\mathbf{Ent}, \mathbf{Rel}, \mathbf{Att}$. Let $\mathbf{Obj} = \mathbf{Ent} + \mathbf{Att}$
* $`[\![\texttt{code}]\!] = \alpha`$ — mathematical meaning of "code" is "$`\alpha`$"

## Schema

### Types

* $`[\![\texttt{entity A}]\!] = A : \mathbf{Ent}`$
* $`[\![\texttt{entity A sub B}]\!] = A : \mathbf{Ent}, A < B`$
* $`[\![\texttt{A sub B}]\!] = A : \mathbf{Ent}`$

### Values

### Constraints

## Data

### Matching

### Manipulating

### Functions

## Pipelines

### Clauses

### Operators
