# Data definition language

Modifies types and constraints what terms can be in types.


## Invariants

What holds true in any valid database (schema) state. See [READ](./read.md) for semantics of relevant patterns.

1. `A sub! $B` for at most one `$B`
1. When `A relates I` then `I sub! $J` for at most one `$J`
1. `$A relates! I` for at most one `$A`
1. `A relates! I` and `A relates! I[]` not at the same time.

## Define

### Overview

* Takes *multiple* statements
* Adds [axioms](./type_system.md) to the type system

### Type axioms

#### **Case ENT_DEF**
* ➖ `entity A` adds $`A : \mathbf{Ent}`$
* ➖ `(entity) A sub B` adds $`A : \mathbf{Ent}, A <_! B`$

#### **Case REL_DEF**
* ➖ `relation A` adds $`A : \mathbf{Rel}`$
* ➖ `(relation) A sub B` adds $`A : \mathbf{Rel}, A <_! B`$ where $`B : \mathbf{Rel}`$ 
* ➖ `(relation) A relates I` adds $`A : \mathbf{Rel}(I)`$ and $`I : \mathbf{Trait}`$.
* ➖ `(relation) A relates I as J` adds $`A : \mathbf{Rel}(I)`$, $`I <_! J`$ where $`B : \mathbf{Rel}(J)`$ and $`A <_! B`$
* 🔶 `(relation) A relates I[]` adds $`A : \mathbf{Rel}([I])`$
* 🔶 `(relation) A relates I[] as J[]` adds $`A : \mathbf{Rel}([I])`$, $`I <_! J`$ where $`B : \mathbf{Rel}([J])`$ and $`A <_! B`$

#### **Case ATT_DEF**
* ➖ `attribute A` adds 
    * $`A : \mathbf{Att}(O_A)`$, $`O_A : \mathbf{Trait}`$
    * $`[A] : \mathbf{List}(O_{A[]})`$, $`O_{A[]} : \mathbf{Trait}`$
* ➖ `(attribute) A value V` adds $`\mathsf{val} : A \to V`$, where $`V`$ is a primitive or a user-defined struct value type
* ➖ `(attribute) A sub B` adds
    * $`A : \mathbf{Att}(O_A)`$, $`A <_! B`$ and $`O_A <_! O_B`$ where $`B : \mathbf{Att}(O_B)`$
    * $`[A] : \mathbf{List}(O_{A[]})`$, $`[A] <_! [B]`$ and $`O_{A[]} <_! O_{B[]}`$ where $`B : \mathbf{Att}(O_B)`$

_System property_: 

1. ➖ _Single inheritance_: Cannot have $`A <_! B`$ and $`A <_! C \neq B`$ for $`A, B, C : \mathbf{Att}`$.
1. 🔶 _Downward consistent value types_: When $`A <_! B`$, $`B <_! W`$ then must have $`\mathsf{val} : A \to V = W`$ for value type $`V`$. (**Note**: could in theory weaken this to  $`\mathsf{val} : A \to V \leq W`$)


#### **Case PLAYS_DEF**

* ➖ `A plays B:I` adds $`A <_! I`$ where $`B: \mathbf{Rel}(I)`$, $`A :\mathbf{Obj}`$.

_System property_: 

1. ➖ _Dissallow inherited roles_: Cannot have that $B \lneq B'$ with $`B': \mathbf{Rel}(I)`$ (otherwise fail).

_Remark_. The property ensures that we can only declare `A plays B:I` if `I` is a role directly declared for `B`, and not an inherited role.

#### **Case OWNS_DEF**
* ➖ `A owns B` adds $`A <_! O_B`$ where $`B: \mathbf{Att}(O_B)`$, $`A :\mathbf{Obj}`$
* 🔶 `A owns B[]` adds $`A <_! O_{B[]}`$ where $`B: \mathbf{Att}(O_B)`$, $`A :\mathbf{Obj}`$

_System property_: 

1. 🔶 _Automatic abstractions_: 
    * _Un-ordering_: when `A owns B[]` then automatically `A owns B @abstract` (see **OWNS_ABSTRACT_DEF** for mathematical meaning of the latter)
1. 🔶 _Exclusive attribute modes_: Dissalow `$A owns $B` and `$A owns $B[]` **non-abstractly** at the same time (we use variable to include not just direct declaration but also inferred validity, see "Pattern semantics").
1. 🔶 _Attribute mode exclusivity in hierarchies_: When $`A' \leq A`$, $`B' \leq B`$ then
    * Dissallow `A owns B` and `A' owns B'[]` at the same time
    * Dissallow `A owns B[]` and `A' owns B'` at the same time

### Constraints

#### Cardinality

##### **Case CARD_DEF**
* ➖ `A relates I @card(n..m)` postulates $n \leq k \leq m$ whenever $`a :_! A'(\{x_1, ..., x_k\} : I)`$, $`A' \leq A`$, $`A' : \mathbf{Rel}(I)`$.
  * **defaults** to `@card(1..1)` if omitted ("one")
* 🔶 `A plays B:I @card(n..m)` postulates $n \leq |B(a:I)| \leq m$ for all $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")
* ➖ `A owns B @card(n...m)` postulates $n \leq |B(a:I)| \leq m$ for all $`a : A`$
  * **defaults** to `@card(0..1)` if omitted ("one or null")

_System property_:

1. ➖ For inherited traits, we cannot redeclare cardinality (this is actually a consequence of "Implicit inheritance" above). 
2. ➖ When we have direct subtraits $`I_i <_! J`$, for $`i = 1,...,n`$, and each $`I_i`$ has `card(`$`n_i`$`..`$`m_i`$`)` while J has `card(`$`n`$`..`$`m`$`)` then we must have $`\sum_i n_i \leq m`$ and $`n \leq \sum_i m_i`$.
  
_Remark 1: Upper bounds can be omitted, writing `@card(2..)`, to allow for arbitrary large cardinalities_

_Remark 2: For cardinality, and for most other constraints, we should reject redundant conditions. Example: when `A sub A'` and `A' owns B card(1..2);` then `A owns B card(0..3);` is redundant_

##### **Case CARD_LIST_DEF**
* 🔶 `A relates I[] @card(n..m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $`a : A'(l : [I])`$, $A' \leq A$, $`A' : \mathbf{Rel}([I])`$, and $`k`$ is _maximal_ (for fixed $a : A$).
  * **defaults** to `@card(0..)` if omitted ("many")
* 🔶 `A owns B[] @card(n...m)` postulates $n \leq \mathrm{len}(l) \leq m$ whenever $`l : [B](a:O_B)`$ for $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")

#### Modalities

##### **Case UNIQUE_DEF**
* 🔷 `A owns B @unique` postulates that if $`b : B(a:O_B)`$ for some $`a : A`$ then this $`a`$ is unique (for fixed $`b`$).

_Note_. This is "uniqueness by value" (not uniqueness by direct-typed attribute).

##### **Case KEY_DEF**
* 🔷 `A owns B @key` postulates that if $`b : B(a:O_B)`$ for some $`a : A`$ then this $`a`$ is unique, and also $`|B(a:O_B)| = 1`$.

_Note_. This is "keyness by value" (not keyqueness by direct-typed attribute).

##### **Case SUBKEY_DEF**
* 🔶 `A owns B1 @subkey(<LABEL>); A owns B2 @subkey(<LABEL>)` postulates that if $`b : B_1(a:O_{B_1}) \times B_2(a:O_{B_2})`$ for some $`a : A`$ then this $`a`$ is unique, and also $`|B_1(a:O_{B_1}) \times B_2(a:O_{B_2})| = 1`$. **Generalizes** to $`n`$ subkeys.


##### **General case: ABSTRACT_DEF**

_Key principle_: If $`\diamond(A : K)`$ can be inferred in the type system, then we say "$`A : K`$ holds abstractly". 

* In all cases, the purposse of abstractness is to ***constrain `insert` behavior.***
* In a commited schema, it is never possible that both $`\diamond(A : K)`$ and $`A : K`$ are both true the same time (and _neither implies the other_).

_System property_

* 🔶 _Abstractness prevails_. When both $`\diamond(A : K)`$ and $`A : K`$ are present after a define stage is completed, then we remove the latter statements from the type system. (_Note_. Abstractness can only be removed through ***undefining it***, not by "overwriting" it in another `define` statement ... define is always additive!).

##### **Case TYP_ABSTRACT_DEF**
* 🔷 `(kind) A @abstract` adds $`\diamond(A : \mathbf{Kind})`$ which affects `insert` behavior.

_System property_

1. 🔷 _Upwards closure_ If `(kind) A @abstract` and $`A \leq B`$ then `(kind) B (sub ...)`cannot be declared non-abstractly.

##### **Case REL_ABSTRACT_DEF**
* 🔶 `A relates I @abstract` adds $`\diamond(A : \mathbf{Rel}(I))`$ which affects `insert` behavior.
* 🔶 `A relates I as J @abstract` adds $`\diamond(A : \mathbf{Rel}(I))`$ which affects `insert` behavior.
* 🔶 `A relates I[] @abstract` adds $`\diamond(A : \mathbf{Rel}([I]))`$ which affects `insert` behavior.
* 🔶 `A relates I[] as J[] @abstract` adds $`\diamond(A : \mathbf{Rel}([I]))`$ which affects `insert` behavior.

_System property_

1. 🔶 _Abstract trait inheritance_. Abstract traits are inherited if not specialized just like non-abstract traits. In math: if $`\diamond(B : \mathbf{Rel}(J))`$ and $`A \lneq B`$ without specializing $J$ (i.e. $`\not \exists I. A(I) \leq B(J)`$) then the type system will infer $`\diamond(A : \mathbf{Rel}(J))`$.
1. 🔶 _Upwards closure_. When $`\diamond(A : \mathbf{Rel}(I))`$ and $`A(I) \leq B(J)`$ then $`\diamond(B : \mathbf{Rel}(J))`$

_Remark_: In addition to user declarations, let's also recall the **three cases** in which `relates @abstract` gets implicitly inferred by the type system:
* Un-specialization: if a relation type relates a specialized trait, _then_ it abstractly relates the unspecialized versions of the trait.
* Un-specialization for lists: if a relation type relates a specialized list trait, _then_ it abstractly relates the unspecialized versions of the list trait.
* Un-ordering: if a relation type relates a list trait, _then_ it abstractly relates the "un-ordered" (un-listed?) trait.


##### **Case PLAYS_ABSTRACT_DEF**
* 🔶 `A plays B:I @abstract` adds $`\diamond(A <_! I)`$ which affects `insert` behavior.

_System property_

1. 🔶 _Upwards closure_. If `A plays B:I @abstract` and $`B'(I) \leq B'(I')`$ then `A plays B':I'` cannot be declared non-abstractly.

##### **Case OWNS_ABSTRACT_DEF**
* 🔶 `A owns B @abstract` adds $`\diamond(A <_! O_B)`$ which affects `insert` behavior.
* 🔶 `A owns B[] @abstract` adds $`\diamond(A <_! O_{B[]})`$ which affects `insert` behavior.

_Remark_: Recall also that this constraint may be inferred (cf. "Un-ordering"): if a object type owns a list attribute then it abstractly owns the "un-ordered" (un-listed?) attribute.

_System property_

1. 🔶 _Upwards closure_. If `A owns B @abstract` and $`B \leq B'`$ then `A owns B'` cannot be declared non-abstractly.

##### **Case DISTINCT_DEF**
* 🔶 `A owns B[] @distinct` postulates that when $`[b_1, ..., b_n] : [B]`$ then all $`b_i`$ are distinct. 
* 🔶 `B relates I[] @distinct` postulates that when $`[x_1, ..., x_n] : [I]`$ then all $`x_i`$ are distinct.

#### Values

##### **Case OWNS_VALUES_DEF**
* 🔷 `A owns B @values(v1, v2)` postulates if $`a : A`$ then $`\mathsf{val}(a) \in \{v_1, v_2\}`$ , where $`A : \mathbf{Att}`$, $`\mathsf{val} : A \to V`$, $`v_i : V`$, 
* 🔮  `A owns B[] @values(v1, v2)` postulates if $`l : [A]`$ and $`a \in l`$ then $`a \in \{v_1, v_2\}`$ , where $`A : \mathbf{Att}`$, $`\mathsf{val} : A \to V`$, $`v_i : V`$, 
  
  **Generalizes** to $`n`$ values.
* 🔷 `A owns B @regex(<REGEX>)` postulates if $`a : A`$ then $`a`$ conforms with regex `<EXPR>`.
* 🔮  `A owns B[] @regex(<REGEX>)` ... (similar, for individual list members)
* 🔷 `A owns B @range(v1..v2)` postulates if $`a : A`$ then $`a \in [v_1,v_2]`$ (conditions as before).
* 🔮  `A owns B[] @range(v1..v2)` ... (similar, for individual list members)

##### **Case VALUE_VALUES_DEF**
* 🔷 `A value B @values(v1, v2)` postulates if $`a : A`$ then $`\mathsf{val}(a) \in \{v_1, v_2\}`$ , where: 
  * either $`A : \mathbf{Att}`$, $`\mathsf{val} : A \to V`$, $`v_i : V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.
  
  **Generalizes** to $`n`$ values.
* 🔷 `A value B @regex(<REGEX>)` postulates if $`a : A`$ then $`a`$ conforms with regex `<REGEX>`, where: 
  * either $`A : \mathbf{Att}`$, $`\mathsf{val} : A \to V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.
* 🔷 `A value B @range(v1..v2)` postulates if $`a : A`$ then $`a \in [v_1,v_2]`$ (conditions as before), where: 
  * either $`A : \mathbf{Att}`$, $`\mathsf{val} : A \to V`$, 
  * or $`A`$ is the component of a struct, see section on struct defs.

### Triggers

#### **Case DEPENDENCY_DEF** (CASCADE/INDEPEDENT)
* 🔮  `(relation) B relates I @cascade`: deleting $`a : A`$ with existing $`b :_! B(a:I,...)`$, such that $`b :_! B(...)`$ violates $`B`$'s cardinality for $`I`$, triggers deletion of $`b`$.
  * **defaults** to transaction time error
* 🔮  `(relation) B @cascade`: deleting $`a : A`$ with existing $`b :_! B(a:I,...)`$, such that $`b :_! B(...)`$ violates $`B`$'s cardinality _for any role_ of $`B`$, triggers deletion of $`b`$.
  * **defaults** to transaction time error
* 🔷 `(attribute) B @independent`. When deleting $`a : A`$ with existing $`b :_! B(a:O_B)`$, update the latter to $`b :_! B`$.
  * **defaults** to: deleting $`a : A`$ with existing $`b :_! B(a:O_B)`$ triggers deletion of $`b`$.


### Value types

#### **Case PRIMITIVES_DEF**
* ➖ `bool`
  * Terms: `true`, `false`
* ➖ `long` — _Comment: still think this could be named more nicely_
  * Terms: 64bit integers
* ➖ `double` 
  * Terms: 64bit doubles
* ➖ `decimal` 
  * Terms: 64bit with fixed e19 resolution after decimal point
* ➖ `date`
  * See expression grammar for valid formats
* ➖ `datetime`
  * See expression grammar for valid formats
* ➖ `datetime_tz`
  * See expression grammar for valid formats
* ➖ `duration`
  * See expression grammar for valid formats
* ➖ `string`
  * Terms: arbitrary sized strings

#### **Case STRUCT_DEF**

* 🔷 _struct_ definition takes the form: 
    ```
    struct S:
      C1 value V1? (@values(<EXPR>)),
      C2 value V2 (@values(<EXPR>));
    ```
    which adds
    * _Struct type and components_: $`S : \mathbf{Type}`$, $`C_1 : \mathbf{Type}`$, $`C_2 : \mathbf{Type}`$
    * _Struct identity_:  $`S = C_1? \times C_2`$ (_Note_: the option type operator matches the use of `?` above.)
        * _Component value casting rule_: $`C_1 \leq V_1`$, $`C_2 \leq V_2`$
        * _Component value constraint rule_: whenever $`v : V_i`$ and $`v`$ conforms with `<EXPR>` then $`v : C_i`$
          * **defaults** to: whenever $`v : V_i`$ then $`v : C_i`$ (no condition)
    * **Generalizes** to $`n`$ components

### Functions defs

#### **Case STREAM_RET_FUN_DEF**

* 🔷 _stream-return function_ definition takes the form: 
    ```
    fun F <SIGNATURE_STREAM_FUN>:
    <READ_PIPELINE_FUN>
    <RETURN_STREAM_FUN> 
    ```

_Note_ See "Function semantics" for details on this syntax.

#### **Case SINGLE_RET_FUN_DEF**
* 🔷 _single-return function_ definition takes the form: 
    ```
    fun f <SIGNATURE_SINGLE_FUN>:
    <READ_PIPELINE_FUN>
    <RETURN_SINGLE_FUN> 
    ```

_Note_ See "Function semantics" for details.

## Undefine semantics

`undefine` clauses comprise _undefine statements_ which are described in this section.

_Principles._

* ➖ `undefine` removes axiom, constraints, triggers, value types, or functions
* ➖ `undefine` **can be a no-op**

### Type axioms

#### **Case ENT_UNDEF**
* ➖ `entity A` removes $`A : \mathbf{Ent}`$
* ➖ `sub B from (entity) A` removes $`A \leq B`$

#### **Case REL_UNDEF**
* ➖ `relation A` removes $`A : \mathbf{Rel}`$
* ➖ `sub B from (relation) A` removes $`A \leq B`$
* ➖ `relates I from (relation) A` removes $`A : \mathbf{Rel}(I)`$
* ➖ `as J from (relation) A relates I` removes $`I <_! J`$ 
* 🔶 `relates I[] from (relation) A` removes $`A : \mathbf{Rel}([I])$
* 🔶 `as J[] from (relation) A relates I[]` removes $`I <_! J`$

#### **Case ATT_UNDEF**
* ➖ `attribute A` removes $`A : \mathbf{Att}`$ and $`A : \mathbf{Att}(O_A)`$
* ➖ `value V from (attribute) A value V` removes $`\mathsf{val} : A \to V`$
* ➖ `sub B from (attribute) A` removes $`A <_! B`$ and $`O_A <_! O_B`$

#### **Case PLAYS_UNDEF**
* ➖ `plays B:I from (kind) A` removes $`A <_! I`$ 

#### **Case OWNS_UNDEF**
* ➖ `owns B from (kind) A` removes $`A <_! O_B`$ 
* 🔶 `owns B[] from (kind) A` removes $`A <_! O_{B[]}`$

### Constraints

_In each case, `undefine` removes the postulated condition (restoring the default)._ (minor exception: subkey)

#### Cardinality

##### **Case CARD_UNDEF**
* ➖ `@card(n..m) from A relates I`
* 🔶 `@card(n..m) from A plays B:I`
* ➖ `@card(n...m) from A owns B`

##### **Case CARD_LIST_UNDEF**
* 🔶 `@card(n..m) from A relates I[]`
* 🔶 `@card(n...m) from A owns B[]`


#### Modalities

##### **Case UNIQUE_UNDEF**
* 🔷 `@unique from A owns B`

##### **Case KEY_UNDEF**
* 🔷 `@key from A owns B`

##### **Case SUBKEY_UNDEF**
* 🔷 `@subkey(<LABEL>) from A owns B` removes $`B`$ as part of the `<LABEL>` key of $`A`$

##### **Case TYP_ABSTRACT_UNDEF**
* 🔷 `@abstract from (kind) B` 

##### **Case PLAYS_ABSTRACT_UNDEF**
* 🔷 `@abstract from A plays B:I`

##### **Case OWNS_ABSTRACT_UNDEF**
* 🔷 `@abstract from A owns B` 
* 🔶 `@abstract from A owns B[]` 

##### **Case REL_ABSTRACT_UNDEF**
* 🔷 `@abstract from A relates I` 
* 🔶 `@abstract from A relates I[]` 

##### **Case DISTINCT_UNDEF**
* 🔶 `@distinct from A owns B[]`
* 🔶 `@distinct from B relates I[]`

#### Values

##### **Case OWNS_VALUES_UNDEF**
* 🔷 `@values(v1, v2) from A owns B` 
* 🔷 `@range(v1..v2) from A owns B`

##### **Case VALUE_VALUES_UNDEF**
* 🔷 `@values(v1, v2) from A value B` 
* 🔷 `@range(v1..v2) from A value B`


### Triggers

_In each case, `undefine` removes the triggered action._

#### **Case DEPENDENCY_UNDEF** (CASCADE/INDEPEDENT)
* 🔮 `@cascade from (relation) B relates I`
* 🔮 `@cascade from (relation) B`
* 🔷 `@independent from (attribute) B`

### Value types

#### **Case PRIMITIVES_UNDEF**
cannot undefine primitives

#### **Case STRUCT_UNDEF**

* 🔶 `struct S;`
  removes $S : \mathbf{Type}$ and all associated defs.

_System property_

1. _In-use check_. error if
  * 🔶 $`S`$ is used in another struct
  * 🔶 $`S`$ is used as value type of an attribute

### Functions defs

#### **Case STREAM_RET_FUN_UNDEF**
* 🔶 `fun F;`
  removes $`F`$ and all associated defs.

_System property_

1. 🔶 _In-use check_. error if $`S`$ is used in another function


#### **Case SINGLE_RET_FUN_UNDEF**
* 🔶 `fun f;`
  removes $`f`$ and all associated defs.

_System property_

1. 🔶 _In-use check_. error if $`S`$ is used in another function

## Redefine semantics

`redefine` clauses comprise _redefine statements_ which are described in this section.

_Principles._

`redefine` redefines type axioms, constraints, triggers, structs, or functions. Except for few cases (`sub`), `redefine` **cannot be a no-op**, i.e. it always redefines something! We disallow redefining boolean properties:
  * _Example 1_: a type can either exists or not. we cannot "redefine" its existence, but only define or undefine it.
  * _Example 2_: a type is either abstract or not. we can only define or undefine `@abstract`.

_System property_: 
1. 🔮 Can have multiple statement per redefine but within a single `redefine` clause we cannot both redefine a type axiom _and_ constraints affecting that type axioms

    _Example_. We can redefine
    ```
    define person owns name @card(0..1);
    // first clause: redefine name -> name[] 
    redefine person owns name[];
    // next clause: redefine annotations
    redefine person owns name[] @card(0..3) @values("A", "B", "C");
    ```
    but we cannot redefine both in the same clause;
    ```
    redefine person owns name[] @card(0..3) @values("A", "B", "C");
    ```

### Type axioms

#### **Case ENT_REDEF**
* **cannot** redefine `entity A`
* ➖ `(entity) A sub B` redefines $`A \leq B`$

#### **Case REL_REDEF**
* **cannot** redefine `relation A` 
* ➖ `(relation) A sub B` redefines $`A \leq B`$, ***requiring*** 
  * either $`A <_! B' \neq B`$ (to be redefined)
  * or $`A`$ has no direct super-type
* ➖ `(relation) A relates I` redefines $`A : \mathbf{Rel}(I)`$, ***requiring*** that $`A : \mathbf{Rel}([I])`$ (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(0..)`) 
  * _Data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I`$
* ➖ `(relation) A relates I as J` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role
* 🔶 `(relation) A relates I[]` redefines $`A : \mathbf{Rel}([I])`$, ***requiring*** that $`A : \mathbf{Rel}(I)`$ (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(1..1)`) (STICKY)
  * _Data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I`$
* 🔶 `(relation) A relates I[] as J[]` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role

#### **Case ATT_REDEF**
* **cannot** redefine `attribute A`
* `(attribute) A value V` redefines $`\mathsf{val} : A \to V`$
* **cannot** redefine `(attribute) A sub B`

#### **Case PLAYS_REDEF**
* **cannot** redefine `(kind) A plays B:I`

#### **Case OWNS_REDEF**
* **cannot** redefine `(kind) A owns B`
* **cannot** redefine `(kind) A owns B[]`

### Constraints

_In each case, `redefine` redefines the postulated condition._

#### Cardinality

##### **Case CARD_REDEF**
* ➖ `A relates I @card(n..m)`
* 🔶  `A plays B:I @card(n..m)`
* ➖ `A owns B @card(n...m)`

##### **Case CARD_LIST_REDEF**
* ➖ `A relates I[] @card(n..m)`
* ➖ `A owns B[] @card(n...m)`


#### Modalities

Cannot redefine `@unique`, `@key`, `@abstract`, or `@distinct`.

#### Values

##### **Case OWNS_VALUES_REDEF**
* 🔷 `A owns B @values(v1, v2)` 
* 🔷 `A owns B @regex(<EXPR>)` 
* 🔷 `A owns B @range(v1..v2)`

##### **Case VALUE_VALUES_REDEF**
* 🔷 `A value B @values(v1, v2)` 
* 🔷 `A value B @regex(<EXPR>)` 
* 🔷 `A value B @range(v1..v2)`

### Triggers

_In each case, `redefine` redefines the triggered action._

#### **Case DEPENDENCY_REDEF** (CASCADE/INDEPEDENT)
* **cannot** redefine `(relation) B relates I @cascade`
* **cannot** redefine `(relation) B @cascade`
* **cannot** redefine `(attribute) B @independent`

### Value types

#### **Case PRIMITIVES_REDEF**

* **cannot** redefine primitives

#### **Case STRUCT_REDEF**

* 🔶 `redefine struct A ...` replaces the previous definition of `A` with a new one. 

### Functions defs

#### **Case STREAM_RET_FUN_REDEF**

* 🔶 `redefine fun F ...` replaces the previous definition of `F` with a new one. 

#### **Case SINGLE_RET_FUN_REDEF**

* 🔶 `redefine fun f ...` replaces the previous definition of `f` with a new one. 

## Labels and aliases

* Each type has a **label**, which is its primary identifier
* In addition, the user may _define_ (and _undefine_) any number of aliases, which can be used in place of the primary label in pattern
* Primary labels themselves can be _redefined_
* Labels and aliases must be unique (e.g. one type's label cannot be another type's alias)

### Define

* 🔷 **adding aliases**
```
define person alias p, q, r;
define marriage:spouse alias marriage:p, marriage:q, marriage:r;
```

### Undefine

* 🔷 **removing aliases**
```
undefine alias p, q, r from person;
undefine alias marriage:p, marriage:q, marriage:r from marriage:spouse;
```

### Redefine 

* 🔷 **changing primary label**
```
redefine person label animal;
redefine marriage:spouse label marriage:super_spouse;
```


