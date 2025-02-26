# The type system

> **Types** are organizational tool of formal languages, and the analogs of **sets** in classical mathematics. 

TypeDB's type system is a set of rules describing:

* What types the system contains (how to create them, etc.) 
* What terms these types contain
* Which relations (e.g. subtyping) and functions (e.g. casting) between types the system supports

This document introduces the type system in two stages (which is common)

* We first introduce and explain the **grammar** of statements in the system.
* We then discuss the **rule system** for inferring which statements are _true_.

## Grammar and notations

### Ordinary types

We discuss the grammar for statements relating to types, and explain them in natural language statements.

* **Ordinary type kinds**. We write 
  $`A : \mathbf{Kind}`$ to mean the statement:
  > $`A`$ is a type of kind $`\mathbf{Kind}`$. 

  The type system allows six cases of **type kinds**:
    * $`\mathbf{Ent}`$ (collection of **entity types**)
    * $`\mathbf{Rel}`$ (collection of **relation types**)
    * $`\mathbf{Att}`$ (collection of **attribute types**)
    * $`\mathbf{Trait}`$ (collection of **trait types**)
    * $`\mathbf{Val}`$ (collection of **value types**)
    * $`\mathbf{List}`$ (collection of **list types**)

  _Example_: $`\mathsf{Person} : \mathbf{Ent}`$ means $`\mathsf{Person}`$ an entity type.

* **Combined kinds notation**. The following are useful abbreviations:
  * $`\mathbf{Obj} = \mathbf{Ent} + \mathbf{Rel}`$ (collection of **object types**)
  * $`\mathbf{Dep} = \mathbf{Rel} + \mathbf{Att}`$ (collection of **dependent types**)
  * $`\mathbf{ERA} = \mathbf{Ent} + \mathbf{Rel} + \mathbf{Att}`$ (collection of **ERA types**)
  * $`\mathbf{Label} = \mathbf{ERA} + \mathbf{Trait} + \mathbf{Value}`$ (collection of all **labeled types**)
  * $`\mathbf{Alg} = \mathbf{Op}^*(\mathbf{Label})`$ (collection of all **algebraic types**, obtained by closing simple types under operators: sum, product, option... see "Type operators" below) *[USE-CASE: type-inference]*
  * $`\mathbf{Type} = \mathbf{Alg} + \mathbf{List}`$ (collection of all **types**)

* **Typing of elements**
  If $`A`$ is a type, then we may write $`a : A`$ to mean the statement:
  > $`a`$ is an element in type $`A`$.

  _Example_: $`p : \mathsf{Person}`$ means $`p`$ is of type $`\mathsf{Person}`$

* **Direct typing**: We write $`a :_! A`$ to mean:
  > $`a`$ was declared as an element of $`A`$ by the user (we speak of a ***direct typing***).

  _Remark_. The notion of direct typing might be confusing at first. Mathematically, it is merely an additional statement in our type system. Intuively, you can think of it as a way of keeping track of the _user-provided_ information. A similar remark applies to direct subtyping ($`<_!`$) below.

### Dependent types

We discuss the grammar for statements relating to dependent types, and explain them in natural language statements.

* **Dependent type kinds**. We write $`A : \mathbf{Kind}(I,J,...)`$ to mean:
  > $`A`$ is a type of kind $`\mathbf{Kind}`$ with **trait types** $`I, J, ...`$.

  The type system allows two cases of **dependent type kinds**:
    * $`\mathbf{Att}`$ (collection of **attribute types**)
        * Attribute traits are called **ownership types**
    * $`\mathbf{Rel}`$ (collection of **relation types**)
        * Relation traits are called **role types**

  _Example_: $`\mathsf{Marriage : \mathbf{Rel}(Spouse)}`$ is a relation type with trait type $`\mathsf{Spouse} : \mathbf{Trait}`$.

* **Dependent typing**.  We write $`a : A(x : I, y : J,...)`$ to mean:
  > The element $`a`$ lives in the type "$`A`$ of $`x`$ (cast as $`I`$), and $`y`$ (cast as $`J`$), and ...".

* **Dependency deduplication (+set notation)**:  Our type system rewrites dependencies by removing duplicates in the same trait, i.e. $`a : A(x : I, y : I, y : I)`$ is rewritten to (and identified $`a : A(x : I, y : I)`$. In other words:
  > We **deduplicate** dependencies on the same element in the same trait.
  
  It is therefore convenient to use _set notation_, writing $`a : A(x : I, y : I)`$ as $`A : A(\{x,y\}:I)`$. (Similarly, when $`I`$ appears $`k`$ times in $`A(...)`$, we would write $`\{x_1, ..., x_k\} : I`$) 

* **Trait specialization notation**:  If $`A : \mathbf{Kind}(J)`$, $`B : \mathbf{Kind}(I)`$, $`A \lneq B`$ and $`J \lneq I`$, then we say:
  > The trait $`J`$ of $`A`$ **specializes** the trait $`I`$ of $`B`$

  We write this as $`A(J) \leq B(I)`$. 

* **Role cardinality notation**: $`|a|_I`$ counts elements in $`\{x_1,...,x_k\} :I`$

  _Example_: $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse})`$. Then $`|m|_{\mathsf{Spouse}} = 2`$.

### Subtypes and castings

We discuss the grammar for statements relating to subtypes (which allow _implicit casting_) and explicit castings, and explain them in natural language statements.

* **Subtyping**. We write $`A \leq B`$ to mean:
  > Implicit casts from $`A`$ to $`B`$ are possible.

    _Example_ The $`\mathsf{Child} \leq \mathsf{Person}`$ means children $`c`$ can cast into persons $`c`$.

* **Direct subtyping**: We write $`A <_! B`$ to mean:
    > An implicit cast from A to B was declared by user (we speak of a ***direct subtyping*** of A into B).

    _Example_: $`\mathsf{Child} <_! \mathsf{Person}`$

    _Example_: $`\mathsf{Child} <_! \mathsf{NicknameOwner}`$

    _Example_: $`\mathsf{Person} <_! \mathsf{Spouse}`$

* **Explicit castings**: We write $`f : A \to B`$ to mean:
  > An explicity cast $f$ from $`A`$ to $`B`$ is possible.

    _Example_ The $`\mathsf{val} : \mathsf{Name} \to \mathsf{String}`$ means names $`n`$ can be cast to string $`\mathsf{val}(n)`$.

### Algebraic type operators

We discuss the grammar for statements relating to operators and modalitites, and explain them in natural language statements.

* **Sum operator**. we may construct $`A + B : \mathbf{Alg}`$ for $`A, B :\mathbf{Alg}`$ 

    > this is the sum type of $`A`$ and $`B`$, containing all elements of $`A`$ and of $`B`$
* **Product operator**. we may construct $`A \times B : \mathbf{Alg}`$ for $`A, B :\mathbf{Alg}`$

    > this is the sum type of $`A`$ and $`B`$, containing all elements of $`A`$ and of $`B`$
* **Option operator**. we may construct $`A? : \mathbf{Alg}`$ for $`A :\mathbf{Alg}`$ 

    > this is the option type, containing $`A`$ plus the empty element $`\emptyset`$
* **Cardinality operator**.$`|A| : \mathbb{N}`$ for $`A : \mathbf{Alg}`$. 

    > This is the cardinality of $`A`$, counting the elements in $`A`$.

_Remark for nerds: list types are not algebraic types... they are so-called inductive types!_

### List types

We discuss the grammar for statements relating to list types, and explain them in natural language statements.

* **List types**. For any $`A : \mathbf{Alg}`$, we write $`[A] : \mathbf{List}`$ to mean
  > the type of $`A`$-lists, i.e. the type which contains lists $`[a_0, a_1, ...]`$ of elements $`a_i : A`$.

    _Example_: Since $`\mathsf{Person} + \mathsf{City} : \mathbf{Alg}`$ we may consider $`[\mathsf{Person} + \mathsf{City}] : \mathbf{List}`$ — these are lists of persons or cities.

* **Dependency on list trait (of relation)**: For $`A : \mathbf{Rel}`$, we may have statements $`A : \mathbf{Rel}([I])`$. Thus, our type system has types of the form $`A(x:[I]) : \mathbf{Rel}`$.
    > $`A(x:[I])`$ is a relation type with dependency on a list $`x : [I]`$.

    _Example_: $`\mathsf{FlightPath} : \mathbf{Rel}([\mathsf{Flight}])`$

* **Dependent list type (of attributes)**: For any $`A : \mathbf{Att}(I)`$, we introduce $`[A] : \mathbf{List}(I_{[]})`$ where $`I_{[]}`$ is the **list version** of $`I`$. Thus, our type system has types of the form $`[A](x:I) : \mathbf{List}`$.
    > $`[A](x:I)`$ is a type of $`A`$-lists depending on trait $`I`$.

    _Example_: For $`[\mathsf{Name}] : \mathbf{List}(\mathsf{NameOwner})`$, we may have attribute lists $`[a,b,c] : [\mathsf{Name}](x : \mathsf{NameOwner})`$

* **List length notation**: for list $`l : [A]`$ the term $\mathrm{len}(l) : \mathbb{N}$ represents $`l`$'s length.

### Modalities

The following is purely for keeping track of certain information in the type system.

* **Abstract modality**. Certain statements $`\mathcal{J}`$ about types (namely: type kinds $`A : \mathbf{Kind}(...)`$ and subtyping $`A < B`$) have abstract versions written $`\diamond(\mathcal{J})`$.

    > If $`\diamond(\mathcal{J})`$ is true then this means $`\mathcal{J}`$ is _abstractly true_ in the type system, where "abstractly true" is a special truth-value which entails certain special behaviors in the database.

_Remark_: **Key**, **subkey**, **unique** could also be modalities, but for simplicity (and to reduce the amount of symbols in our grammar), we'll leave them be and only keep track of them in pure TypeQL.

## Rule system

This section describes the **rules** that govern the interaction of statements.

### Ordinary types

* **Direct typing rule**: The statement $`a :_! A`$ implies the statement $`a : A`$. (The converse is not true!)

  _Example_. $`p :_! \mathsf{Child}`$ means the user has inserted $`p`$ into the type $`\mathsf{Child}`$. Our type system may derive $`p : \mathsf{Person}`$ from this (but _not_ $`p :_! \mathsf{Person}`$)

### Dependendent types


* **Applying dependencies**: Writing $A : \mathbf{Kind}(I,J,...)$ *implies* $`A(x:I, y:J, ...) : \mathbf{Kind}`$ whenever we have $`x: I, y: J, ...`$.
  > We say $`A(x:I)`$ is the type $`A`$ with "applied dependency" $`x : I`$. In contrast, $`A`$ by itself is a "unapplied" type.

* **Combining dependencies**: Given $A : \mathbf{Kind}(I)$ and $`A : \mathbf{Kind}(J)`$, this *implies* $`A : \mathbf{Kind}(I,J)`$. In words:
  > If a type separately depends on $`I`$ and on $`J`$, then it may jointly depend on $`I`$ and $`J`$! 

  _Remark_: This applies recursively to types with $`k`$ traits.

  _Example_: $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband})`$ and $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Wife})`$ then $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband},\mathsf{Wife})`$

* **Weakening dependencies**: Given $`A : \mathbf{Kind}(I,J)`$, this *implies* $`A : \mathbf{Kind}(I)`$. In words:
  > Dependencies can be simply ignored (note: this is a coarse rule — we later discuss more fine-grained constraints, e.g. cardinality).

  _Remark_: This applies recursively to types with $`k`$ traits.

  _Example_: $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ implies $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ and also $`\mathsf{Marriage} : \mathbf{Rel}`$ (we identify the empty brackets "$`()`$" with no brackets).

* **Auto-inheritance rule**: If $`A : \mathbf{Kind}`$, $`B : \mathbf{Kind}(I)`$, $`A \leq B`$ and $`A`$ has no trait strictly specializing $`I`$ then $`A : \mathbf{Kind}(I)`$ ("strictly" meaning "not equal to $`I`$"). In words:

  > Dependencies that are not specialized are inherited.
    
### Subtypes and castings

Beside the rules below, subtyping ($`\leq`$) is transitive and reflexive.

* **Subtyping rule**: If $`A \leq B`$ is true and $`a : A`$ is true, then this *implies* $`a : B`$ is true.

* **Explicit casting rule**: If $`f : A \to B`$ is true and $`a : A`$ is true, then this *implies* $`f(a) : B`$ is true.

* **Direct-to-general rule**: $`A <_! B`$ *implies* $`A \leq B`$.

* **"Weakening dependencies of terms" rule**: If $`a : A(x:I, y:J)`$ then this *implies* $`a : A(x:I)`$, equivalently: $`A(x:I, y:J) \leq A(x:I)`$. In other words:
    > Elements in $`A(I,J)`$ casts into elements of $`A(I)`$.

    * _Remark_: More generally, this applies for types with $k \leq 0$ traits. (In particular, $`A(x:I) \leq A() = A`$)
    * _Example_: If $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse})`$ then both $`m : \mathsf{Marriage}(x:\mathsf{Spouse})`$ and $`m : \mathsf{Marriage}(y:\mathsf{Spouse})`$

* **"Covariance of dependencies" rule**: If $`A(J) \leq B(I)`$ (see "trait specialization" in "Grammar and notation" above) and $`a : A(x:I)`$ then this _implies_ $`a : B(x:J)`$. In other words:
    > When $`A`$ casts to $`B`$, and $`I`$ to $`J`$, then $`A(x : I)`$ casts to $`B(x : J)`$.

    _Remark_: This applies recursively for types with $`k`$ traits.

    _Example_: If $`r : \mathsf{HeteroMarriage}(x:\mathsf{Husband}, y:\mathsf{Wife})`$ then $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse})`$

The next rule is special to attributes, describing their interactions with value types.

* **Attribute identity rule**. If $`V : \mathbf{Val}`$, $`A : \mathbf{Att}`$, $`\mathsf{val} : A \to V`$, and $`a, b :_! A`$ such that $`\mathsf{val}(a) =\mathsf{val}(b)`$  then we identify $`a = b`$ for all purposes.

### Algebraic type operators

* **Sums**: Sum types follow the usual rules of type system.

    _Note_: the inclusion $`A \leq A + B`$ is also **subsumptive** subtyping. This induces certain equalities on elements: for example, if $`A \leq B`$, then $`A + B = B`$. Indeed when $`a : A`$ then $`a : B`$ and both include into $`A + B`$ as the same element: $`a = a : A + B`$ ... (therefore, technically $`A + B`$ is the so-called _fibered_ sum over the intersection $`A \cap B`$). We omit the detailed rules here, and use common sense.

* **Products**: Product types follow the usual rules of type systems.

    _Note_: again, subsumptive subtyping interacts with this. For example, we would have if $`A \leq A'$ and $B \leq B'`$ then $`A \times B \leq A' \times B'`$ ...  We omit the detailed rules here, and use common sense.

* **Options**: Option types follow the usual rules of type systems (effectively, options are sum types: $`A? = A + \{ \emptyset \}`$).

    _Note_: same note as before applies. Also note $`A?? = A?`$ due to subsumptive subtyping.

### List types

* **Direct typing list rule**: Given $`l = [l_0,l_1,...] :_! [A](x: I_{[]})`$ this implies $`l_i :_! A(x: I)`$. In other words:
  > If the user intends a list typing $`l :_! [A]`$ then the list entries $`l_i`$ will be direct elements of $`A`$.

* **Direct dependency list rule**: Given $`l = [l_0,l_1,...] : [I]`$ and $`a :_! A(l : [I])`$ implies $`a :_! A(l_i : I)`$. In other words:
  > If the user intends $a$ to directly depend on the list $`l`$ then they intend $a$ to directly depend on each list's entries $`l_i`$.

* **Empty attribute lists**: For $`A : \mathbf{Att}(I)`$ and $`x : I_{[]}`$ such that no non-empty list $`l : `[A](x : I_{[]})`$ exists then this implies an empty **"default"** list $`[] : [A](x : I_{[]})`$.

_Note_. The last rule is the reason why we don't need the type `[A]?` in our type system — the "None" case is simple the empty list.

_Note 2_. List types also interact with subtyping in the obvious way: when `A \leq B` then `[A] \leq [B]`. 

### Modalities

There are no specific rules relating to modalities.
They are just a tool for keeping track of additional properties.

_Remark_: Recall from an earlier remark, **key**, **subkey**, **unique** could also be modalities, but for simplicity (and to reduce the amount of symbols in our grammar), we'll leave them be and only keep track of them in pure TypeQL.


