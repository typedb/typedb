# TypeDB - Behaviour Specification

<!-- TODO
* Add function calls to expressions
* Add vector search and embedding models
[DONE] * Ensure role player set semantics
[DONE]  * idempotency non-list inserts
* Improve explanation of dependent list types
[DONE] * distinct, select distinct
[DONE] * simplify match semantics. in crows have
  * type vars (typed by type kinds)
  * instance vars (always have direct type)
  * value vars (always have value type)
  * list vars (can have any list type)
[DONE] * cascade
[DONE] * "coarsen" and "refine" terminology
[DONE] * relates abstract
[DONE] * owns order in match semantics
[DONE] * "overwrite"/override -> specialization
[DONE] * reduce in fetch returning single value using curved brackets ()
[DONE] * fill in operators
-->

<!-- To be discussed:
* when the _same_ insert clause inserts the same list attribute twice ... throw?
*
-->

<details>
  <summary> <b>Table of contents</b> <i>(Overview)</i> </summary>

<!-- vim-markdown-toc GFM -->

* [Introduction](#introduction)
* [The type system](#the-type-system)
    * [Grammar and notations](#grammar-and-notations)
    * [Rule system](#rule-system)
* [Data definition language](#data-definition-language)
    * [Basics of schemas](#basics-of-schemas)
    * [Define semantics](#define-semantics)
    * [Undefine semantics](#undefine-semantics)
    * [Redefine semantics](#redefine-semantics)
    * [Labels and aliases](#labels-and-aliases)
* [Pattern matching language](#pattern-matching-language)
    * [Basics: Patterns, variables, concept rows, satisfaction](#basics-patterns-variables-concept-rows-satisfaction)
    * [Pattern semantics](#pattern-semantics)
    * [Type patterns](#type-patterns)
    * [Type constraint patterns](#type-constraint-patterns)
    * [Element patterns](#element-patterns)
    * [Expression and list patterns](#expression-and-list-patterns)
    * [Function patterns](#function-patterns)
    * [Patterns of patterns](#patterns-of-patterns)
* [Data manipulation language](#data-manipulation-language)
    * [Match semantics](#match-semantics)
    * [Functions semantics](#functions-semantics)
    * [Insert semantics](#insert-semantics)
    * [Delete semantics](#delete-semantics)
    * [Update semantics](#update-semantics)
    * [Put semantics](#put-semantics)
* [Query execution principles](#query-execution-principles)
    * [Basics: Pipelines, clauses, operators, branches](#basics-pipelines-clauses-operators-branches)
    * [Clauses (match, insert, delete, update, put, fetch)](#clauses-match-insert-delete-update-put-fetch)
    * [Operators (select, distinct, sort, limit, offset, reduce)](#operators-select-distinct-sort-limit-offset-reduce)
    * [Branches](#branches)
    * [Transactions](#transactions)
* [Glossary](#glossary)
    * [Type system](#type-system)
    * [TypeQL syntax](#typeql-syntax)
    * [Syntactic Sugar](#syntactic-sugar)
    * [Typing of operators](#typing-of-operators)

<!-- vim-markdown-toc -->

</details>


<details>
  <summary> <b>Table of contents</b> <i>(Detailed)</i> </summary>

<!-- vim-markdown-toc GFM -->

* [Introduction](#introduction)
* [The type system](#the-type-system)
    * [Grammar and notations](#grammar-and-notations)
        * [Ordinary types](#ordinary-types)
        * [Dependent types](#dependent-types)
        * [Subtypes and castings](#subtypes-and-castings)
        * [Algebraic type operators](#algebraic-type-operators)
        * [List types](#list-types)
        * [Modalities](#modalities)
    * [Rule system](#rule-system)
        * [Ordinary types](#ordinary-types-1)
        * [Dependendent types](#dependendent-types)
        * [Subtypes and castings](#subtypes-and-castings-1)
        * [Algebraic type operators](#algebraic-type-operators-1)
        * [List types](#list-types-1)
        * [Modalities](#modalities-1)
* [Schema definition language](#schema-definition-language)
    * [Basics of schemas](#basics-of-schemas)
        * [(Theory) Clauses and execution](#theory-clauses-and-execution)
        * [(Feature) Pipelines and match-define's](#feature-pipelines-and-match-defines)
    * [Define semantics](#define-semantics)
        * [Type axioms](#type-axioms)
            * [**Case ENT_DEF**](#case-ent_def)
            * [**Case REL_DEF**](#case-rel_def)
            * [**Case ATT_DEF**](#case-att_def)
            * [**Case PLAYS_DEF**](#case-plays_def)
            * [**Case OWNS_DEF**](#case-owns_def)
        * [Constraints](#constraints)
            * [Cardinality](#cardinality)
                * [**Case CARD_DEF**](#case-card_def)
                * [**Case CARD_LIST_DEF**](#case-card_list_def)
            * [Modalities](#modalities-2)
                * [**Case UNIQUE_DEF**](#case-unique_def)
                * [**Case KEY_DEF**](#case-key_def)
                * [**Case SUBKEY_DEF**](#case-subkey_def)
                * [**General case: ABSTRACT_DEF**](#general-case-abstract_def)
                * [**Case TYP_ABSTRACT_DEF**](#case-typ_abstract_def)
                * [**Case REL_ABSTRACT_DEF**](#case-rel_abstract_def)
                * [**Case PLAYS_ABSTRACT_DEF**](#case-plays_abstract_def)
                * [**Case OWNS_ABSTRACT_DEF**](#case-owns_abstract_def)
                * [**Case DISTINCT_DEF**](#case-distinct_def)
            * [Values](#values)
                * [**Case OWNS_VALUES_DEF**](#case-owns_values_def)
                * [**Case VALUE_VALUES_DEF**](#case-value_values_def)
        * [Triggers](#triggers)
            * [**Case DEPENDENCY_DEF** (CASCADE/INDEPEDENT)](#case-dependency_def-cascadeindepedent)
        * [Value types](#value-types)
            * [**Case PRIMITIVES_DEF**](#case-primitives_def)
            * [**Case STRUCT_DEF**](#case-struct_def)
        * [Functions defs](#functions-defs)
            * [**Case STREAM_RET_FUN_DEF**](#case-stream_ret_fun_def)
            * [**Case SINGLE_RET_FUN_DEF**](#case-single_ret_fun_def)
    * [Undefine semantics](#undefine-semantics)
        * [Type axioms](#type-axioms-1)
            * [**Case ENT_UNDEF**](#case-ent_undef)
            * [**Case REL_UNDEF**](#case-rel_undef)
            * [**Case ATT_UNDEF**](#case-att_undef)
            * [**Case PLAYS_UNDEF**](#case-plays_undef)
            * [**Case OWNS_UNDEF**](#case-owns_undef)
        * [Constraints](#constraints-1)
            * [Cardinality](#cardinality-1)
                * [**Case CARD_UNDEF**](#case-card_undef)
                * [**Case CARD_LIST_UNDEF**](#case-card_list_undef)
            * [Modalities](#modalities-3)
                * [**Case UNIQUE_UNDEF**](#case-unique_undef)
                * [**Case KEY_UNDEF**](#case-key_undef)
                * [**Case SUBKEY_UNDEF**](#case-subkey_undef)
                * [**Case TYP_ABSTRACT_UNDEF**](#case-typ_abstract_undef)
                * [**Case PLAYS_ABSTRACT_UNDEF**](#case-plays_abstract_undef)
                * [**Case OWNS_ABSTRACT_UNDEF**](#case-owns_abstract_undef)
                * [**Case REL_ABSTRACT_UNDEF**](#case-rel_abstract_undef)
                * [**Case DISTINCT_UNDEF**](#case-distinct_undef)
            * [Values](#values-1)
                * [**Case OWNS_VALUES_UNDEF**](#case-owns_values_undef)
                * [**Case VALUE_VALUES_UNDEF**](#case-value_values_undef)
        * [Triggers](#triggers-1)
            * [**Case DEPENDENCY_UNDEF** (CASCADE/INDEPEDENT)](#case-dependency_undef-cascadeindepedent)
        * [Value types](#value-types-1)
            * [**Case PRIMITIVES_UNDEF**](#case-primitives_undef)
            * [**Case STRUCT_UNDEF**](#case-struct_undef)
        * [Functions defs](#functions-defs-1)
            * [**Case STREAM_RET_FUN_UNDEF**](#case-stream_ret_fun_undef)
            * [**Case SINGLE_RET_FUN_UNDEF**](#case-single_ret_fun_undef)
    * [Redefine semantics](#redefine-semantics)
        * [Type axioms](#type-axioms-2)
            * [**Case ENT_REDEF**](#case-ent_redef)
            * [**Case REL_REDEF**](#case-rel_redef)
            * [**Case ATT_REDEF**](#case-att_redef)
            * [**Case PLAYS_REDEF**](#case-plays_redef)
            * [**Case OWNS_REDEF**](#case-owns_redef)
        * [Constraints](#constraints-2)
            * [Cardinality](#cardinality-2)
                * [**Case CARD_REDEF**](#case-card_redef)
                * [**Case CARD_LIST_REDEF**](#case-card_list_redef)
            * [Modalities](#modalities-4)
            * [Values](#values-2)
                * [**Case OWNS_VALUES_REDEF**](#case-owns_values_redef)
                * [**Case VALUE_VALUES_REDEF**](#case-value_values_redef)
        * [Triggers](#triggers-2)
            * [**Case DEPENDENCY_REDEF** (CASCADE/INDEPEDENT)](#case-dependency_redef-cascadeindepedent)
        * [Value types](#value-types-2)
            * [**Case PRIMITIVES_REDEF**](#case-primitives_redef)
            * [**Case STRUCT_REDEF**](#case-struct_redef)
        * [Functions defs](#functions-defs-2)
            * [**Case STREAM_RET_FUN_REDEF**](#case-stream_ret_fun_redef)
            * [**Case SINGLE_RET_FUN_REDEF**](#case-single_ret_fun_redef)
    * [Labels and aliases](#labels-and-aliases)
        * [Define](#define)
        * [Undefine](#undefine)
        * [Redefine](#redefine)
* [Pattern matching language](#pattern-matching-language)
    * [Basics: Patterns, variables, concept rows, satisfaction](#basics-patterns-variables-concept-rows-satisfaction)
        * [(Theory) Statements, patterns](#theory-statements-patterns)
        * [(Theory) Variables](#theory-variables)
        * [(Theory) Typed concept rows](#theory-typed-concept-rows)
        * [(Feature) Pattern satisfication, typing conditions, answer](#feature-pattern-satisfication-typing-conditions-answer)
        * [(Feature) Optionality and boundedness](#feature-optionality-and-boundedness)
    * [Pattern semantics](#pattern-semantics)
        * [Types](#types)
            * [**Case TYPE_DEF_PATT**](#case-type_def_patt)
            * [**Case REL_PATT**](#case-rel_patt)
            * [**Case PLAY_PATT**](#case-play_patt)
            * [**Case OWNS_PATT**](#case-owns_patt)
            * [**Cases TYP_IS_PATT and LABEL_PATT**](#cases-typ_is_patt-and-label_patt)
        * [Constraints](#constraints-3)
            * [Cardinality](#cardinality-3)
                * [**Case CARD_PATT**](#case-card_patt)
            * [Bevavior flags](#bevavior-flags)
                * [**Case UNIQUE_PATT**](#case-unique_patt)
                * [**Case KEY_PATT**](#case-key_patt)
                * [**Case SUBKEY_PATT**](#case-subkey_patt)
                * [**Case TYP_ABSTRACT_PATT**](#case-typ_abstract_patt)
                * [**Case RELATES_ABSTRACT_PATT**](#case-relates_abstract_patt)
                * [**Case PLAYS_ABSTRACT_PATT**](#case-plays_abstract_patt)
                * [**Case OWNS_ABSTRACT_PATT**](#case-owns_abstract_patt)
                * [**Case DISTINCT_PATT**](#case-distinct_patt)
            * [Values](#values-3)
                * [**Cases VALUE_VALUES_PATT and OWNS_VALUES_PATT**](#cases-value_values_patt-and-owns_values_patt)
        * [Data](#data)
            * [**Case ISA_PATT**](#case-isa_patt)
            * [**Case LINKS_PATT**](#case-links_patt)
            * [**Case HAS_PATT**](#case-has_patt)
            * [**Case IS_PATT**](#case-is_patt)
        * [Expression grammar (sketch)](#expression-grammar-sketch)
        * [Expression patterns](#expression-patterns)
            * [(Feature) Boundedness constraints](#feature-boundedness-constraints)
            * [**Case ASSIGN_PATT**](#case-assign_patt)
            * [**Case DESTRUCT_PATT**](#case-destruct_patt)
            * [**Case IN_LIST_PATT**](#case-in_list_patt)
            * [**Case EQ_PATT**](#case-eq_patt)
            * [**Case COMP_PATT**](#case-comp_patt)
        * [Functions](#functions)
            * [**Case IN_FUN_PATT**](#case-in_fun_patt)
            * [**Case ASSIGN_FUN_PATT**](#case-assign_fun_patt)
        * [Patterns](#patterns)
            * [**Case AND_PATT**](#case-and_patt)
            * [**Case OR_PATT**](#case-or_patt)
            * [**Case NOT_PATT**](#case-not_patt)
            * [**Case TRY_PATT**](#case-try_patt)
    * [Match semantics](#match-semantics)
    * [Functions semantics](#functions-semantics)
        * [Function signature, body, operators](#function-signature-body-operators)
            * [**Case FUN_SIGN_STREAM**](#case-fun_sign_stream)
            * [**Case FUN_SIGN_SINGLE**](#case-fun_sign_single)
            * [**Case FUN_BODY**](#case-fun_body)
            * [**Case FUN_OPS**](#case-fun_ops)
        * [Function body semantics](#function-body-semantics)
            * [Function evaluation](#function-evaluation)
            * [Order of execution](#order-of-execution)
        * [Stream-return semantics](#stream-return-semantics)
        * [Single-return semantics](#single-return-semantics)
    * [Insert semantics](#insert-semantics)
        * [Basics of inserting](#basics-of-inserting)
            * [(Theory) Execution](#theory-execution)
            * [(Feature) Optionality](#feature-optionality)
        * [Insert statements](#insert-statements)
            * [**Case ASSIGN_INS**](#case-assign_ins)
            * [**Case OBJ_ISA_INS**](#case-obj_isa_ins)
            * [**Case ATT_ISA_INS**](#case-att_isa_ins)
            * [**Case LINKS_INS**](#case-links_ins)
            * [**Case LINKS_LIST_INS**](#case-links_list_ins)
            * [**Case HAS_INS**](#case-has_ins)
            * [**Case HAS_LIST_INS**](#case-has_list_ins)
        * [Optional inserts](#optional-inserts)
            * [**Case TRY_INS**](#case-try_ins)
        * [Leaf attribute system constraint](#leaf-attribute-system-constraint)
    * [Delete semantics](#delete-semantics)
        * [Basics of deleting](#basics-of-deleting)
            * [(Theory) execution](#theory-execution-1)
            * [(Feature) optionality](#feature-optionality-1)
        * [Delete statements](#delete-statements)
            * [**Case CONCEPT_DEL**](#case-concept_del)
            * [**Case ROL_OF_DEL**](#case-rol_of_del)
            * [**Case ROL_LIST_OF_DEL**](#case-rol_list_of_del)
            * [**Case ATT_OF_DEL**](#case-att_of_del)
            * [**Case ATT_LIST_OF_DEL**](#case-att_list_of_del)
        * [Clean-up](#clean-up)
    * [Update semantics](#update-semantics)
        * [Basics of updating](#basics-of-updating)
            * [(Theory) Execution](#theory-execution-2)
            * [(Feature) Optionality](#feature-optionality-2)
        * [Update statements](#update-statements)
            * [**Case LINKS_UP**](#case-links_up)
            * [**Case LINKS_LIST_UP**](#case-links_list_up)
            * [**Case HAS_UP**](#case-has_up)
            * [**Case HAS_LIST_UP**](#case-has_list_up)
        * [Clean-up](#clean-up-1)
    * [Put semantics](#put-semantics)
* [Query execution principles](#query-execution-principles)
    * [Basics: Pipelines, clauses, operators, branches](#basics-pipelines-clauses-operators-branches)
    * [Clauses (match, insert, delete, update, put, fetch)](#clauses-match-insert-delete-update-put-fetch)
        * [Match](#match)
        * [Insert](#insert)
        * [Delete](#delete)
        * [Update](#update)
        * [Put](#put)
        * [Fetch](#fetch)
            * [**Case FETCH_VAL**](#case-fetch_val)
            * [**Case FETCH_EXPR**](#case-fetch_expr)
            * [**Case FETCH_ATTR**](#case-fetch_attr)
            * [**Case FETCH_MULTI_ATTR**](#case-fetch_multi_attr)
            * [**Case FETCH_LIST_ATTR**](#case-fetch_list_attr)
            * [**Case FETCH_SNGL_FUN**](#case-fetch_sngl_fun)
            * [**Case FETCH_STREAM_FUN**](#case-fetch_stream_fun)
            * [**Case FETCH_FETCH**](#case-fetch_fetch)
            * [**Case FETCH_REDUCE_VAL**](#case-fetch_reduce_val)
            * [**Case FETCH_REDUCE_LIST_VAL**](#case-fetch_reduce_list_val)
            * [**Case FETCH_NESTED**](#case-fetch_nested)
    * [Operators (select, distinct, sort, limit, offset, reduce)](#operators-select-distinct-sort-limit-offset-reduce)
        * [Select](#select)
        * [Deselect](#deselect)
        * [Distinct](#distinct)
        * [Sort](#sort)
        * [Limit](#limit)
        * [Offset](#offset)
        * [Reduce](#reduce)
            * [**Case SIMPLE_RED**](#case-simple_red)
            * [**Case GROUP_RED**](#case-group_red)
    * [Branches](#branches)
    * [Transactions](#transactions)
        * [Basics](#basics)
        * [Snapshots](#snapshots)
        * [Concurrency](#concurrency)
* [Glossary](#glossary)
    * [Type system](#type-system)
        * [Type](#type)
        * [Schema type](#schema-type)
        * [Value type](#value-type)
        * [Data instance / instance](#data-instance--instance)
        * [Data value / value](#data-value--value)
        * [Attribute instance value / attribute value](#attribute-instance-value--attribute-value)
        * [Data element / element](#data-element--element)
        * [Concept](#concept)
        * [Concept row](#concept-row)
        * [Stream](#stream)
        * [Answer set](#answer-set)
        * [Answer](#answer)
    * [TypeQL syntax](#typeql-syntax)
        * [Schema query](#schema-query)
        * [Data query](#data-query)
        * [Clause / Stream clause](#clause--stream-clause)
        * [Operators / Stream operator](#operators--stream-operator)
        * [Functions](#functions-1)
        * [Statement](#statement)
        * [Pattern](#pattern)
        * [Stream reduction / reduction](#stream-reduction--reduction)
        * [Clause](#clause)
        * [Block](#block)
        * [Suffix](#suffix)
    * [Syntactic Sugar](#syntactic-sugar)
    * [Typing of operators](#typing-of-operators)

<!-- vim-markdown-toc -->

</details>

# Introduction

This document specifies the behaviour of TypeDB and its query language TypeQL.

* Best viewed _not in Chrome_ (doesn't display math correctly)
* Badge system: 
    * ✅ -> implemented / part of alpha
    * 🔷 -> up next / part of beta
    * 🔶 -> part of first stable release
    * 🔮 -> roadmap 
    * ❓ -> speculative / to-be-discussed
    * ⛔ -> rejected

# The type system

TypeDB's type system is a [logical system](https://en.wikipedia.org/wiki/Formal_system#Deductive_system), which we describe in this section with a reasonable level of formality (not all details are included, and some basic mathematical rules are taken for granted: for example, the rule of equality, i.e. if $a = b$ then $a$ and $b$ are exchangeable for all purposes in our type system.)

It is convenient to present the type system in _two stages_ (though some people prefer to do it all in one go!):

* We first introduce and explain the **grammar** of statements in the system.
* We then discuss the **rule system** for inferring which statement are _true_.

_IMPORTANT_: Not all parts of the type system introduced in this section are exposed to the user through TypeQL (but most parts are). This shouldn't be surprising. As an analogy: the inner workings (or formal specification) of the borrow checker in Rust is not exposed in actual Rust. In other words,  defining the meaning of language often "needs more" language than the original language itself.

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
    * $`\mathbf{Itf}`$ (collection of **interface types**)
    * $`\mathbf{Val}`$ (collection of **value types**)
    * $`\mathbf{List}`$ (collection of **list types**)

  _Example_: $`\mathsf{Person} : \mathbf{Ent}`$ means $`\mathsf{Person}`$ an entity type.

* **Combined kinds notation**. The following are useful abbreviations:
  * $`\mathbf{Obj} = \mathbf{Ent} + \mathbf{Rel}`$ (collection of **object types**)
  * $`\mathbf{ERA} = \mathbf{Obj} + \mathbf{Att}`$ (collection of **ERA types**)
  * $`\mathbf{Schema} = \mathbf{ERA} + \mathbf{Itf}`$ (collection of **schema types**)
  * $`\mathbf{Label} = \mathbf{Schema} + \mathbf{Value}`$ (collection of all **labeled types**, i.e. types refered to by a single label)
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
  > $`A`$ is a type of kind $`\mathbf{Kind}`$ with **interface types** $`I, J, ...`$.

  The type system allows two cases of **dependent type kinds**:
    * $`\mathbf{Att}`$ (collection of **attribute types**)
        * Attribute interfaces are called **ownership types**
    * $`\mathbf{Rel}`$ (collection of **relation types**)
        * Relation interfaces are called **role types**

  _Example_: $`\mathsf{Marriage : \mathbf{Rel}(Spouse)}`$ is a relation type with interface type $`\mathsf{Spouse} : \mathbf{Itf}`$.

* **Dependent typing**.  We write $`a : A(x : I, y : J,...)`$ to mean:
  > The element $`a`$ lives in the type "$`A`$ of $`x`$ (cast as $`I`$), and $`y`$ (cast as $`J`$), and ...".

* **Dependency deduplication (+set notation)**:  Our type system rewrites dependencies by removing duplicates in the same interface, i.e. $`a : A(x : I, y : I, y : I)`$ is rewritten to (and identified $`a : A(x : I, y : I)`$. In other words:
  > We **deduplicate** dependencies on the same element in the same interface.
  
  It is therefore convenient to use _set notation_, writing $`a : A(x : I, y : I)`$ as $`A : A(\{x,y\}:I^2)`$. (Similarly, when $`I`$ appears $`k`$ times in $`A(...)`$, we would write $`\{x_1, ..., x_k\} : I^k`$) 

* **Interface specialization notation**:  If $`A : \mathbf{Kind}(J)`$, $`B : \mathbf{Kind}(I)`$, $`A \lneq B`$ and $`J \lneq I`$, then we say:
  > The interface $`J`$ of $`A`$ **specializes** the interface $`I`$ of $`B`$

  We write this as $`A(J) \leq B(I)`$. 

* **Role cardinality notation**: $`|a|_I`$ counts elements in $`\{x_1,...,x_k\} :I^k`$

  _Example_: $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)`$. Then $`|m|_{\mathsf{Spouse}} = 2`$.

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

* **Dependency on list interface (of relation)**: For $`A : \mathbf{Rel}`$, we may have statements $`A : \mathbf{Rel}([I])`$. Thus, our type system has types of the form $`A(x:[I]) : \mathbf{Rel}`$.
    > $`A(x:[I])`$ is a relation type with dependency on a list $`x : [I]`$.

    _Example_: $`\mathsf{FlightPath} : \mathbf{Rel}([\mathsf{Flight}])`$

* **Dependent list type (of attributes)**: For any $`A : \mathbf{Att}(I)`$, we introduce $`[A] : \mathbf{List}(I_{[]})`$ where $`I_{[]}`$ is the **list version** of $`I`$. Thus, our type system has types of the form $`[A](x:I) : \mathbf{List}`$.
    > $`[A](x:I)`$ is a type of $`A`$-lists depending on interface $`I`$.

    _Example_: For $`[\mathsf{Name}] : \mathbf{List}(\mathsf{NameOwner})`$, we may have attribute lists $`[a,b,c] : [\mathsf{Name}](x : \mathsf{NameOwner})`$

* **List length notation**: for list $`l : [A]`$ the term $\mathrm{len}(l) : \mathbb{N}$ represents $`l`$'s length.

### Modalities

The following is purely for keeping track of certain information in the type system.

* **Abstract modality**. Certain statements $`\mathcal{J}`$ about types (namely: type kinds $`A : \mathbf{Kind}(...)`$ and subtyping $`A < B`$) have abstract versions written $`\diamond(\mathcal{J})`$.

    > If $`\diamond(\mathcal{J})`$ is true then this means $`\mathcal{J}`$ is _abstractly true_ in the type system, where "abstractly true" is a special truth-value which entails certain special behaviours in the database.

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

  _Remark_: This applies recursively to types with $`k`$ interfaces.

  _Example_: $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband})`$ and $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Wife})`$ then $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband},\mathsf{Wife})`$

* **Weakening dependencies**: Given $`A : \mathbf{Kind}(I,J)`$, this *implies* $`A : \mathbf{Kind}(I)`$. In words:
  > Dependencies can be simply ignored (note: this is a coarse rule — we later discuss more fine-grained constraints, e.g. cardinality).

  _Remark_: This applies recursively to types with $`k`$ interfaces.

  _Example_: $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse^2})`$ implies $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ and also $`\mathsf{Marriage} : \mathbf{Rel}`$ (we identify the empty brackets "$`()`$" with no brackets).

* **Auto-inheritance rule**: If $`A : \mathbf{Kind}`$, $`B : \mathbf{Kind}(I)`$, $`A \leq B`$ and $`A`$ has no interface strictly specializing $`I`$ then $`A : \mathbf{Kind}(I)`$ ("strictly" meaning "not equal to $`I`$"). In words:

  > Dependencies that are not specialized are inherited.
    
### Subtypes and castings

Beside the rules below, subtyping ($`\leq`$) is transitive and reflexive.

* **Subtyping rule**: If $`A \leq B`$ is true and $`a : A`$ is true, then this *implies* $`a : B`$ is true.

* **Explicit casting rule**: If $`f : A \to B`$ is true and $`a : A`$ is true, then this *implies* $`f(a) : B`$ is true.

* **Direct-to-general rule**: $`A <_! B`$ *implies* $`A \leq B`$.

* **"Weakening dependencies of terms" rule**: If $`a : A(x:I, y:J)`$ then this *implies* $`a : A(x:I)`$, equivalently: $`A(x:I, y:J) \leq A(x:I)`$. In other words:
    > Elements in $`A(I,J)`$ casts into elements of $`A(I)`$.

    * _Remark_: More generally, this applies for types with $k \leq 0$ interfaces. (In particular, $`A(x:I) \leq A() = A`$)
    * _Example_: If $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)`$ then both $`m : \mathsf{Marriage}(x:\mathsf{Spouse})`$ and $`m : \mathsf{Marriage}(y:\mathsf{Spouse})`$

* **"Covariance of dependencies" rule**: If $`A(J) \leq B(I)`$ (see "interface specialization" in "Grammar and notation" above) and $`a : A(x:I)`$ then this _implies_ $`a : B(x:J)`$. In other words:
    > When $`A`$ casts to $`B`$, and $`I`$ to $`J`$, then $`A(x : I)`$ casts to $`B(x : J)`$.

    _Remark_: This applies recursively for types with $`k`$ interfaces.

    _Example_: If $`m : \mathsf{HeteroMarriage}(x:\mathsf{Husband}, y:\mathsf{Wife})`$ then $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse}^2)`$

### Algebraic type operators

* **Sums**: Sum types follow the usual rules of type system.

    _Note_: the inclusion $`A \leq A + B`$ is also **subsumptive** subtyping. This induces certain equalities on elements: for example, if $`A \leq B`$, then $`A + B = B`$ since $`a : A`$ for $`a : B`$ we have $`a = a : A + B`$ ... therefore, technically $`A + B`$ the so-called _fibered_ sum which identifies the $`A \cap B`$). We omit the detailed rules here, and use common sense.

* **Products**: Product types follow the usual rules of type systems.

    _Note_: again, subsumptive subtyping interacts with this. For example, we would have if $`A \leq A'$ and $B \leq B'`$ then $`A \times B \leq A' \times B'`$ ...  We omit the detailed rules here, and use common sense.

* **Options**: Option types follow the usual rules of type systems (effectively, options are sum types: $`A? = A + \{ \emptyset \}`$).

    _Note_: same note as before applies. Also note $`A?? = A?`$ due to subsumptive subtyping.

### List types

* **Direct typing list rule**: Given $`l = [l_0,l_1,...] :_! [A](x: I_{[]})`$ this implies $`l_i :_! A(x: I)`$. In other words:
  > If the user intends a list typing $`l :_! [A]`$ then the list entries $`l_i`$ will be direct elements of $`A`$.

* **Direct dependency list rule**: Given $`l = [l_0,l_1,...] : [I]`$ and $`a :_! A(l : [I])`$ implies $`a :_! A(l_i : I)`$. In other words:
  > If the user intends $a$ to directly depend on the list $`l`$ then they intend $a$ to directly depend on each list's entries $`l_i`$.

### Modalities

There are no specific rules relating to modalities.
They are just a tool for keeping track of additional properties.

_Remark_: Recall from an earlier remark, **key**, **subkey**, **unique** could also be modalities, but for simplicity (and to reduce the amount of symbols in our grammar), we'll leave them be and only keep track of them in pure TypeQL.


# Data definition language

This section describes valid declarations of _types_ and axioms relating types (_dependencies_ and _type castings_) for the user's data model, as well as _schema constraints_ that can be further imposed. These declarations are subject to a set of _type system properties_ as listed in this section. The section also describes how such declarations can be manipulated after being first declared (undefine, redefine).

## Basics of schemas

### (Theory) Clauses and execution

* Kinds of definition clauses:
  * `define`: adds **schema type axioms** or **schema constraints**
  * `undefine`: removes axioms or constraints
  * `redefine`: both removes and adds axioms or constraints
* Loose categories for the main schema components:
  * **Type axioms**: comprises user-defined axioms for the type system (types, subtypes, dependencies).
  * **Constraints**: postulated constraints that the database needs to satisfy.
  * **Triggers**: actions to be executed based on database changes (_Note_: to be renamed)
  * **Value types**: types for primitive and structured values.
  * **Functions**: parametrized query templates ("pre-defined logic")
* Execution:
  * Each statement in a single clause is executed as described in this section (this, e.g., adds or removes statements from the type system)
  * By the end of the clause (up to automatic clean-up operation) the type system must be in a valid state, otherwise the clause is rejected

### (Feature) Pipelines and match-define's

* 🔮 Definition clauses can be pipelined:
  * _Example_: 
    ```
    define A; 
    define B; 
    undefine C; 
    redefine E;
    ```
* 🔶 Definition clauses can be preceded by a match clause
  * _Example_:
    ```
    match P; 
    define A;
    ```
  * _Interpretation_: `A` may contain non-optional **tvar**s bound in `P`; execute define for each results of match.

## Define semantics

`define` clauses comprise _define statements_ which are described in this section.

_Principles._

1. `define` **can be a no-op**: defining the same statement twice is a no-op.

### Type axioms

#### **Case ENT_DEF**
* ✅ `entity A` adds $`A : \mathbf{Ent}`$
* ✅ `(entity) A sub B` adds $`A : \mathbf{Ent}, A <_! B`$

_System property_: 

1. _Single inheritance_: Cannot have $`A <_! B`$ and $`A <_! C \neq B`$

#### **Case REL_DEF**
* ✅ `relation A` adds $`A : \mathbf{Rel}`$
* ✅ `(relation) A sub B` adds $`A : \mathbf{Rel}, A <_! B`$ where $`B : \mathbf{Rel}`$ 
* ✅ `(relation) A relates I` adds $`A : \mathbf{Rel}(I)`$ and $`I : \mathbf{Itf}`$.
* ✅ `(relation) A relates I as J` adds $`A : \mathbf{Rel}(I)`$, $`I <_! J`$ where $`B : \mathbf{Rel}(J)`$ and $`A <_! B`$
* 🔶 `(relation) A relates I[]` adds $`A : \mathbf{Rel}([I])`$
* 🔶 `(relation) A relates I[] as J[]` adds $`A : \mathbf{Rel}([I])`$, $`I <_! J`$ where $`B : \mathbf{Rel}([J])`$ and $`A <_! B`$

_System property_: 

1. ✅ _Single inheritance_: 
    * _for relation types_: Cannot have $`A <_! B`$ and $A <_! C \neq B$
    * _for interfaces_: Cannot have $`I <_! J`$ and $I <_! K \neq J$ for $`I,J,K :\mathbf{Itf}`$
1. ✅ _No role re-declaractions_: Cannot redeclare inherited interface (i.e. when `B relates I`, `A sub B` we cannot re-declare `A relates I`... this is automatically inherited!)
1. 🔶 _Automatic abstractions_:
    * _Un-specialization_: when `A relates I as J` then automatically `A relates J @abstract` (see **REL_ABSTRACT_DEF** for mathematical meaning of the latter)
    * _Un-specialization (list case)_: when `A relates I[] as J[]` then automatically `A relates J[] @abstract`
    * _Un-ordering_: when `A relates I[]` then automatically `A relates I @abstract`
1. 🔶 _Exclusive interface modes_: Disallow `$A relates $I` and `$A relates $I[]` to be true **non-abstractly** at the same time.  (we use variable to include not just direct declaration but also inferred validity, see "Pattern semantics").

#### **Case ATT_DEF**
* ✅ `attribute A` adds
    * $`A : \mathbf{Att}(O_A)`$, $`O_A : \mathbf{Itf}`$
    * $`[A] : \mathbf{List}(O_{A[]})`$, $`O_{A[]} : \mathbf{Itf}`$
* ✅ `(attribute) A value V` adds $`\mathsf{val} : A \to V`$, where $`V`$ is a primitive or a user-defined struct value type
* ✅ `(attribute) A sub B` adds
    * $`A : \mathbf{Att}(O_A)`$, $`A <_! B`$ and $`O_A <_! O_B`$ where $`B : \mathbf{Att}(O_B)`$
    * $`[A] : \mathbf{List}(O_{[A]})`$, $`[A] <_! [B]`$ and $`O_{A[]} <_! O_{B[]}`$ where $`B : \mathbf{Att}(O_B)`$

_Remark_ Here $`O_X`$ is an automatically generated interface in our type system (and $`O_{X[]}`$ is its list version, see "Type System").

_System property_: 

1. ✅ _Single inheritance_: Cannot have $`A <_! B`$ and $`A <_! C \neq B`$ for $`A, B, C : \mathbf{Att}`$.
1. 🔶 _Downward consistent value types_: When $`A <_! B`$, $`B <_! W`$ then must have $`\mathsf{val} : A \to V = W`$ for value type $`V`$. (**Note**: could in theory weaken this to  $`\mathsf{val} : A \to V \leq W`$)


#### **Case PLAYS_DEF**

* ✅ `A plays B:I` adds $`A <_! I`$ where $`B: \mathbf{Rel}(I)`$, $`A :\mathbf{Obj}`$.

_System property_: 

1. ✅ _Dissallow inherited roles_: Cannot have that $B \lneq B'$ with $`B': \mathbf{Rel}(I)`$ (otherwise fail).

_Remark_. The property ensures that we can only declare `A plays B:I` if `I` is a role directly declared for `B`, and not an inherited role.

#### **Case OWNS_DEF**
* ✅ `A owns B` adds $`A <_! O_B`$ where $`B: \mathbf{Att}(O_B)`$, $`A :\mathbf{Obj}`$
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
* ✅ `A relates I @card(n..m)` postulates $n \leq k \leq m$ whenever $`a :_! A'(\{...\} : I^k)`$, $`A' \leq A`$, $`A' : \mathbf{Rel}(I)`$.
  * **defaults** to `@card(1..1)` if omitted ("one")
* 🔶 `A plays B:I @card(n..m)` postulates $n \leq |B(a:I)| \leq m$ for all $`a : A`$
  * **defaults** to `@card(0..)` if omitted ("many")
* ✅ `A owns B @card(n...m)` postulates $n \leq |B(a:I)| \leq m$ for all $`a : A`$
  * **defaults** to `@card(0..1)` if omitted ("one or null")

_System property_:

1. ✅ For inherited interfaces, we cannot redeclare cardinality (this is actually a consequence of "Implicit inheritance" above). 
2. ✅ When we have direct subinterfaces $`I_i <_! J`$, for $`i = 1,...,n`$, and each $`I_i`$ has `card(`$`n_i`$`..`$`m_i`$`)` while J has `card(`$`n`$`..`$`m`$`)` then we must have $`\sum_i n_i \leq m`$ and $`n \leq \sum_i m_i`$.
  
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

* In all cases, the purposse of abstractness is to ***contrain `insert` semantics.***
* In a commited schema, it is never possible that both $`\diamond(A : K)`$ and $`A : K`$ are both true the same time (and _neither implies the other_).

_System property_

* 🔶 _Abstractness prevails_. When both $`\diamond(A : K)`$ and $`A : K`$ are present after a define clause is completed, then we remove the latter statements from the type system. (_Note_. Abstractness can only be removed through ***undefining it***, not by "overwriting" it in another `define` statement ... define is always additive!).

##### **Case TYP_ABSTRACT_DEF**
* 🔷 `(kind) A @abstract` adds $`\diamond(A : \mathbf{Kind})`$ which affects `insert` semantics.

_System property_

1. 🔷 _Upwards closure_ If `(kind) A @abstract` and $`A \leq B`$ then `(kind) B (sub ...)`cannot be declared non-abstractly.

##### **Case REL_ABSTRACT_DEF**
* 🔶 `A relates I @abstract` adds $`\diamond(A : \mathbf{Rel}(I))`$ which affects `insert` semantics.
* 🔶 `A relates I as J @abstract` adds $`\diamond(A : \mathbf{Rel}(I))`$ which affects `insert` semantics.
* 🔶 `A relates I[] @abstract` adds $`\diamond(A : \mathbf{Rel}([I]))`$ which affects `insert` semantics.
* 🔶 `A relates I[] as J[] @abstract` adds $`\diamond(A : \mathbf{Rel}([I]))`$ which affects `insert` semantics.

_System property_

1. 🔶 _Abstract interface inheritance_. Abstract interfaces are inherited if not specialized just like non-abstract interfaces. In math: if $`\diamond(B : \mathbf{Rel}(J))`$ and $`A \lneq B`$ without specializing $I$ (i.e. $`\not \exists I. A(I) \leq B(J)`$) then the type system will infer $`\diamond(B : \mathbf{Rel}(J))`$.
1. 🔶 _Upwards closure_. When `A relates I @abstract` and $`I \lneq J`$ then `A` also relates `J` abstractly.

_Remark_: In addition to user declarations, let's also recall the **three cases** in which `relates @abstract` gets implicitly inferred by the type system:
* Un-specialization: if a relation type relates a specialized interface, _then_ it abstractly relates the unspecialized versions of the interface.
* Un-specialization for lists: if a relation type relates a specialized list interface, _then_ it abstractly relates the unspecialized versions of the list interface.
* Un-ordering: if a relation type relates a list interface, _then_ it abstractly relates the "un-ordered" (un-listed?) interface.


##### **Case PLAYS_ABSTRACT_DEF**
* 🔶 `A plays B:I @abstract` adds $`\diamond(A <_! I)`$ which affects `insert` semantics.

_System property_

1. 🔶 _Upwards closure_. If `A plays B:I @abstract` and $`B'(I) \leq B'(I')`$ then `A plays B':I'` cannot be declared non-abstractly.

##### **Case OWNS_ABSTRACT_DEF**
* 🔶 `A owns B @abstract` adds $`\diamond(A <_! O_B)`$ which affects `insert` semantics.
* 🔶 `A owns B[] @abstract` adds $`\diamond(A <_! O_{B[]})`$ which affects `insert` semantics.

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
* ✅ `bool`
  * Terms: `true`, `false`
* ✅ `long` — _Comment: still think this could be named more nicely_
  * Terms: 64bit integers
* ✅ `double` 
  * Terms: 64bit doubles
* ✅ `decimal` 
  * Terms: 64bit with fixed e19 resolution after decimal point
* ✅ `date`
  * See expression grammar for valid formats
* ✅ `datetime`
  * See expression grammar for valid formats
* ✅ `datetime_tz`
  * See expression grammar for valid formats
* ✅ `duration`
  * See expression grammar for valid formats
* ✅ `string`
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
    fun F (x: T, y: S) -> { A, B }:
      match <PATTERN>
      (<OPERATORS>)
      return { z, w };
    ```
    adds the following to our type system:
    * _Function symbol_: $`F : \mathbf{Type}(T,S)`$.
    * _Function type_: when $`x : T`$ and $`y: S`$ then $`F(x:T, y:S) : \mathbf{Type}`$
    * _Output cast_: $`F(x:T, y:S) \leq A \times B`$
    * _Function terms_: $`(z,w) : F(x:T, y:S)`$ are discussed in section "Function semantics"
    * **Generalizes** to $`n`$ inputs and $`m`$ outputs

#### **Case SINGLE_RET_FUN_DEF**
* 🔷 _single-return function_ definition takes the form: 
    ```
    fun f (x: T, y: S) -> A, B:
      match <PATTERN>
      (<OPERATORS>)
      return <AGG>, <AGG>;
    ```
    adds the following to our type system:
    * _Function symbol_: when $`x : T`$ and $`y: S`$ then $`f(x:T, y:S) : A \times B`$
    * _Function terms_: $`(z,w) : f(x:T, y:S)`$ are discussed in section "Function semantics"
    * **Generalizes** to $`n`$ inputs and $`m`$ outputs

_Comment: notice difference in capitalization between the two cases!_

## Undefine semantics

`undefine` clauses comprise _undefine statements_ which are described in this section.

_Principles._

* ✅ `undefine` removes axiom, constraints, triggers, value types, or functions
* ✅ `undefine` **can be a no-op**

### Type axioms

#### **Case ENT_UNDEF**
* ✅ `entity A` removes $`A : \mathbf{Ent}`$
* ✅ `sub B from (entity) A` removes $`A \leq B`$

#### **Case REL_UNDEF**
* ✅ `relation A` removes $`A : \mathbf{Rel}`$
* ✅ `sub B from (relation) A` removes $`A \leq B`$
* ✅ `relates I from (relation) A` removes $`A : \mathbf{Rel}(I)`$
* ✅ `as J from (relation) A relates I` removes $`I <_! J`$ 
* 🔶 `relates I[] from (relation) A` removes $`A : \mathbf{Rel}([I])$
* 🔶 `as J[] from (relation) A relates I[]` removes $`I <_! J`$

#### **Case ATT_UNDEF**
* ✅ `attribute A` removes $`A : \mathbf{Att}`$ and $`A : \mathbf{Att}(O_A)`$
* ✅ `value V from (attribute) A value V` removes $`\mathsf{val} : A \to V`$
* ✅ `sub B from (attribute) A` removes $`A <_! B`$ and $`O_A <_! O_B`$

#### **Case PLAYS_UNDEF**
* ✅ `plays B:I from (kind) A` removes $`A <_! I`$ 

#### **Case OWNS_UNDEF**
* ✅ `owns B from (kind) A` removes $`A <_! O_B`$ 
* 🔶 `owns B[] from (kind) A` removes $`A <_! O_{B[]}`$

### Constraints

_In each case, `undefine` removes the postulated condition (restoring the default)._ (minor exception: subkey)

#### Cardinality

##### **Case CARD_UNDEF**
* ✅ `@card(n..m) from A relates I`
* 🔶 `@card(n..m) from A plays B:I`
* ✅ `@card(n...m) from A owns B`

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
  * _Example 1_: a type can either exists or not. we cannot "redefine" it's existence, but only define or undefine it.
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
* ✅ `(entity) A sub B` redefines $`A \leq B`$

#### **Case REL_REDEF**
* **cannot** redefine `relation A` 
* ✅ `(relation) A sub B` redefines $`A \leq B`$, ***requiring*** 
  * either $`A <_! B' \neq B`$ (to be redefined)
  * or $`A`$ has no direct super-type
* ✅ `(relation) A relates I` redefines $`A : \mathbf{Rel}(I)`$, ***requiring*** that $`A : \mathbf{Rel}([I])`$ (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(0..)`) 
  * _Data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I^k`$
* ✅ `(relation) A relates I as J` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role
* 🔶 `(relation) A relates I[]` redefines $`A : \mathbf{Rel}([I])`$, ***requiring*** that $`A : \mathbf{Rel}(I)`$ (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(1..1)`) (STICKY)
  * _Data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I^k`$
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
* ✅ `A relates I @card(n..m)`
* 🔶  `A plays B:I @card(n..m)`
* ✅ `A owns B @card(n...m)`

##### **Case CARD_LIST_REDEF**
* ✅ `A relates I[] @card(n..m)`
* ✅ `A owns B[] @card(n...m)`


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

* 🔶 `redefine struct A ...` replaces the previous definition of `A` with a new on. 

### Functions defs

#### **Case STREAM_RET_FUN_REDEF**

* 🔶 `redefine fun F ...` replaces the previous definition of `F` with a new on. 

#### **Case SINGLE_RET_FUN_REDEF**

* 🔶 `redefine fun f ...` replaces the previous definition of `f` with a new on. 

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

# Pattern matching language

This section first describes the pattern matching language of TypeDB, which relies on _variables_.

## Basics: Patterns, variables, concept rows, satisfaction

### (Theory) Statements, patterns

* **statements:** syntactic units of TypeQL (see Glossary)
* **patterns:** collection of statements, combined with logical connectives:
  * `PATT1; PATT2` match "`PATT1` **and** `PATT2`" (could also terminate a pattern `PATT;`, in which case read as "and true") 
  * `PATT1 or PATT2` "**either** match `PATT1` or `PATT2`", (extends to $k$ patterns)
  * `not { PATT }` "ensure `PATT` has **no match**", 
  * `try { PATT }` "**optionally** match `PATT` if possible"
  * what's inside `{ ... }` is called a **block**

### (Theory) Variables

Variables appear in statements. They fall in different categories, which can be recognized as follows.

* _Syntax_: vars start with `$`
  * _Examples_: `$x`, `$y`, `$person`

* _Var categories_: In a valid pattern, can always determine wether variables are
  * _Type variables_ (**tvar**, uppercase convention in this spec)
    * Any variable used in a type position in a statement
  * _Value variables_ (**vvar**, lowercase convention in this spec)
    * Any variable which are typed with non-comparable attribute types is a value variables (**#BDD**)
    * Any variable assigned to the output of an non-list expression 
    * Any variable derived from the output of a function (with value output type) is a value variable
  * _List variables_ (**lvar**, lowercase convention in this spec)
    * Any variable typed with a list type
    * Any variable assigned to a list expression.
  * _Instance variables_ (**ivar**, lowercase convention in this spec)
    * Any remaining variable must be an instance var.

... the last three together comprise _element vars_ (**evars**). Evars are those variables that can be in the signature and return statement of a function.

* _Anon vars_: anon vars start with `$_`. They behave like normal variables, but leave the variables name implicit and are automatically discarded (see "Deselect") at the end of the pattern.
  * _Remark_: Anon vars can be both **tvar**s and **evar**s

_Remark 1_. The code variable `$x` will be written as $`x`$ in math notation (without $`\$`$).

### (Theory) Typed concept rows

* _Concepts_. A **concept** is a type or an element in a type.
* _Typed concept rows_. An **typed concept row** (abbreviated 'crow') is a mapping of named variables (the **column names**) to _unapplied_ typed concepts (the **row entries**). A crow `m` may be written as:
  ```
  m = ($x->a:T, $y->b:S, ...)
  ```
  In math. notation $m$ can be written as: $`(x \mapsto a:T, y \mapsto b:S, ...)`$).

  To emphasize: By definition, types in crows are **unapplied** (see "Applying dependencies") 
    > In other words, the definition dissallows using types with applied dependencies like `$x -> a : T($y : I)`. Instead, we only allow `$x -> a:T` for bare symbols "T". (This is because we don't expose dependencies as such to the user)

  * _Assigned concepts_. Write `m($x)` (math. notation $`m(x)`$) for the concept that `m` assigns to `$x`.
  * _Assigned types_. Write `T($x)` (math. notation $`T_m(x)`$) for the type that `m` assigns to `$x`.
    * _Special case: assigned kinds_. Note that `T($x)` may be `Ent`, `Rel`, `Att`, `Itf` (`Rol`), or `Val` (for value types) when `$x` is assigned a type as a concept — we speak of `T($x)` as the **type kind** of `m($x)` in this case.

### (Feature) Pattern satisfication, typing conditions, answer

* ✅ _Satisfaction_. A crow `m` **satisfies** a pattern `P` if
  1. Its typing assignemt satisfies the typing condition below
  1. Its concept assignment satisfis the "pattern semantics" described in the next section.
 
  > Intuitively, this means substituting the variables in `P` with the concepts assigned in `m` yields statements that are true in our type system. 
  
  Here are the **typing conditions**:
    * ✅ For tvars `$X` in `P`, `T($X)` is a type kind (`entity`, `attribute`, `relation`, `interface`, `value`)
    * ✅ For vvars `$x` in `P`, `T($x)` is a value type (primitive or struct)
    * 🔶 For lvars `$x` in `P`, `T($x)` is a list type `A[]` for ***minimal*** `A` a type such that `A` is the minimal upper bounds of the types of the list elements `<EL>` in the list `m($x) = [<EL>, <EL>, ...]` (note: our type system does have minimal upper bounds thanks to sums)
    * ✅ For ivars `$x` in `P`, `T($x)` is a schema type `A` such that $`m(x) :_! A`$ isa **direct typing**

<!-- Examples for the typing algorithm:
fun a($x: person) -> name[]:
match
  $x has firstname $f;
  $x has lastname $l;
  $namelist = [$f, $l]; // type as `[$f] + [$l]`
return $namelist;

match
  $x has color $y;
  $x has name $y; // "violet"
  $y isa color;

// 2x2 matrix:
// no:
$y isa name;
$y isa color;
// no:
$x has name $y;
$y isa color;
// no:
$y isa name;
$x has color $y;
// yes:
$x has name $y;
$x has color $y;
// STICKY: are we happy with this?
-->

* ✅ _Answers_. A crow `m` that satisfies a pattern `P` is an **answer** to the pattern if:
  * **The row is minimal** in that no concept row with less variables satisfies `P`
  * All variables in `m` are **bound outside a negation** in `P`

_Example_: Consider the pattern `$x isa Person;` (this pattern comprises a single statement). Than `($x -> p)` satisfies the pattern if `p` is an element of the type `Person` (i.e. $p : \mathsf{Person}$). The answer `($x -> p, $y -> p)` also satisfies the pattern, but it is not proper minimal.

### (Feature) Optionality and boundedness

**Optional variables** (**#BDD**)

_Key principle_:

* 🔷 If variables are used only in specific positions (called **optional positions**) of patterns, then they are optional variables.
  * if a var is used in _any_ non-optional position, then the var become non-optional!
* ✅  A optional variable `$x` is allowed to have the empty concept assigned to it in an answer: $`m(x) = \emptyset`$.

**Variable boundedness condition** (**#BDD**)

_Key principle_:

* 🔷 A pattern `P` will only be accepted by TypeDB if all variables are **bound**. 
  * (Fun fact: otherwise, we may encounter unbounded/literally impossible computations of answers.)
* 🔷 A variable is bound if it appears in a _binding position_ of at least one statement. 
  * Most statements bind their variables: in the next section we highlight _non-bound positions_

## Pattern semantics

Given a crow `m` and pattern `P` we say `m` ***satisfies*** `P` if (in addition to the typing conditions in outlined in "Pattern satisfication" above) the following conditions are met.

_Remark (Replacing **var**s with concepts)_. When discussing pattern semantics, we always consider **fully variablized** statements (e.g. `$x isa $X`, `$X sub $Y`). This also determines satisfaction of **partially assigned** versions of these statements (e.g. `$x isa A`, `$X sub A`, `A sub $Y`, or `x isa $A`).


## Type patterns

### **Case TYPE_DEF_PATT**
* ✅ `Kind $A` (for `Kind` in `{entity, relation, attribute}`) is satisfied if $`m(A) : \mathbf{Kind}`$

* ✅ `(Kind) $A sub $B` is satisfied if $`m(A) : \mathbf{Kind}`$, $`m(B) : \mathbf{Kind}`$, $`m(A) \lneq m(B)`$
* ✅  `(Kind) $A sub! $B` is satisfied if $`m(A) : \mathbf{Kind}`$, $`m(B) : \mathbf{Kind}`$, $`m(A) <_! m(B)`$

_Remark_: `sub!` is convenient, but could actually be expressed with `sub`, `not`, and `is`. Similar remarks apply to **all** other `!`-variations of TypeQL key words below.

### **Case REL_PATT**
* ✅ `$A relates $I` is satisfied 
    * either if $`m(A) : \mathbf{Rel}(m(I))`$
    * or if $`\diamond(m(A) : \mathbf{Rel}(m(I)))`$

* 🔮 `$A relates! $I` is satisfied if 
    * $`m(A) : \mathbf{Rel}(m(I))`$ and **not** $`m(A) \lneq B : \mathbf{Rel}(m(I))`$
    * _(to match `@abstract` for relates! must use annotation, see **REL_ABSTRACT_PATT**.)_

* ✅ `$A relates $I as $J` is satisfied if 
    * $`m(A) : \mathbf{Rel}(m(I))`$, $`B : \mathbf{Rel}(m(J))`$, $`A \leq B`$, $`m(I) \leq m(J)`$.
    * either the first or the second statement (or both) can be abstract $`\diamond(...)`$.

* 🔶 `$A relates $I[]` is satisfied if $`m(A) : \mathbf{Rel}(m([I]))`$ and
    * either if $`m(A) : \mathbf{Rel}([m(I)])`$
    * or if $`\diamond(m(A) : \mathbf{Rel}([m(I)]))`$

* 🔮 `$A relates! $I[]` is satisfied if 
    * $`m(A) : \mathbf{Rel}(m([I]))`$ and **not** $`m(A) \lneq m(B) : \mathbf{Rel}(m([I]))`$
    * _(to match `@abstract` for relates! must use annotation, see **REL_ABSTRACT_PATT**.)_

* 🔶 `$A relates $I[] as $J[]` is satisfied if 
    * $`m(A) : \mathbf{Rel}(m([I]))`$, $`B : \mathbf{Rel}(m([J]))`$, $`A \leq B`$, $`m(I) \leq m(J)`$.
    * either the first or the second statement (or both) can be abstract $`\diamond(...)`$.


### **Case PLAY_PATT**
* ✅ `$A plays $I` is satisfied if
    * either $`m(A) \leq A'`$ and $`A' <_! m(I)`$
    * or if $`m(A) \leq A'`$ and $`\diamond(A' <_! m(I))`$

* 🔮 `$A plays! $I` is satisfied if $`m(A) <_! m(I)`$
    * $`A <_! m(I)`$
    * _(to match `@abstract` for `plays!` must use annotation, see **PLAYS_ABSTRACT_PATT**)_

### **Case OWNS_PATT**
* ✅ `$A owns $B` is satisfied if 
    * either $`m(A) \leq A'`$ and $`A' <_! m(O_B)`$ 
    * or $`m(A) \leq A'`$ and $`\diamond(A' <_! m(O_B))`$

* 🔮 `$A owns! $B` is satisfied if 
    * $`m(A) <_! m(O_B)`$ 
    *  _(to match `@abstract` for `owns!` must use annotation, see **OWNS_ABSTRACT_PATT**)_

* 🔶 `$A owns $B[]` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type)
    * either $`m(A) \leq A'`$ and $`A' <_! m(O_{B[]})`$ 
    * or $`m(A) \leq A'`$ and $`\diamond(A' <_! m(O_{B[]}))`$

* 🔮 `$A owns! $B[]` is satisfied if $`m(A) <_! m(O_B)`$
    * $`m(A) <_! m(O_B)`$ 
    *  _(to match `@abstract` for `owns!` must use annotation, see **OWNS_ABSTRACT_PATT**)_ 

_Remark_. In particular, if `A owns B[]` has been declared, then `$X owns B` will match the answer `m($X) = A`.

### **Cases TYP_IS_PATT and LABEL_PATT**
* 🔷 `$A is $B` is satisfied if $`m(A) = m(B)`$ (this is actually covered by the later case `IS_PATT`)
* 🔷 `$A label <LABEL>` is satisfied if $`m(A)`$ has **primary label or alias** `<LABEL>`

## Type constraint patterns

### Cardinality

_To discuss: the usefulness of constraint patterns seems overall low, could think of a different way to retrieve full schema or at least annotations (this would be more useful than, say,having to find cardinalities by "trialing and erroring" through matching)._

#### **Case CARD_PATT**
* ✅ cannot match `@card(n..m)` (STICKY: there's just not much point to do so ... rather have normalized schema dump. discuss `@card($n..$m)`??)
<!-- 
* `A relates I @card(n..m)` is satisfied if $`m(A) : \mathbf{Rel}(m(I))`$ and schema allows $`|a|_I`$ to be any number in range `n..m`.
* `A plays B:I @card(n..m)` is satisfied if ...
* `A owns B @card(n...m)` is satisfied if ...
* `$A relates $I[] @card(n..m)` is satisfied if ...
* `$A owns $B[] @card(n...m)` is satisfied if ...
-->

### Bevavior flags

#### **Case UNIQUE_PATT**
* 🔶 `$A owns $B @unique` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B) @key`.

* 🔶 `$A owns! $B @unique` is satisfied if $`m(A) <_! m(O_B)`$, and schema directly contains constraint `m($A) owns m($B) @unique`.

#### **Case KEY_PATT**
* 🔶 `$A owns $B @key` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B) @key`.

* 🔶 `$A owns! $B @key` is satisfied if $`m(A) <_! m(O_B)`$, and schema directly contains constraint `m($A) owns m($B) @key`.

#### **Case SUBKEY_PATT**
* 🔶 `$A owns $B @subkey(<LABEL>)` is satisfied if $`m(A) \leq A' <_! m(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns m($B) @subkey(<LABEL>)`.

#### **Case TYP_ABSTRACT_PATT**
* 🔶 `(kind) $B @abstract` is satisfied if schema directly contains `(kind) m($B) @abstract`.

#### **Case RELATES_ABSTRACT_PATT**
* 🔶 `$B relates $I @abstract` is satisfied if:
    * $`m(B) \leq B'`$ and $`\diamond(B' : \mathbf{Rel}(I)`$
    * **not** $`m(B) \leq B'' \leq B'`$ such that $`\diamond(B' : \mathbf{Rel}(I)`$

* 🔮 `$B relates! $I @abstract` is satisfied if $`\diamond(m(B) : \mathbf{Rel}(I)`$

* 🔶 `$B relates $I[] @abstract` is satisfied if:
    * $`m(B) \leq B'`$ and $`\diamond(m(B) : \mathbf{Rel}([I])`$
    * **not** $`m(B) \leq B'' \leq B'`$ such that $`B' : \mathbf{Rel}([I])`$

* 🔮 `$B relates! $I[] @abstract` is satisfied if $`\diamond(m(B) : \mathbf{Rel}([I])`$

#### **Case PLAYS_ABSTRACT_PATT**
* 🔶 `$A plays $B:$I @abstract` is satisfied if: 
    * $`m(A) \leq A'`$ and $`\diamond(A' <_! m(I))` 
    * **not** $`m(A) \leq A'' \leq A'`$ such that $`A'' <_! m(I)`$ 

  where $`m(B) \mathbf{Rel}(m(I))`$

* 🔮 `$A plays! $B:$I @abstract` is satisfied if $`\diamond(m(A) <_! m(I))`, where $`m(B) \mathbf{Rel}(m(I))`$

#### **Case OWNS_ABSTRACT_PATT**
* 🔶 `$A owns $B @abstract` is satisfied if
    * $`m(A) \leq A'`$ and $`\diamond(A' <_! O_{m(B)})`$
    * **not** $`m(A) \leq A'' \leq A'`$ such that $`A'' <_!  O_{m(B)})`$

* 🔮 `$A owns! $B @abstract` is satisfied if $`\diamond(m(A) <_! O_{m(B)})`

* 🔶 `$A owns $B[] @abstract` is satisfied if
    * $`m(A) \leq A'`$ and $`\diamond(A' <_! O_{m(B)[]})`$
    * **not** $`m(A) \leq A'' \leq A'`$ such that $`A'' <_!  O_{m(B)[]})`$

* 🔮 `$A owns! $B[] @abstract` is satisfied if $`\diamond(m(A) <_! O_{m(B)[]})`

#### **Case DISTINCT_PATT**
* 🔮 `$A owns $B[] @distinct` is satisfied if $`m(A) \leq A`$ schema directly contains constraint `A' owns m($B)[] @distinct`.
* 🔮 `$A owns! $B[] @distinct` is satisfied if schema directly contains constraint `m($A) owns m($B)[] @distinct`.
* 🔮 `$B relates $I[] @distinct` is satisfied if $`m(B) : \mathbf{Rel}(m([I]))`$, $`B \leq B'`$ and schema directly contains `B' relates m($I)[] @distinct`.
* 🔮 `$B relates! $I[] @distinct` is satisfied if schema directly contains `m($B) relates m($I)[] @distinct`.

### Values

#### **Cases VALUE_VALUES_PATT and OWNS_VALUES_PATT**
* cannot match `@values/@regex/@range` (STICKY: there's just not much point to do so ... rather have normalized schema dump)
<!--
* `A owns B @values(v1, v2)` is satisfied if 
* `A owns B @regex(<EXPR>)` is satisfied if 
* `A owns B @range(v1..v2)` is satisfied if 
* `A value B @values(v1, v2)` is satisfied if 
* `A value B @regex(<EXPR>)` is satisfied if 
* `A value B @range(v1..v2)` is satisfied if 
-->

## Element patterns

#### **Case ISA_PATT**
* ✅ `$x isa $T` is satisfied if $`m(x) : m(T)`$ for $`m(T) : \mathbf{ERA}`$
* ✅ `$x isa! $T` is satisfied if $`m(x) :_! m(T)`$ for $`m(T) : \mathbf{ERA}`$

#### **Case LINKS_PATT**
* ✅ `$x links ($I: $y)` is satisfied if $`m(x) : A(m(y):m(I))`$ for some $`A : \mathbf{Rel}(m(I))`$.
* 🔮 `$x links! ($I: $y)` is satisfied if $`m(x) :_! A(m(y):m(I))`$ for some $`A : \mathbf{Rel}(m(I))`$.
* 🔶 `$x links ($I[]: $y)` is satisfied if $`m(x) : A(m(y):[m(I)])`$ for some $`A : \mathbf{Rel}([m(I)])`$.
* 🔮 `$x links! ($I[]: $y)` is satisfied if $`m(x) :_! A(m(y):[m(I)])`$ for some $`A : \mathbf{Rel}([m(I)])`$.
* ✅ `$x links ($y)` is equivalent to `$x links ($_: $y)` for anonymous `$_` (See "Syntactic Sugar")

#### **Case HAS_PATT**
* ✅ `$x has $B $y` is satisfied if $`m(y) : m(B)(m(x):O_{m(B)})`$ for some $`m(B) : \mathbf{Att}`$.
* 🔮 `$x has! $B $y` is satisfied if $`m(y) :_! m(B)(m(x):O_{m(B)})`$ for some $`m(B) : \mathbf{Att}`$.
* 🔶 `$x has $B[] $y` is satisfied if $`m(y) : [m(B)](m(x):O_{m(B[])})`$ for some $`m(B) : \mathbf{Att}`$.
* 🔮 `$x has! $B[] $y` is satisfied if $`m(y) :_! [m(B)](m(x):O_{m(B[])})`$ for some $`m(B) : \mathbf{Att}`$.
* ✅ `$x has $y` is equivalent to `$x has $_ $y` for anonymous `$_`

_Remark_. Note that `$x has $B $y` will match the individual list elements of list attributes (e.g. when $`m(x) : A`$ and $`A <_! O_B`$).

#### **Case IS_PATT**
* 🔷 `$x is $y` is satisfied if $`m(x) :_! A`$, $`m(y) :_! A`$, $`m(x) = m(y)`$, for $`A : \mathbf{ERA}`$ (XXX: add ERA lists here) (**#BDD**)
* 🔷 `$A is $B` is satisfied if $`A = B`$ for $`A : \mathbf{ERA}`$, $`B : \mathbf{ERA}`$

_System property_

1. 🔷 In the `is` pattern, left or right variables are **not bound**.

_Remark_: In the `is` pattern we cannot syntactically distinguish whether we are in the "type" or "element" case (it's the only such pattern where tvars and evars can be in the same position!) but this is alleviated by the pattern being non-binding, i.e. we require further statements which bind these variables, which then determines them to be tvars are evars.

## Expression and list patterns

### Grammar

```javascript
BOOL       ::= VAR | bool                                     // VAR = variable
INT        ::= VAR | long | ( INT ) | INT (+|-|*|/|%) INT 
               | (ceil|floor|round)( DBL ) | abs( INT ) | len( T_LIST )
               | (max|min) ( INT ,..., INT )
DBL        ::= VAR | double | ( DBL ) | DBL (+|-|*|/) DBL 
               | (max|min) ( DBL ,..., DBL ) |                
DEC        ::= ...                                            // similar to DBL
STRING     ::= VAR | string | string + string
DUR        ::= VAR | time | DUR (+|-) DUR 
DATE       ::= VAR | datetime | DATETIME (+|-) DUR 
DATETIME   ::= VAR | datetime | DATETIME (+|-) DUR 
PRIM       ::= <any-expr-above>
STRUCT     ::= VAR | { <COMP>: (value|VAR|STRUCT), ... }      // <COMP> = struct component
               | <HINT> { <COMP>: (value|VAR|STRUCT), ... }   // <HINT> = struct label
DESTRUCT   ::= { T_COMP: (VAR|VAR?|DESTRUCT), ... }           
VAL        ::= PRIM | STRUCT | STRUCT.<COMP>                  
<T>        ::= <T> | <T>_LIST [ INT ] | <T>_FUN               // T : Schema (incl. Val)
<T>_LIST   ::= VAR | [ <T> ,..., <T> ] | <T>_LIST + <T>_LIST  // includes empty list []
               T_LIST [ INT .. INT ]
INT_LIST   ::= INT_LIST | [ INT .. INT ]
VAL_EXPR   ::= <T> | ( VAL_EXPR )                             // "value expression"
LIST_EXPR  ::= <T>_LIST | INT_LIST | ( LIST_EXPR )            // "list expression"
EXPR       ::=  VAL_EXPR | LIST_EXPR
```

***Selected details***

* Careful: the generic case `<T>` modify earlier parts of the 
* `T`-functions (`T_FUN`) are function calls to *single-return* functions with non-tupled output type `T` or `T?`
* Datetime and time formats
  ```
  long       ::=   (0..9)*
  double     ::=   (0..9)*\.(0..9)+
  date       ::=   ___Y__M__D
  datetime   ::=   ___Y__M__DT__h__m__s
                 | ___Y__M__DT__h__m__s:___
  datetimetz ::= ...
  duration   ::=   P___Y__M__D              
                 | P___Y__M__DT__h__m__s
                 | P___Y__M__DT__h__m__s:___
  ```

_Remark_. 🔮 Introduce explicit castings between types to our grammar. For example:
* `(int) 1.0 == 1` 
* `(double) 10 / (double) 3 == 3.3333`)

### (Theory) Typed evaluation of expressions

Given a concept map `m` that assign all vars in an `<EXPR>` we define
* value evaluation `vev@m(<EXPR>)` (math. notation $`v_m(expr)`$)
* type evaluation `Tev@m(<EXPR>)` (math. notation $`v_m(expr)`$)

#### Value expressions

* 🔶 The _value expresssions_ `VAL_EXPR` is evaluated as follows:
    * **Substitute** all vars `$x` by `m($x)`
    * If `m($x)` isa attribute instance, **replace** by `val(m($x))`
    * $`v_m(expr)`$ is the result of evaluating all operations with their **usual semantics** 
        * `1 + 1 == 2` 
        * `10 / 3 == 3` (integer division satisfies `p/q + p%q = p`)
    * $`T_m(expr)`$ is the type of the **substituted expression**, noting:
        * This is always unique except possibly for the `STRUCT` case (see property below)!

_System property_.

* 🔶 If $`T_m(expr)`$ is non-unique for a `STRUCT` expression (which may be the case because, `STRUCT` may share fields) we require the expression to have a `HINT`, or otherwise throw an error.

_Remark_. Struct values are semantically considered up to reordering their components.

#### List expressions

* 🔶 The _list expresssions_ `LIST_EXPR` is evaluated as follows:
    * Substitute all vars `$x` by `m($x)`
    * (**Do not replace** attributes!)
    * $`v_m(expr)`$ is the result of concatenation and sublist operations with their **usual semantics**
        * e.g. `[a] + [a,b,c][1..2] = [a,b,c]` (`[1..2]` includes indices `[1,2]`)
        * or `([a] + [a,b,c])[1..2] = [a,b]`
    * $`T_m(expr)`$ is the **minimal type** of all the list elements


### (Feature) Boundedness constraints

1. 🔶 Generally, variables in expressions `<EXPR>` are **never bound**, except ...
    * 🔶 the exception are **single-variable list indices**, i.e. `$list[$index]`; in this case `$index` is bound. (This makes sense, since `$list` must be bound elsewhere, and then `$index` is bound to range over the length of the list) (**#BDD**)
3. 🔶 Struct components are considered to be unordered: i.e., `{ x: $x, y: $y}` is equal to `{ y: $y, x: $x }`.

_Remark_: The exception for list indices is mainly for convenience. Indeed, you could always explicitly bind `$index` with the pattern `$index in [0..len($list)-1];`. See "Case **IN_LIST_PATT**" below.

_Remark_ In this specification, we assume all struct components to be **uniquely named** in the schema: as such, each component has a unique associated type. Without this constraint, weird value polymorphism may arise (but might be intended?).

### Simple expression patterns

#### **Case ASSIGN_PATT**
* 🔷 `$x = <EXPR>` is satisfied if **both** 
    * `m($x)` equals $`v_m(expr)`$
    * `T($x)` equals $`T_m(expr)`$

_System property_

1. _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Assign once, to vars only_. Any variable can be assigned only once within a pattern—importantly, the left hand side _must be_ a variable (replacing it with a concept will throw an error; this implicitly applies to "Match semantics").
3. _Acyclicity_. It must be possibly to determine answers of all variables in `<EXPR>` before answering `$x` — this avoids cyclic assignments (like `$x = $x + $y; $y = $y - $x;`)

_Remark_. TODO: consider `let $x = <EXPR>` as alternative syntax.

#### **Case DESTRUCT_PATT**
* 🔶 `<DESTRUCT> = <STRUCT>` is satisfied if, after substituting concepts from `m`, the left hand side (up to potentially omitting components whose variables are marked as optional) matched the structure of the right and side, and each variable on the left matches the evaluated expression of the correponding position on the right.

_System property_

1. 🔶 _Assignments bind_. The left-hand variable is bound by the pattern.
2. 🔶 _Acyclicity_. Applies as before (now applie to _all_ variables assigned on the LHS)
3. 🔶 _Type check_. We require that $

#### **Case EQ_PATT**
* ✅ `<EXPR1> == <EXPR2>` is satisfied if $`v_m(expr_1) = v_m(expr_2)`$
* ✅ `<EXPR1> != <EXPR2>` is equivalent to `not { <EXPR1> != <EXPR2> }` (see "Patterns")

_System property_

1. All variables are bound **not bound**.

#### **Case COMP_PATT**

The following are all kind of obvious (for `<COMP>` one of `<`,`<=`,`>`,`>=`):

* ✅ `<INT> <COMP> <INT>` 
* ✅ `<BOOl> <COMP> <BOOL>` (`false`<`true`)
* ✅ `<STRING> <COMP> <STRING>` (lexicographic comparison)
* ✅ `<DATETIME> <COMP> <DATETIME>` (usual datetime order)
* ✅ `<TIME> <COMP> <TIME>` (usual time order)
* ✅ `<STRING> contains <STRING>` 
* 🔷 `<STRING> like <REGEX>` (where `<REGEX>` is a regex string without variables)

_System property_

1. In all the above patterns all variables are **not bound**.


### List expression patterns

### **Case IN_LIST_PATT**
* 🔷 `$x in $l` is satisfied if $`m(x) \in m(l)`$
* 🔶 `$x in <LIST_EXPR>` is equivalent to `$l = <LIST_EXPR>; $x in $l` (see "Syntactic Sugar") 

_System property_

1. The right-hand side variable(s) of the pattern are **not bound**. (The left-hand side variable is bound.)

## Function patterns

### **Case IN_FUN_PATT**
* 🔶 `$x, $y?, ... in <FUN_CALL>` is satisfied, after substituting concepts, the left hand side is an element of the **function answer set** $`F`$ of evaluated `<FUN_CALL>` on the right (see "Function semantics") meaning that: for some tuple $t \in F$ we have
  * for the $`i`$th variable `$z`, which is non-optional, we have $`m(z) = t_i`$
  * for the $`i`$th variable `$z`, which is marked as optional using `?`, we have either
    * $`m(z) = t_i`$ and $`t_i \neq \emptyset`$
    * $`m(z) = t_i`$ and $`t_i = \emptyset`$

### **Case ASSIGN_FUN_PATT**
* 🔶 `$x, $y?, ... = <FUN_CALL>` is satisfied, after substituting concepts, the left hand side complies with the **function answer tuple** $`t`$ of `<FUN_CALL>` on the right (see "Function semantics") meaning that:
  * for the $`i`$th variable `$z`, which is non-optional, we have $`m(z) = t_i`$
  * for the $`i`$th variable `$z`, which is marked as optional using `?`, we have either
    * $`m(z) = t_i`$ and $`t_i \neq \emptyset`$
    * $`m(z) = t_i`$ and $`t_i = \emptyset`$

_Remark_: variables marked with `?` in function assignments are the first example of **optional variables**. We will meet other pattern yielding optional variables in the following section.


## Patterns of patterns

Now that we have seen how to determine when answers satisfy individual statements, we can extend our discussion of match semantics to composite patterns (patterns of patterns).

#### **Case AND_PATT**
* ✅ An answer satisfies the pattern `<PATT1>; <PATT2>;` that simultaneously satisfies both `<PATT1>` and `<PATT2>`.


#### **Case OR_PATT**
* 🔷 An answer for the pattern `{ <PATT1> } or { <PATT2> };` is an answer that satisfies either `<PATT1>` or `<PATT2>`.

_Remark_: this generalize to a chain of $`k`$ `or` clauses.

#### **Case NOT_PATT**
* 🔷 An answer satisfying the pattern `not { <PATT> };` is any answer which _cannot_ be completed to a answer satisfying `<PATT>`.

#### **Case TRY_PATT**
* 🔷 The pattern `try { <PATT> };` is equivalent to the pattern `{ <PATT> } or { not { <PATT>}; };`.


# Data manipulation language

## Match semantics

* _Syntax_ The `match` clause has syntax
    ```
    match <PATTERN>
    ```

* _Input crows_: The clause can take as input a stream `{ m }` of concept rows `m`.

* _Output crows_: For each `m`: 
  * replace all patterns in `P` with concepts from `m`. 
  * Compute the stream of answer `{ m' }`. 
  * The final output stream will be `{ (m,m') }`.


## Functions semantics

### Function signature, body, operators

#### **Case FUN_SIGN_STREAM**

* 🔶 Stream-return function signature syntax:
    _Syntax_:
    ```
    fun F ($x: A, $y: B[]) -> { C, D[], E? } :
    ```
    where
    * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_Remark: see GH issue on trais (Could have `A | B | plays C` input types)._

#### **Case FUN_SIGN_SINGLE**

* 🔶 Single-return function signature syntax:
    ```
    fun F ($x: A, $y: B[]) -> C, D[], E? :
    ```
    where
    * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_STICKY: allow types to be optional in args (this extends types to sum types, interface types, etc.)_

#### **Case FUN_BODY**

* 🔶 Function body syntax:
    _Syntax_:
    ```
    match <PATT>
    ```
    * `<PATT>;` can be any pattern as defined in the previous sections. 

#### **Case FUN_OPS**

* 🔶 Function operator syntax:
    _Syntax_:
    ```
    <OP>;
    ...
    <OP>;
    ```
    where:

    * `<OP>` can be one of:
      * `limit <int>`
      * `offset <int>`
      * `sort $x, $y` (sorts first in `$x`, then in `$y`)
      * `select $x, $y`
    * Each `<OP>` stage takes the concept row set from the previous stage and return a concept row set for the 
      * These concept row set operatioins are described in **"Operators"** below
    * The final output concept row set of the last operator is called the **body concept row set**

### Function body semantics

#### Function evaluation

* 🔶 In each function call, we ***evaluate the function***: 
    1. substitute input arguments into body pattern
    2. Then act like an ordinary pipeline, outputting a stream (see "Pipelines")
    3. perform `return` transformation outlined below for final output.

#### Order of execution

Since functions can only be called from `match` stages in pipelines, evaluation is deterministic and does not depend on any choices of execution order (i.e. which statements in the pattern we retrieve first), with the only exception begin **negation**. (see "recursion semantics": negation patterns can only be evaluated once all previous strata function calls have been fully evaluated).

* 🔮 **Recursion** Functions can be called recursively, as long as negation can be **stratified**:

    * The set of all defined functions is divided into groups called "strata" which are ordered
    * If a function `F` calls a function `G` if must be a in an equal or higher stratum. Moreover, if `G` appears behind an odd number of `not { ... }` in the body of `F`, then `F` must be in a strictly higher stratum.

    _Note_: The semantics in this case is computed "stratum by stratum" from lower strata to higher strata. New facts in our type systems ($`t : T`$) are derived in a bottom-up fashion for each stratum separately.

### Stream-return semantics

* 🔶 `return { $x, $y, ... }`
  * performs a `select` for the listed variables (See "Select") on the ***output stream of the body pipeline***)
  * returns resulting concept row set

### Single-return semantics

* 🔶 `return <AGG> , ... , <AGG>;` where `<AGG>` is an aggregate function.
    * For syntax and semantics, see Case **SIMPLE_RED**

* Each `<AGG>` reduces the concept row `{ m }` passed to it from the function's body to a single concept in the specified way.


## Insert semantics

### Basics of inserting

An `insert` clause comprises collection of _insert statements_

* _Input crow_: The clause can take as input a stream `{ m }` of concept rows `m`, in which case 
  * the clause is **executed** for each row `m` in the stream individually

* _Extending input row_: Insert clauses can extend bindings of the input concept row `m` in two ways
  * `$x` is the subject of an `isa` statement in the `insert` clause, in which case $`m(x) =`$ _newly-inserted-concept_ (see "Case **ISA_INS**")
  * `$x` is the subject of an `=` assignment statement in the `insert` clause, in which case $`m(x) =`$ _assigned-value_ (see "Case **ASSIGN_INS**")

#### (Theory) Execution

_Execution_: An `insert` clause is executed by executing its statements individually.
* Not all statement need to execute (see Optionality below)
  * **runnable** statements will be executed
  * **skipped** statements will not be executed
* The order of execution is arbitrary except for:
  1. We execute all runnable `=` assignments first.
  2. We then execute all runnable `isa` statements.
  3. Finally, we execute remaining runnable statements.
* Executions of statements will modify the database state by 
  * adding elements
  * refining dependencies
* (Execution can also affect the state of concept row `m` as mentioned above)
* Modification are buffered in transaction (see "Transactions")
* Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

#### (Feature) Optionality

* 🔶 **Optionality**: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `insert` clauses cannot be nested
  * `try` blocks variables are **block-level bound** if
    * they are bound outside the block
    * they are bound by an `isa` or `=` statement in the block
  * If any variable is not block-level bound, the `try` block statements are skipped.
  * If all variables are block-level bound, the `try` block statements are runnable.
  * All variables outside of a `try` block must be bound outside of that try block (in other words, variable in a block bound with `isa` cannot be used outside of the block)

### Insert statements


#### **Case ASSIGN_INS**
* 🔷 `$x = <EXPR>` adds nothing, and sets $`m(x) = v`$ where $`v`$ is the value that `<EXPR>` evaluates to.

_System property_:

1. 🔷 `$x` cannot be bound elsewhere.
2. 🔷 All variables in `<EXPR>` must be bound elsewhere (as before, we require acyclicity of assignement, see "Acyclicity").
3. 🔷 `<EXPR>` cannot contain function calls.

_Note_. All **EXPR_INS** statements are executed first as described in the previous section.

#### **Case OBJ_ISA_INS**
* ✅ `$x isa $T` adds new $`a :_! m(T)`$, $`m(T) : \mathbf{Obj}`$, and sets $`m(x) = a`$

_System property_:

1. ✅ `$x` cannot be bound elsewhere (i.e. `$x` cannot be bound in the input row `m` nor in other `isa` or `=` statements).

#### **Case ATT_ISA_INS**
* 🔮  `<VAL_EXPR> isa $T` adds new $`v :_! m(T)`$, $`m(T) : \mathbf{Att}`$, where `v` is the result of evaluating the expression `<EXPR>`

_System property_:

* 🔮 `<EXPR>` must be of the right value type, and be evaluatable (i.e. all vars are bound).
* 🔮 `m(T)` must be an independent attribute, i.e. the schema must contain `attribute m(T) (sub B) @indepedent`

#### **Case LINKS_INS** 
* ✅ `$x links ($I: $y)` replaces $`m(x) :_! A(a : J, b : K, ...)`$ by $`m(x) :_! A(m(y)a : m(I), b : K, ...)`$

_Remark_. Set semantics for interfaces means that inserts become idempotent when inserting the same role players twice.

_System property_:

1. ✅ _Capability check_. 
    * Must have $`T(x) \leq B : \mathbf{Rel}(m(I))`$ **non-abstractly**, i.e. $`\diamond (B : \mathbf{Rel}(m(I)))`$ is not true for the minimal choice of $`B`$ satisfying the former
    * Must have $`T(y) \leq B <_! m(I)`$ **non-abstractly**, i.e. $`\diamond (B <_! m(I))`$ is not true for the minimal $`B`$ satisfying the former.

#### **Case LINKS_LIST_INS** 
* 🔶 `$x links ($I[]: <T_LIST>)` replaces $`m(x) :_! A()`$ by $`m(x) :_! A(l : [m(I)])`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

_System property_:

1. 🔶 _Single list_. Transaction will fail if $`m(x) :_! A(...)`$ already has a roleplayer list. (In this case, user should `update` instead!)
1. 🔶 _Capability check_. 
    * Must have $`T(x) \leq B : \mathbf{Rel}(m(I))`$ **non-abstractly**, i.e. $`\diamond (B : \mathbf{Rel}(m(I)))`$ is not true for the minimal choice of $`B`$ satisfying the former
    * Must have $`l_i : T_i \leq B <_! m(I)`$ **non-abstractly**, i.e. $`\diamond (B <_! m(I))`$ is not true for the minimal $`B`$ satisfying the former.

#### **Case HAS_INS**
* ✅ `$x has $A $y` adds new $`a :_! m(A)(m(x) : O_{m(A)})`$ and
    * If `$y` is instance add the new cast $`\mathsf{val}(a) = \mathsf{val}(y)`$
    * If `$y` is value var set $`\mathsf{val}(a) = m(a)`$
* 🔷 `$x has $A <VAL_EXPR>` adds new element $`a :_! m(A)(m(x) : O_{m(A)})`$ and add cast $`\mathsf{val}(a) = v_m(val\_expr)`$

_System property_:

1. ✅ _Idempotency_. If $`a :_! m(A)`$ with $`\mathsf{val}(a) = \mathsf{val}(b)`$ then we equate $`a = b`$.
1. 🔶 _Capability check_. Must have $`T(x) \leq B <_! O_{m(A)}`$ **non-abstractly**, i.e. $`\diamond (B <_! O_{m(A)})`$ is not true for the minimal choice of $`B`$ satisfying the former
1. 🔶 _Type check_. Must have $`T(y) \leq m(A)`$ **or** $`T(y) = V`$ where $`\mathsf{val} : m(A) \to V`$ (similarly for `<EXPR>`)

_Remark_: ⛔ Previously we had the constraint that we cannot add $`m(y) :_! A(m(x) : O_A)`$ if there exists any subtype $`B \lneq A`$.

#### **Case HAS_LIST_INS**
* 🔶 `$x has $A[] <LIST_EXPR>` adds new $`l = [l_1, l_2, ...] :_! [m(A)](m(x) : O_{m(A)[]})`$ and $`l_i :_! m(A)(m(x) : O_{m(A)})`$ where
    * $`l`$ has the same length as $`[v_1,v_2, ...] = v_m(list\_expr)`$
    * if $`v_i`$ is an attribute instance, add new cast $`\mathsf{val}(l_i) = \mathsf{val}(v_i)`$ 
    * if $`v_i`$ is a value, add new cast $`\mathsf{val}(l_i) = v_i`$ 

_System property_:

1. ✅ _Idempotency_. Idempotency is automatic (since lists are identified by their list elements).
1. 🔶 _System cardinality bound: **1 list per owner**_. We cannot have $`k :_! m(A)(m(x) : O_{m(A)})`$ with $`k \neq l`$. (Users should use "Update" instead!)
1. 🔶 _Capability check_. Must have $`T(x) \leq B <_! O_{m(A)}`$ **non-abstractly**, i.e. $`\diamond (B <_! O_{m(A)})`$ is not true for the minimal choice of $`B`$ satisfying the former
1. 🔶 _Type check_.For each list element, must have $`T(l_i) \leq m(A)`$ or $`T(l_i) = V`$ where $`\mathsf{val} : A \to V`$

### Optional inserts

#### **Case TRY_INS**
* 🔶 `try { <INS>; ...; <INS>; }` where `<INS>` are insert statements as described above.
  * `<TRY_INS>` blocks can appear alongside other insert statements in an `insert` clause
  * Execution is as described in "Basics of inserting" (**#BDD**)



## Delete semantics

### Basics of deleting


A `delete` clause comprises collection of _delete statements_.

* _Input crows_: The clause can take as input a stream `{ m }` of concept rows `m`: 
  * the clause is **executed** for each row `m` in the stream individually

* _Updating input rows_: Delete clauses can update bindings of their input concept row `m`
  * Executing `delete $x;` will remove `$x` from `m` (but `$x` may still appear in other crows `m'` of the input stream)

_Remark_: Previously, it was suggested: if `$x` is in `m` and $`m(x)`$ is deleted from $`T_m(x)`$ by the end of the execution of the clause (for _all_ input rows of the input stream) then we set $`m(x) = \emptyset`$ and $`T_m(x) = \emptyset`$.
Fundamental question: **is it better to silently remove vars? Or throw an error if vars pointing to deleted concepts are used?** (STICKY)
* Only for `delete $x;` can we statically say that `$x` must not be re-used
* Other question: would this interact with try? idea: take $`m(x) = \emptyset`$ if it points to a previously deleted concept

<!--
match 
  $x isa Thing, has Property $p;
delete 
  Property $p of $x;
... // can still use $p

match
  $x isa Thing, has Id "id", has Property $p;
  $y isa Bool; try { $y == true; $x has Property $q; }
delete
  try { Property $q of $x; }
... // output could be:
($x -> "id", $p -> EMPTY, $y -> false)
($x -> "id", $p -> EMPTY, $y -> true, $q -> EMPTY)

... removing attributes tells us NOTHING about how many "has" references were actually deleted!!
-->

#### (Theory) execution

* _Execution_: An `delete` clause is executed by executing its statements individually.
  * Not all statement need to execute (see Optionality below)
    * **runnable** statements will be executed
    * **skipped** statements will not be executed
  * The order of execution is arbitrary order.
  * Executions of statements will modify the database state by 
    * removing elements 
    * remove dependencies
  * Modification are buffered in transaction (see "Transactions")
  * Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

#### (Feature) optionality

* 🔶 _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `delete` clauses cannot be nested
  * `try` blocks variables are **block-level bound** if they are bound in `m`
  * If any variable is not block-level bound, the `try` block statements are **skipped**.
  * If all variables are block-level bound, the `try` block statements are **runnable**.

### Delete statements

#### **Case CONCEPT_DEL**
* ✅ `$x;` removes $`m(x) :_! A(...)`$. If $`m(x)`$ is an object, we also:
  * replaces any $`b :_! B(m(x) : I, z : J, ...)`$ by $`b :_! B(z : J, ...)`$ for all such dependencies on $`m(x)`$

_Remark 1_. This applies both to $`B : \mathbf{Rel}`$ and $`B : \mathbf{Att}`$.

_Remark 2_. The resulting $`m(x) :_! m(A)(z : J, ...)`$ must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

_System property_:

1. 🔷 If $`m(x) : A : \mathbf{Att}`$ and $`A`$ is _not_ marked `@independent` then the transaction will fail.


#### **Modifier: CASCADE_DEL**
* 🔮 `delete` clause keyword can be modified with a `@cascade(<LABEL>,...)` annotation, which acts as follows:

  If `@cascade(C, D, ...)` is specified, and `$x` is delete then we not only remove $`m(x) :_! A(...)`$ but (assuming $`m(x)`$ is an object) we also:
  * whenever we replace $`b :_! B(m(x) : I, z : J, ...)`$ by $`b :_! B(z : J, ...)`$ and the following are _both_ satisfied:

    1. the new axiom $`b :_! B(...)`$ violates interface cardinality of $`B`$,
    2. $`B`$ is among the listed types `C, D, ...`
    
    then delete $`b`$ and _its_ depenencies (the cascade may recurse).

_Remark_. In an earlier version of the spec, condition (1.) for the recursive delete was omitted—however, there are two good reasons to include it:

1. The extra condition only makes a difference when non-default interface cardinalities are imposed, in which case it is arguably useful to adhere to those custom constraints.
2. The extra condition ensure that deletes cannot interfere with one another, i.e. the order of deletion does not matter.

#### **Case ROL_OF_DEL**
* ✅ `($I: $y) of $x` replaces $`m(x) :_! m(A)(m(y) : m(I), z : J, ...)`$ by $`m(x) :_! m(A)(z : J, ...)`$

_Remark_. The resulting $`m(x) :_! m(A)(z : J, ...)`$ must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

#### **Case ROL_LIST_OF_DEL**
* 🔶 `($I[]: <T_LIST>) of $x` replaces $`m(x) :_! m(A)(l : m(I))`$ by $`m(x) :_! m(A)()`$ for $`l`$ being the evaluation of `T_LIST`.

#### **Case ATT_OF_DEL**
* 🔷 `$B $y of $x` replaces $`m(y) :_! B'(m(x) : O_{m(B)})`$ by $`m(y) :_! B'()`$ for all possible $`B' \leq m(B)`$

_Remark_. Note the subtyping here! It only makes sense in this case though since the same value `$y` may have been inserted in multiple attribute subtypes (this is not the case for **LINKS_DEL**)—at least if we lift the "Leaf attribute system constraint".

#### **Case ATT_LIST_OF_DEL**
* 🔶 `$B[] <T_LIST> of $x` deletes $`l :_! B'(m(x) : O_{m(B)})`$ for all possible $`B' \leq m(B)`$ and $`l`$ being the evaluation of `T_LIST`. (STICKY: discuss! Suggestion: we do not retain list elements as independent attributes.)


### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete  clause.

## Update semantics

### Basics of updating

A `update` clause comprises collection of _update statements_.

* _Input crow_: The clause can take as input a stream `{ m }` of concept rows `m`, in which case 
  * the clause is **executed** for each row `m` in the stream individually

* _Updating input rows_: Update clauses do not update bindings of their input crow `m`

#### (Theory) Execution

* _Execution_: An `update` clause is executed by executing its statements individually in any order.
  * STICKY: this might be non-deterministic if the same thing is updated multiple times, solution outlined here: throw error if that's the case!

#### (Feature) Optionality

* _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `delete` clauses cannot be nested
  * `try` blocks variables are **block-level bound** if they are supplied by `m`
  * If any variable is not block-level bound, the `try` block statements are **skipped**.
  * If all variables are block-level bound, the `try` block statements are **runnable**.

### Update statements

#### **Case LINKS_UP**
* 🔶 `$x links ($I: $y);` updates $`m(x) :_! A(b:J)`$ to $`m(x) :_! A(m(x) : m(I))`$

_System property_:

1. 🔶 Require there to be exactly one present roleplayer for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction. (STICKY: discuss!)

#### **Case LINKS_LIST_UP** 
* 🔶 `$x links ($I[]: <T_LIST>)` updates $`m(x) :_! A(j : [m(I)])`$ to $`m(x) :_! A(l : [m(I)])`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

_System property_:

1. 🔶 Require there to be a present roleplayer list for update to succeed (can have at most one).
1. 🔶 Require that each update happens at most once, or fail the transaction.

#### **Case HAS_UP**
* 🔶 `$x has $B: $y;` updates $`b :_! m(B)(x:O_{m(B)})`$ to $`m(y) :_! m(B)(x:O_{m(B)})`$

_System property_:

1. 🔶 Require there to be exactly one present attribute for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction.

#### **Case HAS_LIST_UP**
* 🔶 `$x has $A[] <T_LIST>` updates $`j :_! [m(A)](m(x) : O_{m(A)})`$ to $`l :_! [m(A)](m(x) : O_{m(A)})`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

_System property_:

1. 🔶 Require there to be a present attribute list for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction.


### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete clause.

## Put semantics

* 🔶 `put <PUT>` is equivalent to 
  ```
  if (match <PUT>; check;) then (match <PUT>;) else (insert <PUT>)
  ```
  In particular, `<PUT>` needs to be an `insert` compatible set of statements. 

# Query execution principles

## Basics: Pipelines, clauses, operators, branches

Pipelines comprises chains of clauses and operators, and may branch.

_Key principle_:

* Each clause/operator:
    * gets an input stream (initially this is the empty crow stream `{}`) 
    * outputs a crow stream
* **Clauses** use the DB for their operation
* **Operators** only operate on the stream at hand
* Clauses and operators are executed eagerly
    * In this way, executing later stages of the pipeline can never affect earlier stages.
* Pipelines can be **branched**, in which case each branch is executed separately
    * _(of course, shared results will be cached for performance!)_

## Clauses (match, insert, delete, update, put, fetch)

Clauses are stages in which patterns are matched or statements are executed.

### Match

As described in "Match semantics".

### Insert

As described in "Insert semantics".

### Delete

As described in "Delete semantics".

### Update

As described in "Update semantics".

### Put

As described in "Put semantics".

### Fetch

* 🔶 The `fetch` clause is of the form

  ```
  fetch { 
   <fetch-KV-statement>;
   ...
   <fetch-KV-statement>;
  }
  ```

  * The `fetch` clause takes as input a crow stream `{ m }`
  * It output a stream `{ doc<m> }` of JSON documents (one for each `m` in the input stream)
  * The `fetch` clause is **terminal**

#### **Case FETCH_VAL**
* 🔶 `"key": $x`

#### **Case FETCH_EXPR**
* 🔶 `"key": <EXPR>`

_Note_. `<EXPR>` can, in particuar, be `T_LIST` expression (see "Expressions").

#### **Case FETCH_ATTR**
* 🔶 `"key": $x.A` where $`A : \mathbf{Att}`$

_System property_

1. 🔶 fails transaction if $`T_m(x)`$ does not own $`A`$ with `card(0,1)`.

#### **Case FETCH_MULTI_ATTR**
* 🔶 `"key": [ $x.A ]` where $`A : \mathbf{Att}`$

_System property_

1. 🔶 fails transaction if $`T_m(x)`$ does not own $`A`$.

#### **Case FETCH_LIST_ATTR**
* 🔶 `"key": $x.A[]` where  $`A : \mathbf{Att}`$

_System property_

1. 🔶 fails transaction if $`T_m(x)`$ does not own $`[A]`$.

#### **Case FETCH_SNGL_FUN**
* 🔶 `"key": fun(...)` where `fun` is single-return.

#### **Case FETCH_STREAM_FUN**
* 🔶 `"key": [ fun(...) ]` where `fun` is stream-return.

_Note_: (STICKY:) what to do if type inference for function args fails based on previous pipeline stages?

#### **Case FETCH_FETCH**
* 🔶 Fetch list of JSON sub-documents:
```
"key": [ 
  <PIPELINE>
  fetch { <FETCH> }
]
```

#### **Case FETCH_RETURN_VAL** 
* 🔶 Fetch single-value:
```
"key": ( 
  <PIPELINE>
  return <SINGLE_RET> <VAR>; 
)
```

#### **Case FETCH_REDUCE_LIST_VAL** 
* 🔶 Fetch stream as list:
```
"key": [ 
  <PIPELINE>
  reduce <AGG>, ... , <AGG>; 
]
```

#### **Case FETCH_NESTED**
* 🔶 Specify JSON sub-document:
```
"key" : { 
  <FETCH-KV-STATEMENT>;
  ...
  <FETCH-KV-STATEMENT>;
}
```

## Operators (select, distinct, sort, limit, offset, reduce)

Operators (unlike clauses) are **pure**: they do not depend on the DB (i.e. they do not read or write), they just operate directly on the stream that is input into them.

### Select
* 🔶 select syntax:
    `select $x1, $x2, ...`
     
    * input stream of rows `{ m }`
    * output stream of rows `{ p(m) }` for each `m` in the input, where `p(m)` only keeps the given variables that are among `$x1, $x2, ...`

### Deselect 
* 🔶 deselect syntax:
    `deselect $x1, $x2, ...`
     
    * input stream of rows `{ m }`
    * output stream of rows `{ p(m) }` for each `m` in the input, where `p(m)` only keeps the given variables that are **not** among `$x1, $x2, ...`

### Distinct
* 🔶 distinct syntax:
    `deselect $x1, $x2, ...`
     
    * input stream of rows `{ m }`
    * output stream of rows `{ n }` for each distinct row in the input (in other words: duplicates are removed)
        * empty value is its own distinct value

### Require
* 🔮 distinct syntax:
    `require $x1, $x2, ...`
     
    * filters `{ m }` keeping only maps where `m($x1)`, `m($x2)`, ... are non-empty

### Sort
* 🔶 sort syntax:
    `sort $x1, $x2, ...`
     
    * input stream of rows `{ m }`
    * output stream of rows `{ n }` obtained by ordering the input stream:
      * first on values `m($x1)`
      * then on values `m($x2)`,
      * ...

**Remark** absent values are sorted last.

### Limit
* 🔶 
    `limit <NUM>`

    * outputs input stream, truncates after `<NUM>` concept rows

### Offset
* 🔶 
    `limit <NUM>`

    * outputs input stream, offset by `<NUM>` concept rows

_Remark_: Offset is only useful when streams (and the order of answers) are fully deterministic.

### Reduce
* 🔶 Key principles:
    * The `reduce` operator takes as input a stream of rows `{ m }`
    * It outputs a stream of new concept rows

#### **Case SIMPLE_RED**
* 🔶 Default reduce syntax
    ```
    reduce $x_1=<AGG>, ... , $x_k=<AGG>;
    ``` 

    In this case, we output a ***single concept*** row `($x_1 -> <EL>, $x_2 -> <EL>, ...)`, where `<EL>` is a output element (i.e. instance, value, or list, but _never_ type) constructed as follows:


* 🔶 `<AGG>` is one of the following **aggregate functions**:
  * 🔶 `check`:
    * output type `bool`
    * outputs `true` if concept row stream is non-empty
  * 🔶 `check($x)`:
    * output type `bool`
    * outputs `true` if concept row stream contains a row `m` with non-empty entry `m($x)` for `$x`
  * 🔶 `sum($x)`:
    * output type `double` or `int`
    * outputs sum of all non-empty `m($x)` in concept row `m`
    * `$x` can be optional
    * empty sums yield `0.0` or `0`
  * 🔶 `mean($x)`:
    * output type `double?`
    * outputs mean of all non-empty `m($x)` in concept row `m`
    * `$x` can be optional
    * empty mean yield empty output ($\emptyset$)
  * 🔶 `median($x)`, 
    * output type `double?` or `int?` (depending on type of `$x`)
    * outputs median of all non-empty `m($x)` in concept row `m`
    * `$x` can be optional
    * empty medians output $\emptyset
  * 🔶 `count`
    * output type `long`
    * outputs count of all answer
  * 🔶 `count($x)`
    * output type `long`
    * outputs count of all non-empty `m($x)` in input crow stream `{ m }`
    * `$x` can be optionals
  * 🔮 `count($x, $y, ...)`
    * output type `long`
    * outputs count of all non-empty concept tuples `(m($x), m($y), ...)` in input crow stream
    * `$x` can be optional
  * 🔮 `distinct($x)`
    * output type `long`
    * outputs count of all non-empty **distinct** `m($x)` in input crow stream `{ m }`
    * `$x` can be optionals
  * 🔮 `distinct($x, $y, ...)`
    * output type `long`
    * outputs count of all non-empty **distinct** concept tuples `(m($x), m($y), ...)` in input crow stream `{ m }`
    * `$x` can be optional
  * 🔶 `list($x)`
    * output type `[A]`
    * returns list of all non-empty `m($x)` in concept row `m`
    * `$x` can be optional
* Each `<AGG>` reduces the concept row `{ m }` passsed to it from the function's body to a single value in the specified way.

#### **Case GROUP_RED**
* 🔶 Groupe reduce syntax:
    ```
    reduce $x_1=<AGG>, ... , $x_k=<AGG> within $y_1, $y_2, ...;
    ``` 

    In this case, we output the following:
    * 🔶 for each distinct tuple of elements `el_1, el_2, ...` assigned to `$y_1, $y_2, ...` by rows in the stream, we perform the aggregates as described above over _all rows `m`_ for which `m($y_1) = el_1, m($y__2) = el_2, ...` and then output the resulting concept row `($y_1 -> el_1, $y_2 = el_2, ..., $x_1 -> <CPT>, $x_2 -> <CPT>, ...)`


## Branches

(to be written)

## Transactions

(to be written)

### Basics

(to be written)

### Snapshots

(to be written)

### Concurrency

(to be written)

# Glossary

## Type system

### Type 

Any type in the type system.

### Schema type

A type containg data inserted into the database. Cases: 

* Entity type, 
* Relation type, 
* Attribute type,
* Interface type.

### Value type

A type containing a pre-defined set of values. Cases:

* Primitive value type: `bool`, `string`, ...
* Structured value type / struct value type / struct: user-defined

### Data instance / instance

An element in an entity, relation, or attribute type. Special cases:

* Entity: An element in an entity type.
* Relation: An element in a relation type.
* Attribute: An element in attribute type.
* Object: An element in an entity or relation type.
* Roleplayer: Object that is cast into the element of a role type.
* Owner: Object that is cast into the element of an ownership type.

### Data value / value

An element in a value type.

### Attribute instance value / attribute value

An attribute cast into an element of its value type.

### Data element / element

Any element in any type (i.e. value or instance).

### Concept

A element or a type.

### Concept row

Mapping of variables to concepts

### Stream

An ordered concept row.

### Answer set

The set of concept rows that satisfy a pattern in the minimal way.

### Answer 

An element in the answer set of a pattern.


## TypeQL syntax

### Schema query

(Loosely:) Query pertaining to schema manipulation

### Data query

(Loosely:) Query pertaining to data manipulation or retrieval

### Clause / Stream clause

* `match`, `insert`, `delete`, `define`, `undefine`,

### Operators / Stream operator

* `select`, `sort`, ... 

### Functions

callable `match-return` query. can be single-return or stream-return.

### Statement

A syntactic unit (cannot be subdivided). Variations:

* Simple statement: statement not containing `,`
* Combined statement: statement combined with `,`

### Pattern

Context for variables (this "context" describes properties of the variables), used to form a data retrieval query.

### Stream reduction / reduction

* `list`, `sum`, `count` ...

### Clause

part of a query pipeline

### Block

`{ pattern }` 

### Suffix

`[]` and `?`.


## Syntactic Sugar

* General SVO shortening:
  * `X <keyword> <expr>; X <keyword> <expr>;` shortens to `X <keyword> <expr>, <keyword> <expr>;`
  * `<keyword>` can be `sub, relates, value, owns, plays, isa, links, has`
* General OVS shortening
  * `<expr> <keyword> X; <expr> <keyword>  X;` shortens to `<expr>, <expr> <keyword> X;`
  * `<keyword>` can be `from, of`
* `$x in <LIST_EXPR>;` is equivalent to `$l = <LIST_EXPR>; $x in $l;` 
* `$x has $y;` is equivalent to `$x has $_ $y;` for anonymous `$_`
* `$x links ($y);` is equivalent to `$x links ($_: $y)` for anonymous `$_`
* `$x has $B <EXPR>;` abbreviates `$x has $B $y; $y = <EXPR>;`
* `$x has $B <COMP> <EXPR>;` abbreviates `$x has $B $y; $y <COMP> <EXPR>;`
  * `<COMP>` is a comparator (`>, <, >=, <=` etc.)
* `$x has $y` - `$x has $_Y $y;` (_not_: `$_Y[]` !)
* `$x has! $t $y` abbreviates `$x has $t $y; $y isa! $t;`
* `$x links! ($t: $y);` abbreviates `$x isa! $s; $x links ($t: $y);`

## Typing of operators

```
+ : T_LIST x T_LIST -> T_LIST
+ : STRING x STRING -> STRING
+ : DBL x DBL -> DBL
```
