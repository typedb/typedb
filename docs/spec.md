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
* [Data definition language](#data-definition-language)
    * [Basics of schemas](#basics-of-schemas)
        * [(Theory) Clauses and execution](#theory-clauses-and-execution)
        * [(Feature) Pipelined definitions](#feature-pipelined-definitions)
        * [(Feature) Variabilized definitions](#feature-variabilized-definitions)
    * [Define semantics](#define-semantics)
        * [Type axioms](#type-axioms)
        * [Constraints](#constraints)
        * [Triggers](#triggers)
        * [Value types](#value-types)
        * [Functions defs](#functions-defs)
    * [Undefine semantics](#undefine-semantics)
        * [Type axioms](#type-axioms-1)
        * [Constraints](#constraints-1)
        * [Triggers](#triggers-1)
        * [Value types](#value-types-1)
        * [Functions defs](#functions-defs-1)
    * [Redefine semantics](#redefine-semantics)
        * [Type axioms](#type-axioms-2)
        * [Constraints](#constraints-2)
        * [Triggers](#triggers-2)
        * [Value types](#value-types-2)
        * [Functions defs](#functions-defs-2)
    * [Labels and aliases](#labels-and-aliases)
        * [Define](#define)
        * [Undefine](#undefine)
        * [Redefine](#redefine)
* [Pattern matching language](#pattern-matching-language)
    * [Basics: Patterns, variables, concept rows, satisfaction](#basics-patterns-variables-concept-rows-satisfaction)
        * [(Theory) Statements and pattern](#theory-statements-and-pattern)
        * [(Feature) Pattern operations](#feature-pattern-operations)
        * [(Theory) Pattern branches](#theory-pattern-branches)
        * [(Theory) Variables and bindings](#theory-variables-and-bindings)
        * [(Theory) Typed concept rows](#theory-typed-concept-rows)
        * [Input crows for patterns](#input-crows-for-patterns)
    * [Pattern semantics](#pattern-semantics)
        * [Satisfaction and answers](#satisfaction-and-answers)
        * [Typing satisfaction](#typing-satisfaction)
        * [Pattern satisfaction](#pattern-satisfaction)
        * [`Let` declarations in patterns](#let-declarations-in-patterns)
    * [Satisfaction semantics of...](#satisfaction-semantics-of)
    * [... Function statements](#-function-statements)
        * [**Case LET_FUN_PATT**](#case-let_fun_patt)
        * [**Case LET_IN_FUN_PATT**](#case-let_in_fun_patt)
    * [... Type statements](#-type-statements)
        * [**Case TYPE_DEF_PATT**](#case-type_def_patt)
        * [**Case REL_PATT**](#case-rel_patt)
        * [**Case DIRECT_REL_PATT**](#case-direct_rel_patt)
        * [**Case PLAYS_PATT**](#case-plays_patt)
        * [**Case DIRECT_PLAYS_PATT**](#case-direct_plays_patt)
        * [**Case VALUE_PATT**](#case-value_patt)
        * [**Case OWNS_PATT**](#case-owns_patt)
        * [**Case DIRECT_OWNS_PATT**](#case-direct_owns_patt)
        * [**Cases TYP_IS_PATT and LABEL_PATT**](#cases-typ_is_patt-and-label_patt)
    * [... Type constraint statements](#-type-constraint-statements)
        * [Cardinality](#cardinality)
        * [Modalities](#modalities-2)
        * [Values constraints](#values-constraints)
    * [... Element statements](#-element-statements)
        * [**Case ISA_PATT**](#case-isa_patt)
        * [**Case ANON_ISA_PATT**](#case-anon_isa_patt)
        * [**Case DIRECT_ISA_PATT**](#case-direct_isa_patt)
        * [**Case LINKS_PATT**](#case-links_patt)
        * [**Case LINKS_LIST_PATT**](#case-links_list_patt)
        * [**Case DIRECT_LINKS_PATT**](#case-direct_links_patt)
        * [**Case HAS_PATT**](#case-has_patt)
        * [**Case HAS_LIST_PATT**](#case-has_list_patt)
        * [**Case DIRECT_HAS_PATT**](#case-direct_has_patt)
        * [**Case IS_PATT**](#case-is_patt)
    * [... Expression and list statements](#-expression-and-list-statements)
        * [Expressions grammar](#expressions-grammar)
        * [Expression evaluation](#expression-evaluation)
        * [(Feature) Boundedness of variables in expressions](#feature-boundedness-of-variables-in-expressions)
        * [Simple expression patterns](#simple-expression-patterns)
        * [List expression patterns](#list-expression-patterns)
        * [**Case LET_IN_LIST_PATT**](#case-let_in_list_patt)
        * [**Case LIST_CONTAINS_PATT**](#case-list_contains_patt)
* [Data manipulation language](#data-manipulation-language)
    * [Match semantics](#match-semantics)
    * [Function semantics](#function-semantics)
        * [Function signature, body, operators](#function-signature-body-operators)
        * [(Theory) Function evaluation](#theory-function-evaluation)
        * [(Theory) Order of execution (and recursion)](#theory-order-of-execution-and-recursion)
    * [Insert behavior](#insert-behavior)
        * [Basics of inserting](#basics-of-inserting)
        * [Insert statements](#insert-statements)
        * [Optional inserts](#optional-inserts)
    * [Delete semantics](#delete-semantics)
        * [Basics of deleting](#basics-of-deleting)
        * [Delete statements](#delete-statements)
        * [Clean-up](#clean-up)
    * [Update behavior](#update-behavior)
        * [Basics of updating](#basics-of-updating)
        * [Update statements](#update-statements)
        * [Clean-up](#clean-up-1)
    * [Put behavior](#put-behavior)
* [Query execution principles](#query-execution-principles)
    * [Basics: Pipelines, clauses, operators, branches](#basics-pipelines-clauses-operators-branches)
    * [Clauses (match, insert, delete, update, put, fetch)](#clauses-match-insert-delete-update-put-fetch)
        * [Match](#match)
        * [Insert](#insert)
        * [Delete](#delete)
        * [Update](#update)
        * [Put](#put)
        * [Fetch](#fetch)
    * [Operators (select, distinct, sort, limit, offset, reduce)](#operators-select-distinct-sort-limit-offset-reduce)
        * [Select](#select)
        * [Deselect](#deselect)
        * [Distinct](#distinct)
        * [Require](#require)
        * [Sort](#sort)
        * [Limit](#limit)
        * [Offset](#offset)
        * [Reduce](#reduce)
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
        * [Functions](#functions)
        * [Statement](#statement)
        * [Pattern](#pattern)
        * [Stream reduction / reduction](#stream-reduction--reduction)
        * [Clause](#clause)
        * [Block](#block)
        * [Suffix](#suffix)
    * [Syntactic Sugar](#syntactic-sugar)
    * [Typing of operators](#typing-of-operators)

</details>


<details>
  <summary> <b>Table of contents</b> <i>(Detailed)</i> </summary>

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
* [Data definition language](#data-definition-language)
    * [Basics of schemas](#basics-of-schemas)
        * [(Theory) Clauses and execution](#theory-clauses-and-execution)
        * [(Feature) Pipelined definitions](#feature-pipelined-definitions)
        * [(Feature) Variabilized definitions](#feature-variabilized-definitions)
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
        * [(Theory) Statements and pattern](#theory-statements-and-pattern)
        * [(Feature) Pattern operations](#feature-pattern-operations)
            * [**Case AND_PATT**](#case-and_patt)
            * [**Case OR_PATT**](#case-or_patt)
            * [**Case NOT_PATT**](#case-not_patt)
            * [**Case TRY_PATT**](#case-try_patt)
        * [(Theory) Pattern branches](#theory-pattern-branches)
        * [(Theory) Variables and bindings](#theory-variables-and-bindings)
            * [**Variable categories**](#variable-categories)
            * [**Bound variables and valid patterns**](#bound-variables-and-valid-patterns)
            * [**Variable modes and unambiguous patterns**](#variable-modes-and-unambiguous-patterns)
            * [**Anonymous variables**](#anonymous-variables)
        * [(Theory) Typed concept rows](#theory-typed-concept-rows)
        * [Input crows for patterns](#input-crows-for-patterns)
    * [Pattern semantics](#pattern-semantics)
        * [Satisfaction and answers](#satisfaction-and-answers)
        * [Typing satisfaction](#typing-satisfaction)
        * [Pattern satisfaction](#pattern-satisfaction)
            * [Block-level bindings](#block-level-bindings)
            * [Recursive definition](#recursive-definition)
        * [`Let` declarations in patterns](#let-declarations-in-patterns)
    * [Satisfaction semantics of...](#satisfaction-semantics-of)
    * [... Function statements](#-function-statements)
        * [**Case LET_FUN_PATT**](#case-let_fun_patt)
        * [**Case LET_IN_FUN_PATT**](#case-let_in_fun_patt)
    * [... Type statements](#-type-statements)
        * [**Case TYPE_DEF_PATT**](#case-type_def_patt)
        * [**Case REL_PATT**](#case-rel_patt)
        * [**Case DIRECT_REL_PATT**](#case-direct_rel_patt)
        * [**Case PLAYS_PATT**](#case-plays_patt)
        * [**Case DIRECT_PLAYS_PATT**](#case-direct_plays_patt)
        * [**Case VALUE_PATT**](#case-value_patt)
        * [**Case OWNS_PATT**](#case-owns_patt)
        * [**Case DIRECT_OWNS_PATT**](#case-direct_owns_patt)
        * [**Cases TYP_IS_PATT and LABEL_PATT**](#cases-typ_is_patt-and-label_patt)
    * [... Type constraint statements](#-type-constraint-statements)
        * [Cardinality](#cardinality-3)
            * [**Case CARD_PATT**](#case-card_patt)
        * [Modalities](#modalities-5)
            * [**Case UNIQUE_PATT**](#case-unique_patt)
            * [**Case KEY_PATT**](#case-key_patt)
            * [**Case SUBKEY_PATT**](#case-subkey_patt)
            * [**Case TYP_ABSTRACT_PATT**](#case-typ_abstract_patt)
            * [**Case REL_ABSTRACT_PATT**](#case-rel_abstract_patt)
            * [**Case PLAYS_ABSTRACT_PATT**](#case-plays_abstract_patt)
            * [**Case OWNS_ABSTRACT_PATT**](#case-owns_abstract_patt)
            * [**Case DISTINCT_PATT**](#case-distinct_patt)
        * [Values constraints](#values-constraints)
            * [**Cases VALUE_VALUES_PATT and OWNS_VALUES_PATT**](#cases-value_values_patt-and-owns_values_patt)
    * [... Element statements](#-element-statements)
        * [**Case ISA_PATT**](#case-isa_patt)
        * [**Case ANON_ISA_PATT**](#case-anon_isa_patt)
        * [**Case DIRECT_ISA_PATT**](#case-direct_isa_patt)
        * [**Case LINKS_PATT**](#case-links_patt)
        * [**Case LINKS_LIST_PATT**](#case-links_list_patt)
        * [**Case DIRECT_LINKS_PATT**](#case-direct_links_patt)
        * [**Case HAS_PATT**](#case-has_patt)
        * [**Case HAS_LIST_PATT**](#case-has_list_patt)
        * [**Case DIRECT_HAS_PATT**](#case-direct_has_patt)
        * [**Case IS_PATT**](#case-is_patt)
    * [... Expression and list statements](#-expression-and-list-statements)
        * [Expressions grammar](#expressions-grammar)
        * [Expression evaluation](#expression-evaluation)
            * [Value expressions](#value-expressions)
            * [List expressions](#list-expressions)
        * [(Feature) Boundedness of variables in expressions](#feature-boundedness-of-variables-in-expressions)
        * [Simple expression patterns](#simple-expression-patterns)
            * [**Case LET_PATT**](#case-let_patt)
            * [**Case LET_DESTRUCT_PATT**](#case-let_destruct_patt)
            * [**Case EQ_PATT**](#case-eq_patt)
            * [**Case COMP_PATT**](#case-comp_patt)
        * [List expression patterns](#list-expression-patterns)
        * [**Case LET_IN_LIST_PATT**](#case-let_in_list_patt)
        * [**Case LIST_CONTAINS_PATT**](#case-list_contains_patt)
* [Data manipulation language](#data-manipulation-language)
    * [Match semantics](#match-semantics)
    * [Function semantics](#function-semantics)
        * [Function signature, body, operators](#function-signature-body-operators)
            * [**Case SIGNATURE_STREAM_FUN**](#case-signature_stream_fun)
            * [**Case SIGNATURE_SINGLE_FUN**](#case-signature_single_fun)
            * [**Case READ_PIPELINE_FUN**](#case-read_pipeline_fun)
            * [**Case RETURN_STREAM_FUN**](#case-return_stream_fun)
            * [**Case RETURN_SINGLE_FUN**](#case-return_single_fun)
            * [**Case AGG_RETURN_FUN**](#case-agg_return_fun)
      * [(Theory) Function semantics](#theory-function-semantics)
        * [(Theory) Order of execution (and recursion)](#theory-order-of-execution-and-recursion)
    * [Insert behavior](#insert-behavior)
        * [Basics of inserting](#basics-of-inserting)
            * [(Theory) Execution](#theory-execution)
            * [(Feature) Optionality](#feature-optionality)
        * [Insert statements](#insert-statements)
            * [**Case LET_INS**](#case-let_ins)
            * [**Case ISA_INS**](#case-isa_ins)
            * [**Case ANON_ISA_INS**](#case-anon_isa_ins)
            * [**Case LINKS_INS**](#case-links_ins)
            * [**Case LINKS_LIST_INS**](#case-links_list_ins)
            * [**Case HAS_INS**](#case-has_ins)
            * [**Case HAS_LIST_INS**](#case-has_list_ins)
        * [Optional inserts](#optional-inserts)
            * [**Case TRY_INS**](#case-try_ins)
    * [Delete semantics](#delete-semantics)
        * [Basics of deleting](#basics-of-deleting)
            * [(Theory) execution](#theory-execution-1)
            * [(Feature) optionality](#feature-optionality-1)
        * [Delete statements](#delete-statements)
            * [**Case CONCEPT_DEL**](#case-concept_del)
            * [**Modifier: CASCADE_DEL**](#modifier-cascade_del)
            * [**Case ROL_OF_DEL**](#case-rol_of_del)
            * [**Case ROL_LIST_OF_DEL**](#case-rol_list_of_del)
            * [**Case ATT_OF_DEL**](#case-att_of_del)
            * [**Case ATT_LIST_OF_DEL**](#case-att_list_of_del)
        * [Clean-up](#clean-up)
    * [Update behavior](#update-behavior)
        * [Basics of updating](#basics-of-updating)
            * [(Theory) Execution](#theory-execution-2)
            * [(Feature) Optionality](#feature-optionality-2)
        * [Update statements](#update-statements)
            * [**Case LINKS_UP**](#case-links_up)
            * [**Case LINKS_LIST_UP**](#case-links_list_up)
            * [**Case HAS_UP**](#case-has_up)
            * [**Case HAS_LIST_UP**](#case-has_list_up)
        * [Clean-up](#clean-up-1)
    * [Put behavior](#put-behavior)
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
            * [**Case FETCH_ALL_ATTR**](#case-fetch_all_attr)
            * [**Case FETCH_SNGL_FUN**](#case-fetch_sngl_fun)
            * [**Case FETCH_STREAM_FUN**](#case-fetch_stream_fun)
            * [**Case FETCH_FETCH**](#case-fetch_fetch)
            * [**Case FETCH_RETURN_VAL**](#case-fetch_return_val)
            * [**Case FETCH_RETURN_STREAM**](#case-fetch_return_stream)
            * [**Case FETCH_RETURN_AGG**](#case-fetch_return_agg)
            * [**Case FETCH_NESTED**](#case-fetch_nested)
    * [Operators (select, distinct, sort, limit, offset, reduce)](#operators-select-distinct-sort-limit-offset-reduce)
        * [Select](#select)
        * [Deselect](#deselect)
        * [Distinct](#distinct)
        * [Require](#require)
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
        * [Functions](#functions)
        * [Statement](#statement)
        * [Pattern](#pattern)
        * [Stream reduction / reduction](#stream-reduction--reduction)
        * [Clause](#clause)
        * [Block](#block)
        * [Suffix](#suffix)
    * [Syntactic Sugar](#syntactic-sugar)
    * [Typing of operators](#typing-of-operators)

</details>

# Introduction

This document specifies the behavior of TypeDB and its query language TypeQL.

* Best viewed _not in Chrome_ (doesn't display math correctly)
* Badge system: 
    * ✅ -> implemented / part of alpha
    * 🔷 -> up next / part of beta
    * 🔶 -> part of first stable release
    * 🔮 -> on the roadmap 
    * ❓ -> speculative / to-be-discussed
    * ⛔ -> rejected

# The type system

TypeDB's type system is a [logical system](https://en.wikipedia.org/wiki/Formal_system#Deductive_system), which we describe in this section with a reasonable level of formality (not all details are included, and some basic mathematical rules are taken for granted: for example, the rule of equality, i.e. if $a = b$ then $a$ and $b$ are exchangeable for all purposes in our type system.)

It is convenient to present the type system in _two stages_ (though some people prefer to do it all in one go!):

* We first introduce and explain the **grammar** of statements in the system.
* We then discuss the **rule system** for inferring which statements are _true_.

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
  
  It is therefore convenient to use _set notation_, writing $`a : A(x : I, y : I)`$ as $`A : A(\{x,y\}:I)`$. (Similarly, when $`I`$ appears $`k`$ times in $`A(...)`$, we would write $`\{x_1, ..., x_k\} : I`$) 

* **Interface specialization notation**:  If $`A : \mathbf{Kind}(J)`$, $`B : \mathbf{Kind}(I)`$, $`A \lneq B`$ and $`J \lneq I`$, then we say:
  > The interface $`J`$ of $`A`$ **specializes** the interface $`I`$ of $`B`$

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

  _Remark_: This applies recursively to types with $`k`$ interfaces.

  _Example_: $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband})`$ and $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Wife})`$ then $`\mathsf{HeteroMarriage} : \mathbf{Rel}(\mathsf{Husband},\mathsf{Wife})`$

* **Weakening dependencies**: Given $`A : \mathbf{Kind}(I,J)`$, this *implies* $`A : \mathbf{Kind}(I)`$. In words:
  > Dependencies can be simply ignored (note: this is a coarse rule — we later discuss more fine-grained constraints, e.g. cardinality).

  _Remark_: This applies recursively to types with $`k`$ interfaces.

  _Example_: $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ implies $`\mathsf{Marriage} : \mathbf{Rel}(\mathsf{Spouse})`$ and also $`\mathsf{Marriage} : \mathbf{Rel}`$ (we identify the empty brackets "$`()`$" with no brackets).

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
    * _Example_: If $`m : \mathsf{Marriage}(\{x,y\} :\mathsf{Spouse})`$ then both $`m : \mathsf{Marriage}(x:\mathsf{Spouse})`$ and $`m : \mathsf{Marriage}(y:\mathsf{Spouse})`$

* **"Covariance of dependencies" rule**: If $`A(J) \leq B(I)`$ (see "interface specialization" in "Grammar and notation" above) and $`a : A(x:I)`$ then this _implies_ $`a : B(x:J)`$. In other words:
    > When $`A`$ casts to $`B`$, and $`I`$ to $`J`$, then $`A(x : I)`$ casts to $`B(x : J)`$.

    _Remark_: This applies recursively for types with $`k`$ interfaces.

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

### (Feature) Pipelined definitions

* 🔮 Definition clauses can be pipelined:
  * _Example_: 
    ```
    define A; 
    define B; 
    undefine C; 
    redefine E;
    ```

### (Feature) Variabilized definitions

* 🔶 Definition clauses can be preceded by a match clause
  * _Example_:
    ```
    match <PATT>; 
    define A;
    ```
  * _Interpretation_: `A` may contain non-optional **tvar**s bound in `PATT`; execute define for each results of match.

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
    * $`[A] : \mathbf{List}(O_{A[]})`$, $`[A] <_! [B]`$ and $`O_{A[]} <_! O_{B[]}`$ where $`B : \mathbf{Att}(O_B)`$

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
* ✅ `A relates I @card(n..m)` postulates $n \leq k \leq m$ whenever $`a :_! A'(\{x_1, ..., x_k\} : I)`$, $`A' \leq A`$, $`A' : \mathbf{Rel}(I)`$.
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

* In all cases, the purposse of abstractness is to ***constrain `insert` behavior.***
* In a commited schema, it is never possible that both $`\diamond(A : K)`$ and $`A : K`$ are both true the same time (and _neither implies the other_).

_System property_

* 🔶 _Abstractness prevails_. When both $`\diamond(A : K)`$ and $`A : K`$ are present after a define clause is completed, then we remove the latter statements from the type system. (_Note_. Abstractness can only be removed through ***undefining it***, not by "overwriting" it in another `define` statement ... define is always additive!).

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

1. 🔶 _Abstract interface inheritance_. Abstract interfaces are inherited if not specialized just like non-abstract interfaces. In math: if $`\diamond(B : \mathbf{Rel}(J))`$ and $`A \lneq B`$ without specializing $I$ (i.e. $`\not \exists I. A(I) \leq B(J)`$) then the type system will infer $`\diamond(B : \mathbf{Rel}(J))`$.
1. 🔶 _Upwards closure_. When `A relates I @abstract` and $`I \lneq J`$ then `A` also relates `J` abstractly.

_Remark_: In addition to user declarations, let's also recall the **three cases** in which `relates @abstract` gets implicitly inferred by the type system:
* Un-specialization: if a relation type relates a specialized interface, _then_ it abstractly relates the unspecialized versions of the interface.
* Un-specialization for lists: if a relation type relates a specialized list interface, _then_ it abstractly relates the unspecialized versions of the list interface.
* Un-ordering: if a relation type relates a list interface, _then_ it abstractly relates the "un-ordered" (un-listed?) interface.


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
* ✅ `(entity) A sub B` redefines $`A \leq B`$

#### **Case REL_REDEF**
* **cannot** redefine `relation A` 
* ✅ `(relation) A sub B` redefines $`A \leq B`$, ***requiring*** 
  * either $`A <_! B' \neq B`$ (to be redefined)
  * or $`A`$ has no direct super-type
* ✅ `(relation) A relates I` redefines $`A : \mathbf{Rel}(I)`$, ***requiring*** that $`A : \mathbf{Rel}([I])`$ (to be redefined)
  * _Inherited cardinality_: inherits card (default: `@card(0..)`) 
  * _Data transformation_: moves any $`a : A(l : [I])`$ with $`l = [l_0, l_1, ..., l_{k-1}]`$ to $`a : A(\{l_0,l_1,...,l_{k-1}\} : I`$
* ✅ `(relation) A relates I as J` redefines $`I <_! J`$, ***requiring*** that either $`I <_! J' \neq J`$ or $`I`$ has no direct super-role
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

# Pattern matching language

This section first describes the pattern matching language of TypeDB, which relies on _variables_.

## Basics: Patterns, variables, concept rows, satisfaction

### (Theory) Statements and pattern

* **statements:** syntactic units of TypeQL (see Glossary)
* **patterns:** collection of statements, combined with logical connectives.

### (Feature) Pattern operations

#### **Case AND_PATT**

* 🔷 _Conjunction_ `<PATT1> <PATT2>` 
  > We match "`<PATT1>` **and** `<PATT2>`" 

  * _Note:_ Any `<PATT>` is terminated with a `;`

#### **Case OR_PATT**

* 🔷 _Disjunction_ `{ <PATT1> } or { <PATT2> };` 
  > We **either** match `<PATT1>` or `<PATT2>`

  _Note_ This extends to $k$ patterns chained with interleaving `or`.

#### **Case NOT_PATT**

* 🔷 _Negation_. `not { <PATT> };` 
  > We ensure `<PATT>` has **no match**. 

  _Note_ `<PATT>` may have **quantified variables**

#### **Case TRY_PATT**

* 🔷 _Optional pattern_ `try { PATT };` 
  > We **optionally** match `PATT` whenever possible

_Terminology_ What's inside `<OP> { ... }` is called a **`<OP>`-block**.


### (Theory) Pattern branches

A **disjunctive normal (DNF) of a pattern `<PATT>`** is a pattern obtained by recursively applying transformations of the form 
    ```
    <PATT1> { <PATT2> } or {<PATT3> };
    --transforms to-->
    { <PATT1> <PATT2> } or { <PATT1> <PATT3> };
    ```
The resulting DNF of `<PATT>` will itself be a pattern of the form:
```
{ <BRANCH1> } or { <BRANCH2> } or ... ;
```

**Important**. The patterns `<BRANCHi>` are _unique_ up to re-ordering them. We call them the **branches** of `<PATT>`.

_Note_. The transformation also applies _inside_ `not` and `try` blocks.


### (Theory) Variables and bindings

Variables appear in statements. They fall in different categories, which can be recognized as follows.

* **Syntax**: variables in patterns `PATT` are `$`-prefixed labels
  * _Examples_: `$x`, `$y`, `$person`

#### **Variable categories**

* **Categories** indicate what type of concept a variable can hold. In a pattern `PATT`, any variable will belong to one of four _categories_ based on the following rules.

  * **Type variables** (**tvar**, uppercase convention in this spec)
    * Any variable used in a type position in a statement

  * **Value variables** (**vvar**, lowercase convention in this spec)
    * Any variable which are typed with non-comparable attribute types is a value variables (**#BDD**)
    * Any variable assigned to the output of an non-list expression 
    * Any variable derived from the output of a function (with value output type) is a value variable

  * **List variables** (**lvar**, lowercase convention in this spec)
    * Any variable typed with a list type
    * Any variable assigned to a list expression.

  * **Instance variables** (**ivar**, lowercase convention in this spec)
    * Any remaining variable must be an instance var.

... the last three together comprise _element vars_ (**evars**). Evars are those variables that can be in the signature and return statement of a function.

_Remark 1_. The code variable `$x` will be written as $`x`$ in math notation (without $`\$`$).

#### **Bound variables and valid patterns**

* **Bounds** ensure variables are tied to database concepts. (**#BDD**)

  * A variable is **bound** if it appears in a _bound position_ of at least one statement.
    * _Note_. Most statements bind their variables: this is why we will mainly highlight _non-bound positions_

  * A pattern `PATT` is **valid** if ***all variables are bound***. 
    * _(Fun fact: otherwise, we may have to solve provably unsolvable problems)_

_Going forward, we always work with valid patterns_

#### **Variable modes and unambiguous patterns**

* **Modes of variables** indicate how to compute answers for variables. In a valid pattern `PATT`, a variable appear in various _modes_ based on the following rules.

  * **Retrievable variables** are variables that are bound in _some_ statement that is _not_ `not`-gated (i.e. it is not enclosed in a not `not` block). 

  * **Negation-bound variables** are variables that are bound _only_ in statements that are `not`-gated (i.e. they are enclosed in a parent `not` block).

  * **Optional variables** are variables that are 
    * retrievable
    * are bound only in `try`-gated statements

* **Principles of unambiguity**. A valid pattern is **unambiguous** if it satifies the following.

  * _No [alpha-conversion](https://en.wikipedia.org/wiki/Lambda_calculus#%CE%B1-conversion) needed!_ No negation-bound or optional variable appears bound in **sibling blocks** in the **same branch** (see "DNF") of the pattern without also being bound in a joint parent block.
    * _Note_ This applies to `?`-marked variables, as these unpack into `try` as outlined in **IN_FUN_PATT** and **LET_FUN_PATT**

  * _No negated try's_ We dissallow `try`'s which are `not`-gated (i.e. cannot `try` inside a `not`).

#### **Anonymous variables**

* _Anon vars_: anon vars start with `$_`. They behave like normal variables, but leave the variables name implicit and are automatically discarded and results of the pattern they appear in are deduplicated. In other words:
```
[input]               // incoming stream with variables $a, $b, $c
match <PATT>          // binds $x, $y, $z, $_1
```
is equivalent to 
```
for each (a,b,c) in [input] {
  match [set $a=a, $b=b, $c=c, $_1=$var1 in <PATT>]
  deselect $var1;     // equivalent to "select $x, $y, $z"
  distinct;           // deduplicate new results
  return { $a=a, $b=b, $c=c, $x, $y, $z };  // incorporate in parent stream
}
```
(note that this is very much pseudo code, not actual TypeQL)

* _Remark_: Anon vars can be both **tvar**s and **evar**s


### (Theory) Typed concept rows

* _Concepts_. A **concept** is a type or an element in a type.
* _Typed concept rows_. An **typed concept row** (abbreviated 'crow') is a mapping of named variables (the **column names**) to _unapplied_ typed concepts (the **row entries**). A crow `r` may be written as:
  ```
  r = ($x->a:T, $y->b:S, ...)
  ```
  In math. notation $m$ can be written as: $`(x \mapsto a:T, y \mapsto b:S, ...)`$).

  To emphasize: By definition, types in crows are **unapplied** (see "Applying dependencies") 
    > In other words, the definition dissallows using types with applied dependencies like `$x -> a : T($y : I)`. Instead, we only allow `$x -> a:T` for bare symbols "T". (This is because we don't expose dependencies as such to the user)

  * _Assigned concepts_. Write `r($x)` (math. notation $`r(x)`$) for the concept that `r` assigns to `$x`.
  * _Assigned types_. Write `T($x)` (math. notation $`T_r(x)`$) for the type that `r` assigns to `$x`.
    * _Special case: assigned kinds_. Note that `T($x)` may be `Ent`, `Rel`, `Att`, `Itf` (`Rol`), or `Val` (for value types) when `$x` is assigned a type as a concept — we speak of `T($x)` as the **type kind** of `r($x)` in this case.


### Input crows for patterns

An **input crow** `r` for a pattern `PATT` is

* a crow,
* with subset of its variables marked as **input**,

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

## Pattern semantics

### Satisfaction and answers

* Given an input crow `r` for `PATT` ***satisfies*** `PATT` if

  * **Type satisfaction**. the typing assignemt of `r` satisfies the typing condition below.
  * **Pattern semantics**. the concept assignment of `r` satisfis the pattern conditions below. 
 
  > Intuitively, this means substituting the variables in `PATT` with the concepts assigned in `r` yields statements that are true in our type system. 

* A crow `r` is an **answer** to a pattern `PATT` if the following are satisfied:

    * **Retrievable domain**. Each non-input variable in `r` must be a retrievable variable of `PATT`
    * **Satisfaction**. `r` must satisfy the pattern (as outlined in next sections)
    * **Minimality**. No subset of `r` with the same input-marked variables satisfies the `PATT` 

      > In other words, `r` is a _minimal extension_ of the given input.

_Example_: Consider the pattern `$x isa Person;` (i.e. a pattern comprising a single statement). The crow `($x -> p)` satisfies the pattern if `p` is an element of the type `Person`. The crow `($x -> p, $y -> p)` also satisfies the pattern, but it is not minimal... unless `$y` was marked as an input, in which case it _does_ satisfy the pattern.
 
### Typing satisfaction

* For tvars `$X` in `PATT`, `T($X)` is a type kind (`entity`, `attribute`, `relation`, `interface`, `value`)
* For vvars `$x` in `PATT`, `T($x)` is a value type (primitive or struct)
* For lvars `$x` in `PATT`, `T($x)` is a list type `A[]` for ***minimal*** `A` a type such that `A` is the minimal upper bounds of the types of the list elements `<EL>` in the list `r($x) = [<EL>, <EL>, ...]` (note: our type system does have minimal upper bounds thanks to sums)
* For ivars `$x` in `PATT`, `T($x)` is a schema type `A` such that $`r(x) :_! A`$ isa **direct typing**

### Pattern satisfaction

#### Block-level bindings

Given a (valid and unambiguous) pattern `PATT` in DNF, we define for subpatterns

* `not { SUBPATT };`
* `try { SUBPATT };`

a variable `$x` in `SUBPATT` is **block-level** if its bound in statements that appears unnested in a branch of `SUBPATT` but not a parent block.

#### Recursive definition

We define satisfication of a pattern `PATT` in DNF by an input crow `r`. The definition recursively destructures the pattern into subpatterns `P` (to begin, set `P = PATT`). 

* If `P = STMT; SUBPATT` then `r` satisfies `P` if:
    * The statement **<STMT>** is satisfied by `r` as outlined in the _next sections_.
    * `r` satisfies `SUBPATT`;

* If `P = { SUBPATT_1 } or { SUBPATT_2 } or ... ;` then `r` satisfies `P` if:
    * `r` satisfies `SUBPATT_i` for any `i`

* If `P = not { SUBPATT };` then `r` satisfies `P` if:
    * `r` does _not contain any_ block-level bound variables of the block _except_ input vars
    * `r` cannot be completed with entries for block-level bound variable to satisfy `SUBPATT`

* If `P = try { SUBPATT };` then `r` satisfies `P` if:
    * `r` _contains all_ block-level bound variables of the block
    * and:
        * either: `r` satisfies `SUBPATT`
        * or:
            * all block-level bound variables are assigned $`\emptyset`$ (i.e. empty concept) by `r` _except_ input vars
            * after obtain `r'` from `r` by removing block-level bound, non-input variables, `r'` cannot be completed with entries for block-level bound variable to satisfy `SUBPATT`

### `Let` declarations in patterns

_System property_

The keyword `let` has two special properties:

* 🔶 `let` assignments must be acyclic
* 🔶 No variable can be `let` assigned twice

## Satisfaction semantics of...

We discuss the satisfaction semantics of various classes of statements.

_Math. notation (Replacing **var**s with concepts)_. When discussing pattern semantics, we always consider **fully variablized** statements (e.g. `$x isa $X`, `$X sub $Y`). This also determines satisfaction of **partially assigned** versions of these statements (e.g. `$x isa A`, `$X sub A`, `A sub $Y`, or `x isa $A`).

## ... Function statements

### **Case LET_FUN_PATT**

_Remark_ the following can be said in less space, but we chose the more principled longer route, via "single returns".

* 🔶 `let _, ..., _, $x, _, ..., _ = f($a, $b, ...)` (where `$x` is in $i$th position of the comma-separated list of length $n$, and all other positions are "blanks" `_`). This statement is satisfied if:
    * denoting *function evalution* (see "Function evaluation") with inputs from the crow `r` by $`ev(f(r(a), r(b), ...)`$,
    * $`\mathsf{ev}(f(r(a), r(b), ...)`$ contains exactly one row $`w`$ of length $n$ (if it contains no row we reject)
    * $`r(x)`$ is the $i$th entry of $`w`$ (corresponding to `$x` being used in $`i`$th position on the LHS)

    _Note_. This is equivalent to `$x in F_i($a, $b, ...)` where `F_i` modifies `F` with an additional selection of the `i`th variable.

* 🔶 `let $x, $y?, ..., $w = f($a, $b, ...)` is satisfied if the following pattern is satisfied:
    ```
    let $x,_,...,_ in F($a, $b, ...);               // first var (non-optional!)
    try { let _, $y, ...,_ in F($a, $b, ...); };    // second var (optional!)
    ...                                             // ...
    let _, _, ...,$w in F($a, $b, ...);             // last var
    ```
    
    _Note_. `?`-marked variables are retrieved with **separate** `try`-blocks.

* 🔶 `let $x, $y?, ..., $w = F($a, <EXPR_b>, ...)>` is satisfied if the following pattern is satisfied:
    ```
    let $_b = <EXPR_b>;
    ...
    let $x, $y?, ..., $w in F($a, $_b, ...)
    ```

_System property_

* 🔶 _Output type_ Function call must be to a **single-return** function, i.e. have output type `T, ...`.
* 🔶 _Boundedness_ All variable arguments (or variables in expression arguments) to `f` must be set in the crow `r` (i.e. should be bound somewhere else in the pattern).
* 🔶 _Acyclicity (pattern-level constraint)_ All let statements must be acyclic, e.g. we **cannot have**
    ```
    let $x = f($y); 
    let $y = f($x);
    ```

### **Case LET_IN_FUN_PATT**

* 🔶 `let $x, $y?, ..., $w in F($a, <EXPR_b>, ...)>` is satisfied if, for some choice of row $`w \in \mathsf{ev}(F(r(a),v_r(expr_b),...)`$ in the evaluation set of the function call such that the following pattern is satisfied:
    ```
    let $x, $y?, ..., $w = f($a, <EXPR_b>, ...)
    ```

    where `f($a, <EXPR_b>, ...)` denotes a single-return function call ***defined*** to evaluate to row `w`.

_System property_

* 🔶 _Output type_ Function call must be to a **stream-return** function, i.e. have output type `{ T, ... }`.
* 🔶 _Boundedness_ All variable arguments (or variables in expression arguments) to `F` must be set in the crow `r` (i.e. should be bound somewhere else in the pattern).
* 🔶 _Acyclicity (pattern-level constraint)_ All let statements must be acyclic, e.g. we **cannot have**
    ```
    let $x in F($y); 
    let $y in F($x);
    ```

## ... Type statements

### **Case TYPE_DEF_PATT**
* ✅ `Kind $A` (for `Kind` in `{entity, relation, attribute}`) is satisfied if $`r(A) : \mathbf{Kind}`$

* ✅ `(Kind) $A sub $B` is satisfied if $`r(A) : \mathbf{Kind}`$, $`r(B) : \mathbf{Kind}`$, $`r(A) \lneq r(B)`$
* ✅  `(Kind) $A sub! $B` is satisfied if $`r(A) : \mathbf{Kind}`$, $`r(B) : \mathbf{Kind}`$, $`r(A) <_! r(B)`$

_Remark_: `sub!` is convenient, but could actually be expressed with `sub`, `not`, and `is`. Similar remarks apply to **all** other `!`-variations of TypeQL key words below.

### **Case REL_PATT**
* ✅ `$A relates $I` is satisfied 
    * either if $`r(A) : \mathbf{Rel}(r(I))`$
    * or if $`\diamond(r(A) : \mathbf{Rel}(r(I)))`$

* ✅ `$A relates $I as $J` is satisfied if 
    * $`r(A) : \mathbf{Rel}(r(I))`$ or $`\diamond(r(A) : \mathbf{Rel}(r(I)))`$ 
    * there exists $`B : \mathbf{Rel}(r(J))`$ or $`\diamond(B : \mathbf{Rel}(r(J)))`$
    * $`A \leq B`$ and $`r(I) \leq r(J)`$.

* 🔶 `$A relates $I[]` is satisfied if $`r(A) : \mathbf{Rel}(r([I]))`$ and
    * either if $`r(A) : \mathbf{Rel}([r(I)])`$
    * or if $`\diamond(r(A) : \mathbf{Rel}([r(I)]))`$

* 🔶 `$A relates $I[] as $J[]` is satisfied if 
    * $`r(A) : \mathbf{Rel}(r([I]))`$ or $`\diamond(r(A) : \mathbf{Rel}(r([I]))`$
    * there exists $`B : \mathbf{Rel}(r([J]))`$ or $`\diamond(B : \mathbf{Rel}(r([J])))`$ 
    * $`A \leq B`$ and $`r(I) \leq r(J)`$.

### **Case DIRECT_REL_PATT**

* 🔮 `$A relates! $I` is satisfied if 
    * $`r(A) : \mathbf{Rel}(r(I))`$ and **not** $`r(A) \lneq B : \mathbf{Rel}(r(I))`$
    * or if $`\diamond(r(A) : \mathbf{Rel}(r(I)))`$ and **not** $`\diamond(r(A) \lneq B : \mathbf{Rel}(r(I)))`$

* 🔮 `$A relates(!) $I as! $J` is satisfied if `$A relates(!) $I` and 
    * there exists $`B : \mathbf{Rel}(r(J))`$ or $`\diamond(B : \mathbf{Rel}(r(J)))`$
    * $`A <_! B`$ and $`r(I) <_! r(J)`$

* 🔮 `$A relates! $I[]` is satisfied if 
    * $`r(A) : \mathbf{Rel}(r([I]))`$ and **not** $`r(A) \lneq r(B) : \mathbf{Rel}(r([I]))`$
    * or if $`\diamond(r(A) : \mathbf{Rel}(r([I])))`$ and **not** $`\diamond(r(A) \lneq r(B) : \mathbf{Rel}(r([I])))`$

* 🔮 `$A relates $I[] as! $J[]` is satisfied if `$A relates(!) $I[]` and
    * there exists $`B : \mathbf{Rel}(r([J]))`$ or $`\diamond(B : \mathbf{Rel}(r([J])))`$ 
    * $`A <_! B`$ and $`r(I) <_! r(J)`$

### **Case PLAYS_PATT**

* ✅ `$A plays $I` is satisfied if
    * either $`r(A) \leq A'`$ and $`A' <_! r(I)`$
    * or if $`r(A) \leq A'`$ and $`\diamond(A' <_! r(I))`$

### **Case DIRECT_PLAYS_PATT**

* 🔮 `$A plays! $I` is satisfied if $`r(A) <_! r(I)`$
    * $`A <_! r(I)`$
    * _(to match `@abstract` for `plays!` must use annotation, see **PLAYS_ABSTRACT_PATT**)_

### **Case VALUE_PATT**

* ✅ `$A value $V` is satisfied if $`r(A) \leq A'$ and $\mathrm{val} : A' \to r(V)$

### **Case OWNS_PATT**

* ✅ `$A owns $B` is satisfied if 
    * either $`r(A) \leq A'`$ and $`A' <_! O_{r(B)})`$ 
    * or $`r(A) \leq A'`$ and $`\diamond(A' <_! O_{r(B)})`$

* 🔶 `$A owns $B[]` is satisfied if $`r(A) \leq A' <_! r(O_B)`$ (for $`A'`$ **not** an interface type)
    * either $`r(A) \leq A'`$ and $`A' <_! r(O_{B[]})`$ 
    * or $`r(A) \leq A'`$ and $`\diamond(A' <_! r(O_{B[]}))`$

_Remark_. In particular, if `A owns B[]` has been declared, then `$X owns B` will match the answer `r($X) = A`.

### **Case DIRECT_OWNS_PATT**

* 🔮 `$A owns! $B` is satisfied if 
    * $`r(A) <_! r(O_B)`$ 
    * or $`\diamond(r(A) <_! r(O_B))`$ 

* 🔮 `$A owns! $B[]` is satisfied if $`r(A) <_! r(O_B)`$
    * $`r(A) <_! r(O_B)`$ 
    * or if $`\diamond(r(A) <_! r(O_B))`$

### **Cases TYP_IS_PATT and LABEL_PATT**
* 🔷 `$A is $B` is satisfied if $`r(A) = r(B)`$ (this is actually covered by the later case `IS_PATT`)
* 🔷 `$A label <LABEL>` is satisfied if $`r(A)`$ has **primary label or alias** `<LABEL>`

## ... Type constraint statements

### Cardinality

_To discuss: the usefulness of constraint patterns seems overall low, could think of a different way to retrieve full schema or at least annotations (this would be more useful than, say,having to find cardinalities by "trialing and erroring" through matching)._

#### **Case CARD_PATT**
* ✅ cannot match `@card(n..m)` (STICKY: there's just not much point to do so ... rather have normalized schema dump. discuss `@card($n..$m)`??)
<!-- 
* `A relates I @card(n..m)` is satisfied if $`r(A) : \mathbf{Rel}(r(I))`$ and schema allows $`|a|_I`$ to be any number in range `n..m`.
* `A plays B:I @card(n..m)` is satisfied if ...
* `A owns B @card(n...m)` is satisfied if ...
* `$A relates $I[] @card(n..m)` is satisfied if ...
* `$A owns $B[] @card(n...m)` is satisfied if ...
-->

### Modalities

#### **Case UNIQUE_PATT**
* 🔶 `$A owns $B @unique` is satisfied if $`r(A) \leq A' <_! r(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns r($B) @key`.

* 🔶 `$A owns! $B @unique` is satisfied if $`r(A) <_! r(O_B)`$, and schema directly contains constraint `r($A) owns r($B) @unique`.

#### **Case KEY_PATT**
* 🔶 `$A owns $B @key` is satisfied if $`r(A) \leq A' <_! r(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns r($B) @key`.

* 🔶 `$A owns! $B @key` is satisfied if $`r(A) <_! r(O_B)`$, and schema directly contains constraint `r($A) owns r($B) @key`.

#### **Case SUBKEY_PATT**
* 🔶 `$A owns $B @subkey(<LABEL>)` is satisfied if $`r(A) \leq A' <_! r(O_B)`$ (for $`A'`$ **not** an interface type), and schema directly contains constraint `A' owns r($B) @subkey(<LABEL>)`.

#### **Case TYP_ABSTRACT_PATT**
* 🔶 `(kind) $B @abstract` is satisfied if schema directly contains `(kind) r($B) @abstract`.

#### **Case REL_ABSTRACT_PATT**
* 🔶 `$B relates $I @abstract` is satisfied if:
    * $`r(B) \leq B'`$ and $`\diamond(B' : \mathbf{Rel}(I)`$
    * **not** $`r(B) \leq B'' \leq B'`$ such that $`\diamond(B' : \mathbf{Rel}(I)`$

* 🔮 `$B relates! $I @abstract` is satisfied if $`\diamond(r(B) : \mathbf{Rel}(I)`$

* 🔶 `$B relates $I[] @abstract` is satisfied if:
    * $`r(B) \leq B'`$ and $`\diamond(r(B) : \mathbf{Rel}([I])`$
    * **not** $`r(B) \leq B'' \leq B'`$ such that $`B' : \mathbf{Rel}([I])`$

* 🔮 `$B relates! $I[] @abstract` is satisfied if $`\diamond(r(B) : \mathbf{Rel}([I])`$

#### **Case PLAYS_ABSTRACT_PATT**
* 🔶 `$A plays $B:$I @abstract` is satisfied if: 
    * $`r(A) \leq A'`$ and $`\diamond(A' <_! r(I))` 
    * **not** $`r(A) \leq A'' \leq A'`$ such that $`A'' <_! r(I)`$ 

  where $`r(B) \mathbf{Rel}(r(I))`$

* 🔮 `$A plays! $B:$I @abstract` is satisfied if $`\diamond(r(A) <_! r(I))`, where $`r(B) \mathbf{Rel}(r(I))`$

#### **Case OWNS_ABSTRACT_PATT**
* 🔶 `$A owns $B @abstract` is satisfied if
    * $`r(A) \leq A'`$ and $`\diamond(A' <_! O_{r(B)})`$
    * **not** $`r(A) \leq A'' \leq A'`$ such that $`A'' <_!  O_{r(B)})`$

* 🔮 `$A owns! $B @abstract` is satisfied if $`\diamond(r(A) <_! O_{r(B)})`

* 🔶 `$A owns $B[] @abstract` is satisfied if
    * $`r(A) \leq A'`$ and $`\diamond(A' <_! O_{r(B)[]})`$
    * **not** $`r(A) \leq A'' \leq A'`$ such that $`A'' <_!  O_{r(B)[]})`$

* 🔮 `$A owns! $B[] @abstract` is satisfied if $`\diamond(r(A) <_! O_{r(B)[]})`

#### **Case DISTINCT_PATT**
* 🔮 `$A owns $B[] @distinct` is satisfied if $`r(A) \leq A`$ schema directly contains constraint `A' owns r($B)[] @distinct`.
* 🔮 `$A owns! $B[] @distinct` is satisfied if schema directly contains constraint `r($A) owns r($B)[] @distinct`.
* 🔮 `$B relates $I[] @distinct` is satisfied if $`r(B) : \mathbf{Rel}(r([I]))`$, $`B \leq B'`$ and schema directly contains `B' relates r($I)[] @distinct`.
* 🔮 `$B relates! $I[] @distinct` is satisfied if schema directly contains `r($B) relates r($I)[] @distinct`.

### Values constraints

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

## ... Element statements

### **Case ISA_PATT**
* ✅ `$x isa $T` is satisfied if $`r(x) : r(T)`$ for $`r(T) : \mathbf{ERA}`$
* 🔶 `$x isa $T ($I: $y)` is equivalent to `$x isa $T; $x links ($I: $y);`
* 🔶 `$x isa $T <EXPR>` is equivalent to `$x isa $T; $x == <EXPR>;`

### **Case ANON_ISA_PATT**
* 🔶 `$T` is equivalent to `$_ isa $T`
* 🔶 `$T ($R: $y, ...)`  is equivalent to `$_ isa $T ($R: $y, ...)`
* 🔶 `$T <EXPR>` is equivalent to `$_ isa $T <EXPR>`

### **Case DIRECT_ISA_PATT**

* ✅ `$x isa! $T` is satisfied if $`r(x) :_! r(T)`$ for $`r(T) : \mathbf{ERA}`$
* 🔶 `$x isa! $T ($I: $y)` is equivalent to `$x isa! $T; $x links ($I: $y);`
* 🔶 `$x isa! $T <EXPR>` is equivalent to `$x isa! $T; $x == <EXPR>;`

### **Case LINKS_PATT**
* ✅ `$x links ($I: $y)` is satisfied if $`r(x) : A(r(y):r(I))`$ for some $`A : \mathbf{Rel}(r(I))`$.
* ✅ `$x links ($y)` is equivalent to `$x links ($_: $y)` for anonymous `$_` (See "Syntactic Sugar")


### **Case LINKS_LIST_PATT**
* 🔶 `$x links ($I[]: $y)` is satisfied if $`r(x) : A(r(y):[r(I)])`$ for some $`A : \mathbf{Rel}([r(I)])`$.
* 🔶 `$x links ($I[]: <LIST_EXPR>)` is equivalent to `$x links ($I[]: $_y); $_y == <LIST_EXPR>;` for anonymous `$_y`

### **Case DIRECT_LINKS_PATT**
* 🔮 `$x links! ($I: $y)` is satisfied if $`r(x) :_! A(r(y):r(I))`$ for some $`A : \mathbf{Rel}(r(I))`$.
* 🔮 `$x links! ($I[]: $y)` is satisfied if $`r(x) :_! A(r(y):[r(I)])`$ for some $`A : \mathbf{Rel}([r(I)])`$.

### **Case HAS_PATT**
* ✅ `$x has $B $y` is satisfied if $`r(y) : r(B)(r(x):O_{r(B)})`$ for some $`r(B) : \mathbf{Att}`$.
* ✅ `$x has $B == <VAL_EXPR>` is equivalent to `$x has $B $_y; $_y == <VAL_EXPR>` for anonymous `$_y` (see "Expressions")
* ✅ `$x has $B <NV_VAL_EXPR>` is equivalent to  `$x has $B == <NV_VAL_EXPR>` (see "Expressions"; `NV_EXPR` is a "non-variable expression")
* ✅ `$x has $y` is equivalent to `$x has $_ $y` for anonymous `$_`

_Remark_. Note that `$x has $B $y` will match the individual list elements of list attributes (e.g. when $`r(x) : A`$ and $`A <_! O_B`$).

### **Case HAS_LIST_PATT**

* 🔶 `$x has $B[] $y` is satisfied if $`r(y) : [r(B)](r(x):O_{r(B[])})`$ for some $`r(B) : \mathbf{Att}`$.
* 🔶 `$x has $B[] == <LIST_EXPR>` is equivalent to `$x has $B[] $_y; $_y == <LIST_EXPR>` for anonymous `$_y`
* 🔶 `$x has $B[] <NV_LIST_EXPR>`is equivalent to  `$x has $B[] == <NV_VAL_EXPR>`.

### **Case DIRECT_HAS_PATT**

* 🔮 `$x has! $B $y` is satisfied if $`r(y) :_! r(B)(r(x):O_{r(B)})`$ for some $`r(B) : \mathbf{Att}`$.
* 🔮 `$x has! $B[] $y` is satisfied if $`r(y) :_! [r(B)](r(x):O_{r(B[])})`$ for some $`r(B) : \mathbf{Att}`$.

### **Case IS_PATT**
* 🔷 `$x is $y` is satisfied if:
    * `$x` and `$y` are both **ivars** and: $`r(x), r(y) :_! A`$ and $`r(x) = r(y)`$ for $`A : \mathbf{ERA}`$
    * `$x` and `$y` are both **lvars** and: $`r(x), r(y) : [A]`$ and $`r(x) = r(y)`$ for (sum type) $`A = \sum_i A_i`$, $`A_i : \mathbf{ERA}`$ (**#BDD**)
    * `$x` and `$y` are both **tvars** and: $`r(x), r(y) : \mathbf{ERA}`$ and $`r(x) = r(y)`$ (**#BDD**)

_System property_

1. 🔷 In the `is` pattern, neither left nor right variables are **not bound**.

_Remark_: In the `is` pattern we cannot syntactically distinguish whether we are in the "type" or "element" case (it's the only such pattern where tvars and evars can be in the same position!) but this is alleviated by the pattern being non-binding, i.e. we require further statements which bind these variables, which then determines them to be tvars are evars.

## ... Expression and list statements

### Expressions grammar

_Expression composition_

```javascript
BOOL        ::= VAR | bool                                     // VAR = variable
INT         ::= VAR | long | ( INT ) | INT (+|-|*|/|%) INT 
                | (ceil|floor|round)( DBL ) | abs( INT ) | len( T_LIST )
                | (max|min) ( INT ,..., INT )
DBL         ::= VAR | double | ( DBL ) | DBL (+|-|*|/) DBL 
                | (max|min) ( DBL ,..., DBL ) |                
DEC         ::= VAR | dec | ...                                // similar to DBL
STRING      ::= VAR | string | string + string
DUR         ::= VAR | time | DUR (+|-) DUR 
DATE        ::= VAR | datetime | DATETIME (+|-) DUR 
DATETIME    ::= VAR | datetime | DATETIME (+|-) DUR 
PRIM        ::= <any-expr-above>
STRUCT      ::= VAR | { <COMP>: (value|VAR|STRUCT), ... }      // <COMP> = struct component
                | <HINT> { <COMP>: (value|VAR|STRUCT), ... }   // <HINT> = struct label
DESTRUCT    ::= { T_COMP: (VAR|VAR?|DESTRUCT), ... }           
VAL         ::= PRIM | STRUCT |                   
<T>         ::= <T> | <T>_LIST [ INT ]                         // T : Val
                | <T_FCALL>                                    // fun call returning T/T?
                | STRUCT.<T_COMP>                              // component of type T/T?
<T>_LIST    ::= VAR | [ <T> ,..., <T> ] | <T>_LIST + <T>_LIST  // includes empty list []
                T_LIST [ INT .. INT ]
INT_LIST    ::= INT_LIST | [ INT .. INT ]
VAL_EXPR    ::= <T> | STRUCT                                   // "value expression"
LIST_EXPR   ::= <T>_LIST | INT_LIST                            // "list expression"
EXPR        ::= VAL_EXPR | LIST_EXPR
```

As a special case, consider expression that are not single variables (i.e., exclude `$x` but allow `($x + 1)` or even `($x)`)
```
NV_VAL_EXPR ::= VAL_EXPR - VAR                                 // exclude sole variable
NV_LIST_EXPR::= LIST_EXPR - VAR                                // exclude sole variable
NV_EXPR     ::= EXPR - VAR                                     
```

_Value formats_

```
  long       ::=   (0..9)*
  double     ::=   (0..9)*\.(0..9)+
  dec        ::=   (0..9)*\.(0..9)+dec
  date       ::=   ___Y__M__D
  datetime   ::=   ___Y__M__DT__h__m__s
                 | ___Y__M__DT__h__m__s:___
  datetimetz ::= ...
  duration   ::=   P___Y__M__D              
                 | P___Y__M__DT__h__m__s
                 | P___Y__M__DT__h__m__s:___
```

_Remarks_

* Careful: the generic case `<T>` modify earlier parts of the 
* `T`-functions (`T_FUN`) are function calls to *single-return* functions with non-tupled output type `T` or `T?`

_Explicit casts_. 🔮 Introduce explicit castings between types to our grammar. For example:
* `long(1.0) == 1` 
* `double(10) / double(3) == 3.3333`)

### Expression evaluation

Given a crow `r` that assign all vars in an `<EXPR>` we define
* value evaluation `vev@r(<EXPR>)` (math. notation $`v_r(expr)`$)
* type evaluation `Tev@r(<EXPR>)` (math. notation $`T_r(expr)`$)]

as follows. First note that we can unambiguously distinguish **value** from **list** expressions in our grammar. We evaluate each of those as follows.

#### Value expressions

* 🔶 The _value expressions_ `VAL_EXPR` is evaluated as follows:
    * **Substitute** all vars `$x` by `r($x)`
    * If `r($x)` isa attribute instance, **replace** by `val(r($x))`
    * $`v_r(expr)`$ is the result of evaluating all operations with their **usual semantics** 
        * `1 + 1 == 2` 
        * `10 / 3 == 3` (integer division satisfies `p/q + p%q = p`)
    * $`T_r(expr)`$ is the **unique type** of the substituted expression, noting:
        * We allow **implicit casts** of `long -> dec -> double`. 
        * This is always unique except possibly for the `STRUCT` case (see property below)!

_System property_.

* 🔶 If $`T_r(expr)`$ is non-unique for a `STRUCT` expression (which may be the case because, `STRUCT` may share fields) we require the expression to have a `HINT`, or otherwise throw an error (***see Grammar above***, case `STRUCT`).

_Remark_. Struct values are semantically considered up to reordering their components.

#### List expressions

* 🔶 The _list expressions_ `LIST_EXPR` is evaluated as follows:
    * Substitute all vars `$x` by `r($x)`
    * (**Do not replace** attributes!)
    * $`v_r(expr)`$ is the result of concatenation and sublist operations with their **usual semantics**
        * e.g. `[a] + [a,b,c][1..2] = [a,b,c]` (`[1..2]` includes indices `[1,2]`)
        * or `([a] + [a,b,c])[1..2] = [a,b]`
    * $`T_r(expr)`$ is the **minimal type** of all the list elements (usually some sum type)

**Note**: While the type checker cannot statically determine $`T_r(expr)`$, it can statically construct an upper bound of that type.

### (Feature) Boundedness of variables in expressions

1. 🔶 Generally, variables in expressions `<EXPR>` are **never bound**, except ...
    * 🔶 the exception are **single-variable list indices**, i.e. `$list[$index]`; in this case `$index` is bound. (This makes sense, since `$list` must be bound elsewhere, and then `$index` is bound to range over the length of the list) (**#BDD**)
3. 🔶 Struct components are considered to be unordered: i.e., `{ x: $x, y: $y}` is equal to `{ y: $y, x: $x }`.

_Remark_: The exception for list indices is mainly for convenience. Indeed, you could always explicitly bind `$index` with the pattern `$index in [0..len($list)-1];`. See "Case **LET_IN_LIST_PATT**" below.

### Simple expression patterns

#### **Case LET_PATT**
* 🔷 `let $x = <EXPR>` is satisfied if **both** 
    * `r($x)` equals $`v_r(expr)`$
    * `T($x)` equals $`T_r(expr)`$

_System property_

1. _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Assign once, to vars only_. Any variable can be assigned only once within a pattern—importantly, the left hand side _must be_ a variable (replacing it with a concept will throw an error; this implicitly applies to "Match semantics").
3. _Acyclicity (pattern-level constraint)_. All let statements must be acyclic, i.e. the graph of variables with directed edges from RHS vars to LHS vars in let statements is acyclic (e.g. we cannot have `$x = $x + $y; $y = $y - $x;`)

#### **Case LET_DESTRUCT_PATT**
* 🔶 `let <DESTRUCT> = <STRUCT>` is satisfied if, after substituting concepts from `r`, the left hand side (up to potentially omitting components whose variables are marked as optional) matched the structure of the right and side, and each variable on the left matches the evaluated expression of the correponding position on the right.

_System property_

1. 🔶 _Assignments bind_. The left-hand variable is bound by the pattern.
2. _Acyclicity (pattern-level constraint)_. All let statements must be acyclic, i.e. the graph of variables with directed edges from RHS vars to LHS vars in let statements is acyclic.

#### **Case EQ_PATT**
* ✅ `<EXPR1> == <EXPR2>` is satisfied if $`v_r(expr_1) = v_r(expr_2)`$
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

### **Case LET_IN_LIST_PATT**
* 🔷 `let $x in $l` is satisfied if $`r(x) \in r(l)`$
* 🔶 `let $x in <LIST_EXPR>` is equivalent to `$l = <LIST_EXPR>; $x in $l` (see "Syntactic Sugar") 

_System property_

1. The right-hand side variable(s) of the pattern are **not bound**. (The left-hand side variable is bound.)
1. _Acyclicity (pattern-level constraint)_.  All let statements must be acyclic, i.e. the graph of variables with directed edges from RHS vars to LHS vars in let statements is acyclic.

### **Case LIST_CONTAINS_PATT**
* 🔶 `$l contains $x` is satisfied if $`r(x) \in r(l)`$
* 🔶 `<LIST_EXPR contains $x` is equivalent to `$l = <LIST_EXPR>; $l contains $x` (see "Syntactic Sugar") 

_System property_

1. _Boundedness_ no side of the patterns is binding (this is in effect a comparator).


# Data manipulation language

## Match semantics

* _Syntax_ The `match` clause has syntax
    ```
    match <PATTERN>
    ```

* _Input crows_: The clause can take as input a stream `{ r }` of concept rows `r`.

* _Output crows_: For each `r`: 
  * replace all patterns in `PATT` with concepts from `r`. 
  * Compute the **set** of answers `{ r' }`. 
  * The final output stream will be `{ (r,r') }`.


## Function semantics

### Function signature, body, operators

#### **Case SIGNATURE_STREAM_FUN**

* 🔶 Stream-return function signature syntax:
    _Syntax_:
    ```
    fun F ($x: A, $y: B[]) -> { C, D[], E? } :
    ```
    where
    * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_Remark: see GH issue on trais (Could have `A | B | plays C` input types)._

_Terminology_ If the function returns a single types (`{ C }`) then we call it a **singleton** function.

#### **Case SIGNATURE_SINGLE_FUN**

* 🔶 Single-return function signature syntax:
    ```
    fun F ($x: A, $y: B[]) -> C, D[], E? :
    ```
    where
    * types `A, B, C, D, E` can be available entity, relation, attribute, value types (both structure and primitive).

_STICKY: allow types to be optional in args (this extends types to sum types, interface types, etc.)_

_Terminology_ If the function returns a single types (`C`) then we call it a **singleton** function.

#### **Case READ_PIPELINE_FUN**

* 🔶 Function body syntax:
    _Syntax_:
    ```
    <READ_PIPELINE>
    ```

_System property_

* 🔶 _Read only_. Pipeline must be read-only, i.e. cannot use write clauses (`insert`, `delete`, `update`, `put`)
* 🔶 _Require crow stream output_ Pipeline must be non-terminal (e.g. cannot end in `fetch`).

#### **Case RETURN_STREAM_FUN**

* 🔶 Function return syntax:
    _Syntax_:
    ```
    return { $x, $y, ... };
    ```
_System property_

* 🔶 _Require bindings_ all vars (`$x`, `$y`, ...) must be bound in the pipeline (taken into account any variable selections through `select` and `reduce` operators).

#### **Case RETURN_SINGLE_FUN**

* 🔶 Function body syntax:
    _Syntax_:
    ```
    return <SINGLE> $x, $y, ...;
    ```
    where `<SINGLE>` can be:
    * `first`
    * `last`
    * `random`

_System property_

* 🔶 _Require bindings_ all vars (`$x`, `$y`, ...) must be bound in the pipeline (taken into account any variable selections through `select` and `reduce` operators).

#### **Case AGG_RETURN_FUN**

* 🔶 The syntax 
    _Syntax_:
    ```
    return <AGG>, ..., <AGG>;
    ```
    is short-hand for:
    ```
    return $_1 = <AGG>, ..., $_n = <AGG>;
    return first $_1, ..., $_n;
    ```

### (Theory) Function semantics

***Typing***

* `?` marks variables in the row that can be optional
  * this applies both to output types: `{ A?, B }` and `A?, B`
  * and it applies to variables assignments: `$x?, $y in ...` and `$x?, $y = ...`
* **single-row returning** functions return 0 or 1 rows
* **multi-row returning** ("stream-returning") functions return 0 or more rows.

***Matching***

* single row assignment `$x, $y = ...`  fails if no row is assigned
* single row assignment `$x?, $y = ...`  succeeds if a row is assigned, even if it may be missing the optional variables
* multi row assignment `$x, $y in ...`  fails if no row is assigned
* multi row assignment `$x?, $y in ...`  succeeds if one or more row are assigned, even if they may be missing the optional variables

***Evaluation***

* A function `F` counts as ***evaluated*** on a call `F_CALL` when we completely computed its output stream as follows:
    1. provide input arguments from the call `F_CALL` as a single crow, which is the starting point of the body `READ_PIPELINE`
    2. Then act like an ordinary pipeline, outputting a stream (see "Pipelines")
    3. perform `return` transformation outlined below for final output which is effectively a `select`.
        4. For **single(-row) return** functions we first pick the **first**, **last** or a **random** crow in the stream, making the final output an at-most-single-row output
    4. Denote the output stream by `ev(F_CALL)`
* When negations are use, function in lower strata must be evaluated (on all relevant calls) before function in higher strata (see below)

### (Theory) Order of execution (and recursion)

Since functions can only be called from `match` stages in pipelines, evaluation is deterministic and does not depend on any choices of execution order (i.e. which statements in the pattern we retrieve first), except for **negation**: here, execution order _does_ matter.

* 🔮 **Recursion** Functions can be called recursively, as long as negation can be **stratified**:

    * The set of all defined functions is divided into groups called "strata" which are ordered
    * If a function `F` calls a function `G` if must be a in an equal or higher stratum. Moreover, if `G` appears behind an odd number of `not { ... }` in the body of `F`, then `F` must be in a strictly higher stratum.

    _Note_: The semantics in this case is computed "stratum by stratum" from lower strata to higher strata. New facts in our type systems ($`t : T`$) are derived in a bottom-up fashion for each stratum separately.


## Insert behavior

### Basics of inserting

An `insert` clause comprises collection of _insert statements_

* _Input crow_: The clause can take as input a stream `{ r }` of concept rows `r`, in which case 
  * the clause is **executed** for each row `r` in the stream individually

* _Extending input row_: Insert clauses can extend bindings of the input concept row `r` in two ways
  * `$x` is the subject of an `isa` statement in the `insert` clause, in which case $`r(x) =`$ _newly-inserted-concept_ (see "Case **ISA_INS**")
  * `$x` is the subject of an `let` assignment statement in the `insert` clause, in which case $`r(x) =`$ _assigned-value_ (see "Case **LET_INS**")

#### (Theory) Execution

_Execution_: An `insert` clause is executed by executing its statements individually.
* Not all statement need to execute (see Optionality below)
  * **runnable** statements will be executed
  * **skipped** statements will not be executed
* The order of execution is arbitrary except for:
  1. We always execute all `let` assignments before the runnable statements that use the assigned variables variables. (There is an acyclicity condition that guarantees this is possible).
* Executions of statements will modify the database state by 
  * adding elements
  * refining dependencies
* (Execution can also affect the state of concept row `r` as mentioned above)
* Modification are buffered in transaction (see "Transactions")
* Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

#### (Feature) Optionality

* 🔶 **Optionality**: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `insert` clauses cannot be nested
  * variables in `try` are variables are said to be **determined** if
    * they are determined outside the block, i.e.:
        * they are assigned in the crow `r`
        * they are subjects of `isa` or `let` statements outside the block
    * they are subjects of `isa` or `let` statements inside the block
  * If any variable in the `try` block is _not_ determined, then `try` block statement is **skipped** (i.e. **not executed**)
  * If all variables are , the `try` block `isa` statements are marked **runnable**.
  * All variables outside of a `try` block must be bound outside of that try block (in other words, variable in a block bound with `isa` cannot be used outside of the block)

### Insert statements

#### **Case LET_INS**
* 🔷 `let $x = <EXPR>` adds nothing to the DB, and but sets $`r(x) = v_r(expr)`$ in the in put crow

_System property_:

1. 🔷 `$x` cannot be insert-bound elsewhere (i.e. no other `isa` or `let`)
2. 🔷 _Acyclicity_ All `isa` or `let` statements must be acyclic. For example we cannot have:
    ```
    insert
      let $x = $y;
      $y isa name $x;
    ```
3. 🔷 _No reading_ `<EXPR>` cannot contain function calls.

_Note_. All **EXPR_INS** statements are executed first as described in the previous section.

#### **Case ISA_INS**
* ✅ `$x isa $T` adds new $`a :_! r(T)`$, $`r(T) : \mathbf{ERA}`$, and sets $`r(x) = a`$
* 🔶 `$x isa $T ($R: $y, ...)` adds new $`a :_! r(T)(r(y):r(R))`$, $`r(T) : \mathbf{Rel}`$, and sets $`r(x) = a`$
* 🔶 `$x isa $T <EXPR>` adds new $`a :_! r(T)`$, $`r(T) : \mathbf{Att}`$, and sets $`r(x) = a`$ and $`\mathsf{val}(a) = v_r(expr)`$

_System property_:

1. ✅ `$x` cannot be bound elsewhere (i.e. `$x` cannot be bound in the input row `r` nor in other `isa` or `let` statements).
1. 🔮 `<EXPR>` must be of the right value type, and be evaluatable (i.e. all vars are bound).
1. 🔶 In the last case, `r(T)` must be an independent attribute, i.e. the schema must contain `attribute r(T) (sub B) @indepedent`

#### **Case ANON_ISA_INS**

* 🔶 `$T` is equivalent to `$_ isa $T`
* 🔶 `$T ($R: $y, ...)`  is equivalent to `$_ isa $T ($R: $y, ...)`
* 🔶 `$T <EXPR>` is equivalent to `$_ isa $T <EXPR>`

#### **Case LINKS_INS** 

* ✅ `$x links ($I: $y)` replaces $`r(x) :_! A(a : J, b : K, ...)`$ by $`r(x) :_! A(r(y)a : r(I), b : K, ...)`$

_Remark_. Set semantics for interfaces means that inserts become idempotent when inserting the same role players twice.

_System property_:

1. ✅ _Capability check_. 
    * Must have $`T(x) \leq B : \mathbf{Rel}(r(I))`$ **non-abstractly**, i.e. $`\diamond (B : \mathbf{Rel}(r(I)))`$ is not true for the minimal choice of $`B`$ satisfying the former
    * Must have $`T(y) \leq B <_! r(I)`$ **non-abstractly**, i.e. $`\diamond (B <_! r(I))`$ is not true for the minimal $`B`$ satisfying the former.

#### **Case LINKS_LIST_INS** 
* 🔶 `$x links ($I[]: <T_LIST>)` replaces $`r(x) :_! A()`$ by $`r(x) :_! A(l : [r(I)])`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

_System property_:

1. 🔶 _System cardinality bound: **1 list per relation per role**_. Transaction will fail if $`r(x) :_! A(...)`$ already has a roleplayer list. (In this case, user should `update` instead!)
1. 🔶 _Capability check_. 
    * Must have $`T(x) \leq B : \mathbf{Rel}(r(I))`$ **non-abstractly**, i.e. $`\diamond (B : \mathbf{Rel}(r(I)))`$ is not true for the minimal choice of $`B`$ satisfying the former
    * Must have $`l_i : T_i \leq B <_! r(I)`$ **non-abstractly**, i.e. $`\diamond (B <_! r(I))`$ is not true for the minimal $`B`$ satisfying the former.

#### **Case HAS_INS**
* ✅ `$x has $A $y` adds new $`a :_! r(A)(r(x) : O_{r(A)})`$ and
    * If `$y` is instance add the new cast $`\mathsf{val}(a) = \mathsf{val}(y)`$
    * If `$y` is value var set $`\mathsf{val}(a) = r(a)`$
* 🔷 `$x has $A == <VAL_EXPR>` adds new element $`a :_! r(A)(r(x) : O_{r(A)})`$ and add cast $`\mathsf{val}(a) = v_r(val\_expr)`$
* 🔷 `$x has $A <NV_VAL_EXPR>` is shorthand for `$x has $A == <NV_VAL_EXPR>` (recall `NV_VAL_EXPR` is an expression that's not a sole variable)

_System property_:

1. ✅ _Idempotency_. If $`a :_! r(A)`$ with $`\mathsf{val}(a) = \mathsf{val}(b)`$ then we equate $`a = b`$ (this actually follows from the "Attribute identity rule", see "Type system").
1. 🔶 _Capability check_. Must have $`T(x) \leq B <_! O_{r(A)}`$ **non-abstractly**, i.e. $`\diamond (B <_! O_{r(A)})`$ is not true for the minimal choice of $`B`$ satisfying  $`T(x) \leq B <_! O_{r(A)}`$
1. 🔶 _Type check_. Value $`v`$ of newly inserted attribute must be of right value type (up to implicit casts, see "Expression evaluation"). Also must have $`T(y) \leq r(A)`$ if $`y`$ is given.

_Remark_: ⛔ Previously we had the constraint that we cannot add $`r(y) :_! A(r(x) : O_A)`$ if there exists any subtype $`B \lneq A`$.

#### **Case HAS_LIST_INS**
* 🔶 `$x has $A[] == <LIST_EXPR>` adds new list $`l = [l_1, l_2, ...] :_! [r(A)](r(x) : O_{r(A)[]})`$ **and** new attributes $`l_i :_! r(A)(r(x) : O_{r(A)})`$ where
    * denote by $`[v_1,v_2, ...] = v_r(list\_expr)`$ the evaluation of the list expression (relative to crow $`r`$)
    * the list $`l`$ has the same length as $`[x_1,x_2, ...] = v_r(list\_expr)`$
        * if $`y_i = x_i`$ is an attribute instance, we add new cast $`\mathsf{val}(l_i) = \mathsf{val}(v_i)`$ 
        * if $`v_i = x_i`$ is a value, we add new cast $`\mathsf{val}(l_i) = v_i`$ 
* 🔶 `$x has $A[] <NV_LIST_EXPR>` is shorthand for `$x has $A == <NV_LIST_EXPR>` (recall `NV_LIST_EXPR` is an expression that's not a sole variable)
* 🔶 `$x has $A[] $y` is shorthand for `$x has $A == $y` but infers that the type of `$y` is a subtype of $`[r(A)]`$ (and thus we must have $`y_i = x_i`$ for all $`i`$ above)

_System property_:

1. ✅ _Idempotency_. Idempotency is automatic for (since lists are identified by their list elements) and enforced for new attributes as before.
1. 🔶 _System cardinality bound: **1 list per owner**_. We cannot have any $`k : [r(A)](r(x) : O_{r(A)[]})`$ with $`k \neq l`$. (Users should use "Update" instead!)
1. 🔶 _Capability check_. Must have $`T(x) \leq B <_! O_{r(A)[]}`$ **non-abstractly**, i.e. $`\diamond (B <_! O_{r(A)[]})`$ is not true for the minimal choice of $`B`$ satisfying $`T(x) \leq B <_! O_{r(A)[]}`$
1. 🔶 _Type check_. For each list element, must have either $`T(v_i) : V`$ (up implicit casts) or $`T(y) \leq r(A)`$.

### Optional inserts

#### **Case TRY_INS**
* 🔶 `try { <INS>; ...; <INS>; }` where `<INS>` are insert statements as described above.
  * `<TRY_INS>` blocks can appear alongside other insert statements in an `insert` clause
  * Execution is as described in "Basics of inserting" (**#BDD**)


## Delete semantics

### Basics of deleting


A `delete` clause comprises collection of _delete statements_.

* _Input crows_: The clause can take as input a stream `{ r }` of concept rows `r`: 
  * the clause is **executed** for each row `r` in the stream individually

* _Updating input rows_: Delete clauses can update bindings of their input concept row `r`
  * Executing `delete $x;` will remove `$x` from `r` (but `$x` may still appear in other crows `r'` of the input stream)

_Remark_: Previously, it was suggested: if `$x` is in `r` and $`r(x)`$ is deleted from $`T_r(x)`$ by the end of the execution of the clause (for _all_ input rows of the input stream) then we set $`r(x) = \emptyset`$ and $`T_r(x) = \emptyset`$.
Fundamental question: **is it better to silently remove vars? Or throw an error if vars pointing to deleted concepts are used?** (STICKY)
* Only for `delete $x;` can we statically say that `$x` must not be re-used
* Other question: would this interact with try? idea: take $`r(x) = \emptyset`$ if it points to a previously deleted concept

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
  * Modifications are buffered in transaction (see "Transactions")
  * Violation of system properties or schema constraints will lead to failing transactions (see "Transactions")

#### (Feature) optionality

* 🔶 _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `delete` clauses cannot be nested
  * `try` blocks variables are **determined* if they are bound in `r`
  * If any variable is not determined, the `try` block statements are **skipped**.
  * If all variables are determined, the `try` block statements are **runnable**.

### Delete statements

#### **Case CONCEPT_DEL**
* ✅ `$x;` removes $`r(x) :_! A(...)`$. If $`r(x)`$ is an object, we also:
  * replaces any $`b :_! B(r(x) : I, z : J, ...)`$ by $`b :_! B(z : J, ...)`$ for all such dependencies on $`r(x)`$

_Remark 1_. This applies both to $`B : \mathbf{Rel}`$ and $`B : \mathbf{Att}`$.

_Remark 2_. The resulting $`r(x) :_! r(A)(z : J, ...)`$ must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

_System property_:

1. 🔷 If $`r(x) : A : \mathbf{Att}`$ and $`A`$ is _not_ marked `@independent` then the transaction will fail.


#### **Modifier: CASCADE_DEL**
* 🔮 `delete` clause keyword can be modified with a `@cascade(<LABEL>,...)` annotation, which acts as follows:

  If `@cascade(C, D, ...)` is specified, and `$x` is delete then we not only remove $`r(x) :_! A(...)`$ but (assuming $`r(x)`$ is an object) we also:
  * whenever we replace $`b :_! B(r(x) : I, z : J, ...)`$ by $`b :_! B(z : J, ...)`$ and the following are _both_ satisfied:

    1. the new axiom $`b :_! B(...)`$ violates interface cardinality of $`B`$,
    2. $`B`$ is among the listed types `C, D, ...`
    
    then delete $`b`$ and _its_ depenencies (the cascade may recurse).

_Remark_. In an earlier version of the spec, condition (1.) for the recursive delete was omitted—however, there are two good reasons to include it:

1. The extra condition only makes a difference when non-default interface cardinalities are imposed, in which case it is arguably useful to adhere to those custom constraints.
2. The extra condition ensure that deletes cannot interfere with one another, i.e. the order of deletion does not matter.

#### **Case ROL_OF_DEL**
* ✅ `($I: $y) of $x` replaces $`r(x) :_! r(A)(r(y) : r(I), z : J, ...)`$ by $`r(x) :_! r(A)(z : J, ...)`$

_Remark_. The resulting $`r(x) :_! r(A)(z : J, ...)`$ must be within schema constraints, or the transaction will fail. This will follow from the general mechanism for checking schema constraints; see "Transactions".

#### **Case ROL_LIST_OF_DEL**
* 🔶 `($I[]: <T_LIST>) of $x` replaces $`r(x) :_! r(A)(l : r(I))`$ by $`r(x) :_! r(A)()`$ for $`l`$ being the evaluation of `T_LIST`.

#### **Case ATT_OF_DEL**
* 🔶 `$y of $x` replaces any $`r(y) :_! T(y)(r(x) : O_{r(B)})`$ by $`r(y) :_! T(y)()`$
* 🔷 `$B of $x` replaces any $`a :_! r(B)(r(x) : O_{r(B)})`$ by $`a :_! r(B)()`$

_System property_
* 🔶 _Capability check_. Cannot have that `T($y) owns r($B)[]` (in this case, must delete entire lists instead!)
* 🔷 _Type check_ Must have $`T(y) = r(B)`$.

#### **Case ATT_LIST_OF_DEL**
* 🔶 `$y of $x` deletes any $`r(y) = [l_1, l_2, ... ] :_! [A](r(x) : O_{A[]})`$ where we must have $`A(y) = [A]`$ for some $`A : \mathbf{Att}`$, and 
    replaces any $`l_i :_! r(B)(r(x) : O_{r(B)})`$ by $`l_i :_! B'()`$
* 🔷 `$B[] of $x` deletes any $`l = [l_1, l_2, ... ] :_! r(B)(r(x) : O_{r(B)})`$ and replaces any $`l_i :_! r(B)(r(x) : O_{r(B)})`$ by $`l_i :_! B'()`$

### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete  clause.

## Update behavior

### Basics of updating

A `update` clause comprises collection of _update statements_.

* _Input crow_: The clause can take as input a stream `{ r }` of concept rows `r`, in which case 
  * the clause is **executed** for each row `r` in the stream individually

* _Updating input rows_: Update clauses do not update bindings of their input crow `r`

#### (Theory) Execution

* _Execution_: An `update` clause is executed by executing its statements individually in any order.
  * STICKY: this might be non-deterministic if the same thing is updated multiple times, solution outlined here: throw error if that's the case!

#### (Feature) Optionality

* _Optionality_: Optional variables are those exclusively appearing in a `try` block
  * `try` blocks in `delete` clauses cannot be nested
  * `try` blocks variables are **determined** if they are supplied by `r`
  * If any variable is not determined, the `try` block statements are **skipped**.
  * If all variables are determined, the `try` block statements are **runnable**.

### Update statements

#### **Case LINKS_UP**
* 🔶 `$x links ($I: $y);` updates $`r(x) :_! A(b:J)`$ to $`r(x) :_! A(r(x) : r(I))`$

_System property_:

1. 🔶 Require there to be exactly one present roleplayer for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction. (STICKY: discuss!)

#### **Case LINKS_LIST_UP** 
* 🔶 `$x links ($I[]: <T_LIST>)` updates $`r(x) :_! A(j : [r(I)])`$ to $`r(x) :_! A(l : [r(I)])`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

_System property_:

1. 🔶 Require there to be a present roleplayer list for update to succeed (can have at most one).
1. 🔶 Require that each update happens at most once, or fail the transaction.

#### **Case HAS_UP**
* 🔶 `$x has $B $y;` updates $`b :_! r(B)(x:O_{r(B)})`$ to $`r(y) :_! r(B)(x:O_{r(B)})`$

_System property_:

1. 🔶 Require there to be exactly one present attribute for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction.

#### **Case HAS_LIST_UP**
* 🔶 `$x has $A[] <T_LIST>` updates $`j :_! [r(A)](r(x) : O_{r(A)})`$ to $`l :_! [r(A)](r(x) : O_{r(A)})`$ for `<T_LIST>` evaluating to $`l = [l_0, l_1, ...]`$

_System property_:

1. 🔶 Require there to be a present attribute list for update to succeed.
1. 🔶 Require that each update happens at most once, or fail the transaction.


### Clean-up

Orphaned relation and attribute instance (i.e. those with insufficient dependencies) are cleaned up at the end of a delete clause.

## Put behavior

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

As described in "Match behavior".

### Insert

As described in "Insert behavior".

### Delete

As described in "Delete behavior".

### Update

As described in "Update behavior".

### Put

As described in "Put behavior".

### Fetch

* 🔶 The `fetch` clause is of the form

  ```
  fetch { 
   <fetch-KV-statement>;
   ...
   <fetch-KV-statement>;
  }
  ```

  * The `fetch` clause takes as input a crow stream `{ r }`
  * It output a stream `{ doc<m> }` of JSON documents (one for each `r` in the input stream)
  * The `fetch` clause is **terminal**

#### **Case FETCH_VAL**
* 🔶 `"key": $x`

#### **Case FETCH_EXPR**
* 🔶 `"key": <EXPR>`

_Note_. `<EXPR>` can, in particuar, be `T_LIST` expression (see "Expressions").

#### **Case FETCH_ATTR**
* 🔶 `"key": $x.A` where $`A : \mathbf{Att}`$

_System property_

1. 🔶 fails transaction if $`T_r(x)`$ does not own $`A`$ with `card(0,1)`.

#### **Case FETCH_MULTI_ATTR**
* 🔶 `"key": [ $x.A ]` where $`A : \mathbf{Att}`$

_System property_

1. 🔶 fails transaction if $`T_r(x)`$ does not own $`A`$.

#### **Case FETCH_LIST_ATTR**
* 🔶 `"key": $x.A[]` where  $`A : \mathbf{Att}`$

_System property_

1. 🔶 fails transaction if $`T_r(x)`$ does not own $`[A]`$.

#### **Case FETCH_ALL_ATTR**
* 🔶 `"key": { $x.* }` where  $`A : \mathbf{Att}`$

_System property_

1. 🔶 returns document of KV-pairs
    * `"att_label" : [ <atts> ]` and
    * `"att_label[]" : <att-list>`.

#### **Case FETCH_SNGL_FUN**
* 🔶 `"key": fun(...)` where `fun` is **scalar** (i.e. non-tuple) single-return.

#### **Case FETCH_STREAM_FUN**
* 🔶 `"key": [ fun(...) ]` where `fun` is **scalar** (i.e. non-tuple) stream-return.

_Note_: (STICKY:) what to do if type inference for function args fails based on previous pipeline stages?

#### **Case FETCH_FETCH**
* 🔶 Fetch list of JSON sub-documents:
```
"key": [ 
  <READ_PIPELINE>
  fetch { <FETCH> }
]
```

#### **Case FETCH_RETURN_VAL** 
* 🔶 Fetch single-value:
```
"key": ( 
  <READ_PIPELINE>
  return <SINGLE> <VAR>; 
)
```

#### **Case FETCH_RETURN_STREAM** 
* 🔶 Fetch single-value:
```
"key": [ 
  <READ_PIPELINE>
  return { <VAR> }; 
]
```

#### **Case FETCH_RETURN_AGG** 
* 🔶 Fetch stream as list:
```
"key": [ 
  <READ_PIPELINE>
  return <AGG>, ... , <AGG>; 
]
```

This is short hand for:
```
"key": [ 
  <PIPELINE>
  reduce $_1 = <AGG>, ... , $_n = <AGG>; 
  return first $_1, ..., $_n; 
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
     
    * input stream of rows `{ r }`
    * output stream of rows `{ p(m) }` for each `r` in the input, where `p(m)` only keeps the given variables that are among `$x1, $x2, ...`

### Deselect 
* 🔶 deselect syntax:
    `deselect $x1, $x2, ...`
     
    * input stream of rows `{ r }`
    * output stream of rows `{ p(m) }` for each `r` in the input, where `p(m)` only keeps the given variables that are **not** among `$x1, $x2, ...`

### Distinct
* 🔶 distinct syntax:
    `distinct $x1, $x2, ...`
     
    * input stream of rows `{ r }`
    * output stream of rows `{ o }` for each distinct row in the input (in other words: duplicates are removed)
        * empty value is its own distinct value

### Require
* 🔮 require syntax:
    `require $x1, $x2, ...`
     
    * filters `{ r }` keeping only maps where `r($x1)`, `r($x2)`, ... are non-empty

### Sort
* 🔶 sort syntax:
    `sort $x1, $x2, ...`
     
    * input stream of rows `{ r }`
    * output stream of rows `{ o }` obtained by ordering the input stream:
      * first on values `r($x1)`
      * then on values `r($x2)`,
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
    * The `reduce` operator takes as input a stream of rows `{ r }`
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
    * outputs `true` if concept row stream contains a row `r` with non-empty entry `r($x)` for `$x`
  * 🔶 `sum($x)`:
    * output type `double` or `int`
    * outputs sum of all non-empty `r($x)` in concept row `r`
    * `$x` can be optional
    * empty sums yield `0.0` or `0`
  * 🔶 `mean($x)`:
    * output type `double?`
    * outputs mean of all non-empty `r($x)` in concept row `r`
    * `$x` can be optional
    * empty mean yield empty output ($\emptyset$)
  * 🔶 `median($x)`, 
    * output type `double?` or `int?` (depending on type of `$x`)
    * outputs median of all non-empty `r($x)` in concept row `r`
    * `$x` can be optional
    * empty medians output $\emptyset
  * 🔶 `count`
    * output type `long`
    * outputs count of all answer
  * 🔶 `count($x)`
    * output type `long`
    * outputs count of all non-empty `r($x)` in input crow stream `{ r }`
    * `$x` can be optionals
  * 🔶 `count($x, $y, ...)`
    * output type `long`
    * outputs count of all non-empty concept tuples `(r($x), r($y), ...)` in input crow stream
    * `$x` can be optional
  * 🔮 `distinct($x)`
    * output type `long`
    * outputs count of all non-empty **distinct** `r($x)` in input crow stream `{ r }`
    * `$x` can be optionals
  * 🔮 `distinct($x, $y, ...)`
    * output type `long`
    * outputs count of all non-empty **distinct** concept tuples `(r($x), r($y), ...)` in input crow stream `{ r }`
    * `$x` can be optional
  * 🔶 `list($x)`
    * output type `[A]`
    * returns list of all non-empty `r($x)` in concept row `r`
    * `$x` can be optional
* Each `<AGG>` reduces the concept row `{ r }` passsed to it from the function's body to a single value in the specified way.

#### **Case GROUP_RED**
* 🔶 Groupe reduce syntax:
    ```
    reduce $x_1=<AGG>, ... , $x_k=<AGG> within $y_1, $y_2, ...;
    ``` 

    In this case, we output the following:
    * 🔶 for each distinct tuple of elements `el_1, el_2, ...` assigned to `$y_1, $y_2, ...` by rows in the stream, we perform the aggregates as described above over _all rows `r`_ for which `r($y_1) = el_1, r($y__2) = el_2, ...` and then output the resulting concept row `($y_1 -> el_1, $y_2 = el_2, ..., $x_1 -> <CPT>, $x_2 -> <CPT>, ...)`


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

Callable `match-return` query. 

* can be **single-return** function (e.g. `-> A, B`) or **stream-return** function (e.g. `-> { A, B }`)
* can be **scalar** function (e.g. `-> A` or `-> { B }`) or **tuple** function (e.g. `-> { A, B }` or `-> A, B`)

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
