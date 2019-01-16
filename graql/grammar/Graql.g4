/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

grammar Graql;

// Graql end-of-file (aka. end-of-string) query parser
// Needed by Graql's Parser to ensure that it parses till end of string

eof_query           :   query       EOF ;
eof_query_list      :   query*      EOF ;
eof_pattern         :   pattern     EOF ;
eof_pattern_list    :   pattern*    EOF ;

// GRAQL QUERY LANGUAGE ========================================================

query               :   query_define    |   query_undefine                      // define / undefine types from schema
                    |   query_insert    |   query_delete                        // insert / delete data from graph
                    |   query_get       |   query_aggregate |   query_group     // read data from graph (OLTP)
                    |   query_compute   ;                                       // compute analytics over graph (OLAP)

query_define        :   DEFINE      statement_type+ ;
query_undefine      :   UNDEFINE    statement_type+ ;

query_insert        :   MATCH       pattern+    INSERT  statement_instance+
                    |                           INSERT  statement_instance+ ;

query_delete        :   MATCH       pattern+    DELETE  variables   modifier* ; // GET QUERY followed by aggregate fn
query_get           :   MATCH       pattern+    GET     variables   modifier* ; // GET QUERY followed by group fn, and
                                                                                // optionally, an aggregate fn

query_aggregate     :   query_get   function_aggregate  ;
query_group         :   query_get   function_group      function_aggregate? ;

query_compute       :   COMPUTE     compute_method      compute_conditions? ';';// TODO: embbed ';' into subrule

// DELETE AND GET QUERY MODIFIERS ==============================================

variables           :   ( VAR_ ( ',' VAR_ )* )? ';'     ;

modifier            :   offset  |   limit   |   order   ;

offset              :   OFFSET      INTEGER_    ';'     ;
limit               :   LIMIT       INTEGER_    ';'     ;
order               :   ORDER     ( ASC|DESC )? ';'     ;

// GET AGGREGATE QUERY =========================================================
//
// An aggregate function is composed of 2 things:
// The aggregate method name, followed by the variable to apply the function to

function_aggregate  :   function_method    VAR_?   ';';                         // method and, optionally, a variable
function_method     :   COUNT   |   MAX     |   MEAN    |   MEDIAN              // calculate statistical values
                    |   MIN     |   STD     |   SUM     ;

// GET GROUP QUERY =============================================================
//
// An group function is composed of 2 things:
// The 'GROUP' method name, followed by the variable to group the results by

function_group      :   GROUP   VAR_    ';' ;

// COMPUTE QUERY ===============================================================
//
// A compute query is composed of 3 things:
// The "compute" keyword followed by a method and optionally a set of conditions

compute_method      :   COUNT                                                   // compute the number of concepts
                    |   MIN         |   MAX         |   MEDIAN                  // compute statistics functions
                    |   MEAN        |   STD         |   SUM
                    |   PATH                                                    // compute the paths between concepts
                    |   CENTRALITY                                              // compute density of connected concepts
                    |   CLUSTER                                                 // compute detection of cluster
                    ;
compute_conditions  :   compute_condition ( ',' compute_condition )* ;
compute_condition   :   FROM    id                                              // an instance to start the compute from
                    |   TO      id                                              // an instance to end the compute at
                    |   OF      labels                                          // type(s) of instances to apply compute
                    |   IN      labels                                          // type(s) to scope compute visibility
                    |   USING   compute_algorithm                               // algorithm to determine how to compute
                    |   WHERE   compute_args                                    // additional args for compute method
                    ;
compute_algorithm   :   DEGREE | K_CORE | CONNECTED_COMPONENT ;                 // algorithm to determine how to compute
compute_args        :   compute_arg | compute_args_array ;                      // single argument or array of arguments
compute_args_array  :   '[' compute_arg (',' compute_arg)* ']' ;                // an array of arguments
compute_arg         :   MIN_K     '=' INTEGER_                                  // a single argument for min-k=INTEGER
                    |   K         '=' INTEGER_                                  // a single argument for k=INTEGER
                    |   SIZE      '=' INTEGER_                                  // a single argument for size=INTEGER
                    |   CONTAINS  '=' id                                        // a single argument for contains=ID
                    ;

// QUERY PATTERNS ==============================================================

patterns            :   pattern+ ;
pattern             :   pattern_statement
                    |   pattern_conjunction
                    |   pattern_disjunction
                    |   pattern_negation
                    ;
pattern_conjunction :   '{' patterns '}' ';' ;
pattern_disjunction :   '{' patterns '}'  ( OR '{' patterns '}' )+  ';' ;

pattern_negation    :   NOT '{' patterns '}' ';' ;

// PATTERN STATEMENTS ==========================================================

pattern_statement   :   statement_type
                    |   statement_instance
                    |   statement_negation
                    ;
statement_negation  :   NOT statement_type
                    |   NOT statement_instance
                    ;
// TYPE STATEMENTS =============================================================

statement_type      :   type        type_property ( ',' type_property )* ';' ;
type_property       :   ABSTRACT
                    |   SUB_        type
                    |   KEY         type
                    |   HAS         type
                    |   PLAYS       type
                    |   RELATES     type ( AS type )?
                    |   DATATYPE    datatype
                    |   REGEX       regex
                    |   WHEN    '{' pattern+              '}'
                    |   THEN    '{' statement_instance+   '}'                   // TODO: remove '+'
                    |   TYPE        label
                    ;

// INSTANCE STATEMENTS =========================================================

statement_instance  :   statement_thing
                    |   statement_relation
                    |   statement_attribute
                    ;
statement_thing     :   VAR_                ISA_ type   ( ',' attributes )? ';'
                    |   VAR_                ID   id     ( ',' attributes )? ';'
                    |   VAR_                NEQ  VAR_                       ';'
                    |   VAR_                attributes                      ';'
                    ;
statement_relation  :   VAR_? relation      ISA_ type   ( ',' attributes )? ';'
                    |   VAR_? relation      attributes                      ';'
                    |   VAR_? relation                                      ';'
                    ;
statement_attribute :   VAR_? predicate     ISA_ type   ( ',' attributes )? ';'
                    |   VAR_? predicate     attributes                      ';'
                    |   VAR_? predicate                                     ';'
                    ;

// ATTRIBUTE STATEMENT CONSTRUCT ===============================================

attributes          :   attribute ( ',' attribute )* ;
attribute           :   HAS label ( VAR_ | predicate ) via? ;                   // Attribute ownership by variable or a
                                                                                // predicate, and the "via" Relation
// RELATION STATEMENT CONSTRUCT ================================================

relation            :   '(' role_player ( ',' role_player )* ')' ;              // A list of role players in a Relations
role_player         :   type ':' player                                         // The Role type and and player variable
                    |            player ;                                       // Or just the player variable
player              :   VAR_ ;                                                  // A player is just a variable
via                 :   VIA VAR_ ;                                              // The Relation variable that holds the
                                                                                // assertion between an Attribute and
                                                                                // its owner (any Thing)
// TYPE STATEMENT CONSTRUCT ====================================================

type                :   label | VAR_ ;                                          // Matching a Type can done by providing
                                                                                // a label or variable
// PREDICATE STATEMENT CONSTRUCT ===============================================

predicate           :   '=='?   value                   # predicateEq           // TODO: split with value assignment
                    |   '=='    VAR_                    # predicateVariable
                    |   '!=='   valueOrVar              # predicateNeq
                    |   '>'     valueOrVar              # predicateGt
                    |   '>='    valueOrVar              # predicateGte
                    |   '<'     valueOrVar              # predicateLt
                    |   '<='    valueOrVar              # predicateLte
                    |   'contains' (STRING_ | VAR_)    # predicateContains
                    |   LIKE    regex                   # predicateRegex
                    ;
valueOrVar          :   VAR_       # valueVariable
                    |   value      # valuePrimitive
                    ;

// DATA TYPE AND VALUE STATEMENT CONSTRUCTS ====================================

value               :   STRING_    # valueString
                    |   INTEGER_   # valueInteger
                    |   REAL_      # valueReal
                    |   bool       # valueBoolean
                    |   DATE_      # valueDate
                    |   DATETIME_  # valueDateTime
                    ;

regex               : STRING_ ;
datatype            : LONG | DOUBLE | STRING | BOOLEAN | DATE ;
bool                : TRUE | FALSE ;

labels              : labelsArray | label ;
labelsArray         : '[' label (',' label)* ']' ;
label               : identifier | ID_IMPLICIT_;
id                  : identifier ;
identifier          : ID_ | STRING_ | unreserved ;                              // TODO: disallow quoted strings as IDs

// GRAQL SYNTAX KEYWORDS =======================================================

// QUERY COMMAND KEYWORDS

MATCH           : 'match'       ;   GET             : 'get'         ;
DEFINE          : 'define'      ;   UNDEFINE        : 'undefine'    ;
INSERT          : 'insert'      ;   DELETE          : 'delete'      ;
AGGREGATE       : 'aggregate'   ;   COMPUTE         : 'compute'     ;

// DELETE AND GET QUERY MODIFIER KEYWORDS

OFFSET          : 'offset'      ;   LIMIT           : 'limit'       ;
ASC             : 'asc'         ;   DESC            : 'desc'        ;
ORDER           : 'order'       ;

// STATEMENT PROPERTY KEYWORDS

ABSTRACT        : 'abstract'    ;
VIA             : 'via'         ;   AS              : 'as'          ;
ID              : 'id'          ;   TYPE            : 'type'        ;
ISA_            : ISA | ISAX    ;   SUB_            : SUB | SUBX    ;
ISA             : 'isa'         ;   ISAX            : 'isa!'        ;
SUB             : 'sub'         ;   SUBX            : 'sub!'        ;
KEY             : 'key'         ;   HAS             : 'has'         ;
PLAYS           : 'plays'       ;   RELATES         : 'relates'     ;
DATATYPE        : 'datatype'    ;   REGEX           : 'regex'       ;
WHEN            : 'when'        ;   THEN            : 'then'        ;

// GROUP AND AGGREGATE QUERY KEYWORDS (also used by COMPUTE QUERY)

GROUP           : 'group'       ;   COUNT           : 'count'       ;
MAX             : 'max'         ;   MIN             : 'min'         ;
MEAN            : 'mean'        ;   MEDIAN          : 'median'      ;
STD             : 'std'         ;   SUM             : 'sum'         ;

// COMPUTE QUERY KEYWORDS

CLUSTER         : 'cluster'     ;   CENTRALITY      : 'centrality'  ;
PATH            : 'path'        ;   DEGREE          : 'degree'      ;
K_CORE          : 'k-core'      ;   CONNECTED_COMPONENT : 'connected-component';
FROM            : 'from'        ;   TO              : 'to'          ;
OF              : 'of'          ;   IN              : 'in'          ;
USING           : 'using'       ;   WHERE           : 'where'       ;
MIN_K           : 'min-k'       ;   K               : 'k'           ;
SIZE            : 'size'        ;   CONTAINS        : 'contains'    ;


// OPERATOR KEYWORDS

OR              : 'or'          ;   NOT             : 'not'         ;
LIKE            : 'like'        ;   NEQ             : '!='          ;

// DATA TYPE KEYWORDS

LONG            : 'long'        ;   DOUBLE          : 'double'      ;
STRING          : 'string'      ;   BOOLEAN         : 'boolean'     ;
DATE            : 'date'        ;

// BOOLEAN KEYWORDS

TRUE            : 'true'        ;   FALSE           : 'false'       ;

// UNRESERVED KEYWORDS
// Most of Graql syntax should not be reserved from being used as identifiers

unreserved      : MIN | MAX| MEDIAN | MEAN | STD | SUM | COUNT | PATH | CLUSTER
                | FROM | TO | OF | IN
                | DEGREE | K_CORE | CONNECTED_COMPONENT
                | MIN_K | K | CONTAINS | SIZE | WHERE
                ;

// GRAQL INPUT TOKEN PATTERNS
// All token names must end with an underscore ('_')
VAR_            : VAR_ANONYMOUS_ | VAR_NAMED_ ;
VAR_ANONYMOUS_  : '$_' ;
VAR_NAMED_      : '$' [a-zA-Z0-9_-]* ;
ID_             : [a-zA-Z_] [a-zA-Z0-9_-]* ;
ID_IMPLICIT_    : '@' [a-zA-Z0-9_-]+ ;
STRING_         : '"' (~["\\/] | ESCAPE_SEQ_ )* '"' | '\'' (~['\\/] | ESCAPE_SEQ_)* '\'';
//REGEX_          : '"' (~'/' | '\\/')* '"' | '\'' (~'/' | '\\/')* '\'' ;
//REGEX_          : '/' (~'/' | '\\/')* '/' ;
INTEGER_        : ('+' | '-')? [0-9]+ ;
REAL_           : ('+' | '-')? [0-9]+ '.' [0-9]+ ;
DATE_           : DATE_FRAGMENT_ ;
DATETIME_       : DATE_FRAGMENT_ 'T' TIME_ ;

fragment DATE_FRAGMENT_ : YEAR_ '-' MONTH_ '-' DAY_ ;
fragment MONTH_         : [0-1][0-9] ;
fragment DAY_           : [0-3][0-9] ;
fragment YEAR_          : [0-9][0-9][0-9][0-9] | ('+' | '-') [0-9]+ ;
fragment TIME_          : HOUR_ ':' MINUTE_ (':' SECOND_)? ;
fragment HOUR_          : [0-2][0-9] ;
fragment MINUTE_        : [0-6][0-9] ;
fragment SECOND_        : [0-6][0-9] ('.' [0-9]+)? ;
fragment ESCAPE_SEQ_    : '\\' . ;

COMMENT         : '#' .*? '\r'? ('\n' | EOF)    -> channel(HIDDEN) ;
WS              : [ \t\r\n]+                    -> channel(HIDDEN) ;