grammar Graql;

queryList : query* EOF ;

queryEOF       : query EOF ;
query          : getQuery | insertQuery | defineQuery | undefineQuery | deleteQuery | aggregateQuery | computeQuery ;

matchPart      : MATCH patterns                             # matchBase
               | matchPart 'limit' INTEGER              ';' # matchLimit
               | matchPart 'offset' INTEGER             ';' # matchOffset
               | matchPart 'order' 'by' VARIABLE order? ';' # matchOrderBy
               ;

getQuery       : matchPart 'get' variables? ';' ;
insertQuery    : matchPart? INSERT varPatterns ;
defineQuery    : DEFINE varPatterns ;
undefineQuery  : UNDEFINE varPatterns ;
deleteQuery    : matchPart 'delete' variables? ';' ;
aggregateQuery : matchPart 'aggregate' aggregate ';' ;

variables      : VARIABLE (',' VARIABLE)* ;

// GRAQL COMPUTE QUERY: OVERALL SYNTAX GRAMMAR =========================================================================
// Author: Haikal Pribadi
//
// A compute query is composed of 3 things:
// the 'compute' keyword, followed by a compute method, and a set of conditions (that may be optional)
//
// A compute method could only be:
// count                                    (to count the number of concepts in the graph)
// min, max, median, mean, std and sum      (to calculate statistics functions)
// path                                     (to compute the paths between concepts)
// centrality                               (to compute how densely connected concepts are in the graph)
// cluster                                  (to detect communities in the graph)
//
// The compute conditions can be a set of zero or more individual condition separated by a comma
// A compute condition can either be a FromID, ToID, OfLabels, InLabels, Algorithm or Args

computeQuery                    : COMPUTE computeMethod computeConditions? ';';
computeMethod                   : COUNT                                                 // compute count
                                | MIN | MAX | MEDIAN | MEAN | STD | SUM                 // compute statistics
                                | PATH                                                  // compute path
                                | CENTRALITY                                            // compute centrality
                                | CLUSTER                                               // compute cluster
                                ;
computeConditions               : computeCondition (',' computeCondition)* ;
computeCondition                : FROM      computeFromID
                                | TO        computeToID
                                | OF        computeOfLabels
                                | IN        computeInLabels
                                | USING     computeAlgorithm
                                | WHERE     computeArgs
                                ;

// GRAQL COMPUTE QUERY: CONDITIONS GRAMMAR =============================================================================
// Author: Haikal Pribadi
//
// The following are definitions of computeConditions for the Graql Compute Query
// computeFromID and computeToID takes in a concept ID
// computeOfLabels and computeInLabels takes in labels, such as types in the schema
// computeAlgorithm are the different algorithm names that determines how the compute method is performed
// computeArgs can either be a single argument or an array of arguments
// computeArgsArray is an arry of arguments
// computeArg is a single argument

computeFromID                   : id ;
computeToID                     : id ;
computeOfLabels                 : labels ;
computeInLabels                 : labels ;
computeAlgorithm                : DEGREE | K_CORE | CONNECTED_COMPONENT ;
computeArgs                     : computeArgsArray | computeArg ;
computeArgsArray                : '[' computeArg (',' computeArg)* ']' ;
computeArg                      : MIN_K         '='     INTEGER         # computeArgMinK
                                | K             '='     INTEGER         # computeArgK
                                | START         '='     id              # computeArgStart
                                | MEMBERS       '='     bool            # computeArgMembers
                                | SIZE          '='     INTEGER         # computeArgSize ;

aggregate      : identifier argument*             # customAgg
               | '(' namedAgg (',' namedAgg)* ')' # selectAgg
               ;
argument       : VARIABLE  # variableArgument
               | aggregate # aggregateArgument
               ;
namedAgg       : aggregate 'as' identifier ;

patterns       : (pattern ';')+ ;
pattern        : varPattern                    # varPatternCase
               | pattern 'or' pattern          # orPattern
               | '{' patterns '}'              # andPattern
               ;

varPatterns    : (varPattern ';')+ ;
varPattern     : VARIABLE | variable? property (','? property)* ;

property       : 'isa' variable                     # isa
               | 'isa!' variable                    # isaExplicit
               | 'sub' variable                     # sub
               | 'relates' role=variable ('as' superRole=variable)? # relates
               | 'plays' variable                   # plays
               | 'id' id                            # propId
               | 'label' label                      # propLabel
               | 'val'? predicate                   # propValue
               | 'when' '{' patterns '}'            # propWhen
               | 'then' '{' varPatterns '}'         # propThen
               | 'has' label (resource=VARIABLE | predicate) ('via' relation=VARIABLE)? # propHas
               | 'has' variable                     # propResource
               | 'key' variable                     # propKey
               | '(' casting (',' casting)* ')'     # propRel
               | 'is-abstract'                      # isAbstract
               | 'datatype' datatype                # propDatatype
               | 'regex' REGEX                      # propRegex
               | '!=' variable                      # propNeq
               ;

casting        : variable (':' VARIABLE)?
               | variable VARIABLE         {notifyErrorListeners("expecting {',', ':'}");};

variable       : label | VARIABLE ;

predicate      : '=='? value                    # predicateEq
               | '==' VARIABLE                  # predicateVariable
               | '!==' valueOrVar               # predicateNeq
               | '>' valueOrVar                 # predicateGt
               | '>=' valueOrVar                # predicateGte
               | '<' valueOrVar                 # predicateLt
               | '<=' valueOrVar                # predicateLte
               | 'contains' (STRING | VARIABLE) # predicateContains
               | REGEX                          # predicateRegex
               ;
valueOrVar     : VARIABLE # valueVariable
               | value    # valuePrimitive
               ;
value          : STRING   # valueString
               | INTEGER  # valueInteger
               | REAL     # valueReal
               | bool     # valueBoolean
               | DATE     # valueDate
               | DATETIME # valueDateTime
               ;

labels         : labelsArray | label ;
labelsArray    : '[' label (',' label)* ']' ;
label          : identifier | IMPLICIT_IDENTIFIER;
id             : identifier ;

// Some keywords can also be used as identifiers
identifier     : ID | STRING
               | MIN | MAX| MEDIAN | MEAN | STD | SUM | COUNT | PATH | CLUSTER
               | FROM | TO | OF | IN
               | DEGREE | K_CORE | CONNECTED_COMPONENT
               | MIN_K | K | START | MEMBERS | SIZE | WHERE
               ;

datatype       : LONG_TYPE | DOUBLE_TYPE | STRING_TYPE | BOOLEAN_TYPE | DATE_TYPE ;
order          : ASC | DESC ;
bool           : TRUE | FALSE ;

// keywords
MIN            : 'min' ;
MAX            : 'max' ;
MEDIAN         : 'median' ;
MEAN           : 'mean' ;
STD            : 'std' ;
SUM            : 'sum' ;
COUNT          : 'count' ;
PATH           : 'path' ;
CLUSTER        : 'cluster' ;
CENTRALITY     : 'centrality' ;
FROM           : 'from' ;
TO             : 'to' ;
OF             : 'of' ;
IN             : 'in' ;
DEGREE         : 'degree' ;
K_CORE         : 'k-core' ;
CONNECTED_COMPONENT : 'connected-component' ;
MIN_K          : 'min-k' ;
K              : 'k' ;
START          : 'start' ;
MEMBERS        : 'members' ;
SIZE           : 'size' ;
USING          : 'using' ;
WHERE          : 'where' ;
MATCH          : 'match' ;
INSERT         : 'insert' ;
DEFINE         : 'define' ;
UNDEFINE       : 'undefine' ;
COMPUTE        : 'compute' ;
ASC            : 'asc' ;
DESC           : 'desc' ;
LONG_TYPE      : 'long' ;
DOUBLE_TYPE    : 'double' ;
STRING_TYPE    : 'string' ;
BOOLEAN_TYPE   : 'boolean' ;
DATE_TYPE      : 'date' ;
TRUE           : 'true' ;
FALSE          : 'false' ;

// In StringConverter.java we inspect the lexer to find out which values are keywords.
// If literals are used in an alternation (e.g. `'true' | 'false'`) in the grammar, then they don't register as keywords.
// Therefore, we never use an alternation of literals and instead give them proper rule names (e.g. `TRUE | FALSE`).
VARIABLE       : '$' [a-zA-Z0-9_-]+ ;
ID             : [a-zA-Z_] [a-zA-Z0-9_-]* ;
STRING         : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';
REGEX          : '/' (~'/' | '\\/')* '/' ;
INTEGER        : ('+' | '-')? [0-9]+ ;
REAL           : ('+' | '-')? [0-9]+ '.' [0-9]+ ;
DATE           : DATE_FRAGMENT ;
DATETIME       : DATE_FRAGMENT 'T' TIME ;

fragment DATE_FRAGMENT : YEAR '-' MONTH '-' DAY ;
fragment MONTH         : [0-1][0-9] ;
fragment DAY           : [0-3][0-9] ;
fragment YEAR          : [0-9][0-9][0-9][0-9] | ('+' | '-') [0-9]+ ;
fragment TIME          : HOUR ':' MINUTE (':' SECOND)? ;
fragment HOUR          : [0-2][0-9] ;
fragment MINUTE        : [0-6][0-9] ;
fragment SECOND        : [0-6][0-9] ('.' [0-9]+)? ;

fragment ESCAPE_SEQ : '\\' . ;

COMMENT : '#' .*? '\r'? ('\n' | EOF) -> channel(HIDDEN) ;

IMPLICIT_IDENTIFIER : '@' [a-zA-Z0-9_-]+ ;

WS : [ \t\r\n]+ -> channel(HIDDEN) ;

// Unused lexer rule to help with autocomplete on variable names
DOLLAR : '$' ;
