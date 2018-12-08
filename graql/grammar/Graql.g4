grammar Graql;

queryList : query* EOF ;

queryEOF       : query EOF ;
query          : getQuery | insertQuery | defineQuery | undefineQuery | deleteQuery | aggregateQuery | computeQuery ;

matchClause    : MATCH patterns                             # matchBase
               | matchClause 'limit' INTEGER              ';' # matchLimit
               | matchClause 'offset' INTEGER             ';' # matchOffset
               | matchClause 'order' 'by' VARIABLE order? ';' # matchOrderBy
               ;

getQuery       : matchClause 'get' variables? ';' ;
insertQuery    : matchClause? INSERT statements ;
defineQuery    : DEFINE statements ;
undefineQuery  : UNDEFINE statements ;
deleteQuery    : matchClause 'delete' variables? ';' ;

variables      : VARIABLE (',' VARIABLE)* ;

// GRAQL AGGREGATE QUERY GRAMMAR =======================================================================================
aggregateQuery : matchClause 'aggregate' aggregate ';' ;
aggregate      : identifier argument*             # customAgg
               ;
argument       : VARIABLE  # variableArgument
               | aggregate # aggregateArgument
               ;

// GRAQL COMPUTE QUERY GRAMMAR =========================================================================================
//
// A compute query is composed of 3 things:
// the 'compute' keyword, followed by a compute method, and optionally a set of conditions
//
// A compute method could only be:
// count, min, max, median, mean, std, sum, path, centrality, cluster
//
// The compute conditions can be a set of zero or more individual condition separated by a comma
// A compute condition can either be a FromID, ToID, OfLabels, InLabels, Algorithm or Args

computeQuery        : COMPUTE computeMethod computeConditions? ';';
computeMethod       : COUNT                                                     // compute the number of concepts
                    | MIN | MAX | MEDIAN | MEAN | STD | SUM                     // compute statistics functions
                    | PATH                                                      // compute the paths between concepts
                    | CENTRALITY                                                // compute density of connected concepts
                    | CLUSTER                                                   // compute detection of cluster
                    ;
computeConditions   : computeCondition (',' computeCondition)* ;
computeCondition    : FROM      computeFromID
                    | TO        computeToID
                    | OF        computeOfLabels
                    | IN        computeInLabels
                    | USING     computeAlgorithm
                    | WHERE     computeArgs
                    ;

computeFromID       : id ;
computeToID         : id ;
computeOfLabels     : labels ;
computeInLabels     : labels ;
computeAlgorithm    : DEGREE | K_CORE | CONNECTED_COMPONENT ;                   // different types of algorithms that
                                                                                // determines how the method is computed

computeArgs         : computeArg | computeArgsArray ;                           // single argument or array of arguments
computeArgsArray    : '[' computeArg (',' computeArg)* ']' ;                    // an array of arguments
computeArg          : MIN_K         '='     INTEGER        # computeArgMinK     // a single argument for min-k=INTEGER
                    | K             '='     INTEGER        # computeArgK        // a single argument for k=INTEGER
                    | SIZE          '='     INTEGER        # computeArgSize     // a single argument for size=INTEGER
                    | CONTAINS      '='     id             # computeArgContains // a single argument for contains=ID
                    ;

// =====================================================================================================================

patterns       : (pattern ';')+ ;
pattern        : statement                     # patternStatement
               | pattern 'or' pattern          # patternDisjunction
               | '{' patterns '}'              # patternConjunction
               ;

statements    : (statement ';')+ ;
statement     : VARIABLE | variable? property (','? property)* ;

property       : 'isa' variable                     # isa
               | 'isa!' variable                    # isaExplicit
               | 'sub' variable                     # sub
               | 'sub!' variable                    # subExplicit
               | 'relates' role=variable ('as' superRole=variable)? # relates
               | 'plays' variable                   # plays
               | 'id' id                            # propId
               | 'label' label                      # propLabel
               |  predicate                         # propValue
               | 'when' '{' patterns '}'            # propWhen
               | 'then' '{' statements '}'         # propThen
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
               | MIN_K | K | CONTAINS | SIZE | WHERE
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
CONTAINS       : 'contains' ;
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
