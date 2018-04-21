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

// GRAQL COMPUTE QUERY SYNTAX GRAMMAR
// Author: Haikal Pribadi
//
// A compute query is composed of 3 things:
// the 'compute' keyword, followed by a compute method, and a set of conditions (that may be optional)
//
// A compute method could only be:
// count                                    (to count the number of concepts in the graph)
// min, max, median, mean, std and sum      (to calculate statistics functions)
// path, paths                              (to compute the the shortest path / possible paths between concepts)
// centrality                               (to compute how densely connected concepts are in the graph)
// cluster                                  (to detect communities in the graph)
//
// The compute conditions can be a set of zero or more individual condition separated by a comma
// A compute condition can either be a FromID, ToID, OfLabels, InLabels, Algorithm or Args

computeQuery                    : 'compute' computeMethod computeConditions? ';';
computeMethod                   : COUNT | MIN | MAX | MEDIAN | MEAN | STD | SUM
                                | PATH | PATHS
                                | CENTRALITY
                                | CLUSTER ;
computeConditions               : computeCondition (',' computeCondition)* ;
computeCondition                : 'from'    computeFromID
                                | 'to'      computeToID
                                | 'of'      computeOfLabels
                                | 'in'      computeInLabels
                                | USING     computeAlgorithm
                                | WHERE     computeArgs ;

// GRAQL COMPUTE CONDITIONS GRAMMAR
// Author: Haikal Pribadi
//
// The following are definitions of FromID, ToID, OfLabels, InLabels, Algorithm or Args
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
computeAlgorithm                : DEGREE | 'k-core' | 'connected-component' ;
computeArgs                     : computeArgsArray | computeArg ;
computeArgsArray                : '[' computeArg (',' computeArg)* ']' ;
computeArg                      : 'min-k'       '='     INTEGER         # computeArgMinK
                                | 'k'           '='     INTEGER         # computeArgK
                                | 'source'      '='     id              # computeArgStart
                                | MEMBERS       '='     bool            # computeArgMembers
                                | SIZE          '='     INTEGER         # computeArgSize ;

//computeMethod  : min | max | median | mean | std | sum | count | path | paths
//               | connectedComponent | kCore | degree | coreness ;

//min            : MIN      'of' ofList      ('in' inList)? ';' ;
//max            : MAX      'of' ofList      ('in' inList)? ';' ;
//median         : MEDIAN   'of' ofList      ('in' inList)? ';' ;
//mean           : MEAN     'of' ofList      ('in' inList)? ';' ;
//std            : STD      'of' ofList      ('in' inList)? ';' ;
//sum            : SUM      'of' ofList      ('in' inList)? ';' ;
//coreness       : CENTRALITY ('of' ofList)? ('in' inList)? ';' USING 'k-core' (WHERE 'min-k' '=' INTEGER)? ';';
//degree         : CENTRALITY ('of' ofList)? ('in' inList)? ';' USING DEGREE ';';
//connectedComponent    : CLUSTER            ('in' inList)? ';' USING 'connected-component' (WHERE ccParam+)? ';';
//kCore                 : CLUSTER            ('in' inList)? ';' USING 'k-core'              (WHERE kcParam+)? ';';
//path           : PATH    'from' id 'to' id ('in' inList)? ';' ;
//paths          : PATHS   'from' id 'to' id ('in' inList)? ';' ;
//count          : COUNT                     ('in' inList)? ';' ;


//ccParam        : MEMBERS       '='      bool            # ccClusterMembers
//               | SIZE          '='      INTEGER         # ccClusterSize
//               | 'source'      '='      id              # ccStartPoint
//
//
//kcParam        : 'k'           '='      INTEGER         # kValue
//               ;

//ofList         : labelList ;
//inList         : labelList ;
//labelList      : label (',' label)* ;

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
               | 'val' predicate                    # propValue
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

predicate      : '='? value                     # predicateEq
               | '=' VARIABLE                   # predicateVariable
               | '!=' valueOrVar                # predicateNeq
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
               | DEGREE | MEMBERS | SIZE | WHERE
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
PATHS          : 'paths' ;
CLUSTER        : 'cluster' ;
CENTRALITY     : 'centrality' ;
DEGREE         : 'degree' ;
MEMBERS        : 'members' ;
SIZE           : 'size' ;
USING          : 'using' ;
WHERE          : 'where' ;
MATCH          : 'match' ;
INSERT         : 'insert' ;
DEFINE         : 'define' ;
UNDEFINE       : 'undefine' ;
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
// Therefore, we never use an alternation of literals and instead given them proper rule names (e.g. `TRUE | FALSE`).
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
