grammar Graql;

queryList : queryElems ;

// These rules exist to parse match-insert queries unambiguously
queryElems     : matchQuery (queryNotInsert queryElems)?  # queryElemsNotInsert
               | queryNotMatch queryElems                 # queryElemsNotMatch
               | EOF                                      # queryElemsEOF
               ;
queryNotMatch  : insertQuery | simpleQuery ;
queryNotInsert : matchInsert | matchQuery | simpleQuery ;

queryEOF       : query EOF ;
query          : matchQuery | insertQuery | simpleQuery ;
simpleQuery    : askQuery | deleteQuery | aggregateQuery | computeQuery ;

matchEOF       : matchQuery EOF ;
askEOF         : askQuery EOF ;
insertEOF      : insertQuery EOF ;
deleteEOF      : deleteQuery EOF ;
aggregateEOF   : aggregateQuery EOF ;
computeEOF     : computeQuery EOF ;

matchQuery     : 'match' patterns                                 # matchBase
               | matchQuery 'select' VARIABLE (',' VARIABLE)* ';' # matchSelect
               | matchQuery 'limit' INTEGER                   ';' # matchLimit
               | matchQuery 'offset' INTEGER                  ';' # matchOffset
               | matchQuery 'distinct'                        ';' # matchDistinct
               | matchQuery 'order' 'by' VARIABLE ORDER?      ';' # matchOrderBy
               ;

askQuery       : matchQuery 'ask' ';' ;
insertQuery    : matchInsert | insertOnly ;
insertOnly     : insert varPatterns ;
matchInsert    : matchQuery insert varPatterns ;
deleteQuery    : matchQuery 'delete' varPatterns ;
aggregateQuery : matchQuery 'aggregate' aggregate ';' ;
computeQuery   : 'compute' computeMethod ;

computeMethod  : min | max | median | mean | std | sum | count | path | cluster | degrees ;

min            : MIN      'of' ofList      ('in' inList)? ';' ;
max            : MAX      'of' ofList      ('in' inList)? ';' ;
median         : MEDIAN   'of' ofList      ('in' inList)? ';' ;
mean           : MEAN     'of' ofList      ('in' inList)? ';' ;
std            : STD      'of' ofList      ('in' inList)? ';' ;
sum            : SUM      'of' ofList      ('in' inList)? ';' ;
degrees        : DEGREES ('of' ofList)?    ('in' inList)? ';' ;
cluster        : CLUSTER                   ('in' inList)? ';' (MEMBERS ';')? (SIZE INTEGER ';')? ;
path           : PATH    'from' id 'to' id ('in' inList)? ';' ;
count          : COUNT                     ('in' inList)? ';' ;

ofList         : nameList ;
inList         : nameList ;
nameList       : name (',' name)* ;

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
               | 'sub' variable                     # sub
               | 'has-role' variable                # hasRole
               | 'plays-role' variable              # playsRole
               | 'has-scope' VARIABLE               # hasScope
               | 'id' id                            # propId
               | 'type-name' name                   # propName
               | 'value' predicate?                 # propValue
               | 'lhs' '{' patterns '}'             # propLhs
               | 'rhs' '{' varPatterns '}'          # propRhs
               | 'has' name? VARIABLE               # propHasVariable
               | 'has' name (predicate | VARIABLE)? # propHas
               | 'has-resource' variable            # propResource
               | 'has-key' variable                 # propKey
               | '(' casting (',' casting)* ')'     # propRel
               | 'plays' variable                   # plays
               | 'is-abstract'                      # isAbstract
               | 'datatype' DATATYPE                # propDatatype
               | 'regex' REGEX                      # propRegex
               | '!=' variable                      # propNeq
               ;

casting        : variable (':' VARIABLE)?
               | variable VARIABLE         {notifyErrorListeners("expecting {',', ':'}");};

variable       : name | VARIABLE ;

predicate      : '='? value                # predicateEq
               | '!=' value                # predicateNeq
               | '>' value                 # predicateGt
               | '>=' value                # predicateGte
               | '<' value                 # predicateLt
               | '<=' value                # predicateLte
               | 'contains' STRING         # predicateContains
               | REGEX                     # predicateRegex
               ;
value          : VARIABLE # valueVariable
               | STRING   # valueString
               | INTEGER  # valueInteger
               | REAL     # valueReal
               | BOOLEAN  # valueBoolean
               ;

// These rules are used for parsing streams of patterns separated by semicolons
insert         : 'insert' ;
patternSep     : pattern ';' ;
batchPattern   : 'match' | 'insert' | patternSep ;

name           : identifier ;
id             : identifier ;

// Some keywords can also be used as identifiers
identifier     : ID | STRING
               | MIN | MAX| MEDIAN | MEAN | STD | SUM | COUNT | PATH | CLUSTER
               | DEGREES | MEMBERS | SIZE
               ;

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
DEGREES        : 'degrees' ;
MEMBERS        : 'members' ;
SIZE           : 'size' ;

DATATYPE       : 'long' | 'double' | 'string' | 'boolean' ;
ORDER          : 'asc' | 'desc' ;
BOOLEAN        : 'true' | 'false' ;
VARIABLE       : '$' [a-zA-Z0-9_-]+ ;
ID             : [a-zA-Z_] [a-zA-Z0-9_-]* ;
STRING         : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';
REGEX          : '/' (~'/' | '\\/')* '/' ;
INTEGER        : ('+' | '-')? [0-9]+ ;
REAL           : ('+' | '-')? [0-9]+ '.' [0-9]+ ;

fragment ESCAPE_SEQ : '\\' . ;

COMMENT : '#' .*? '\r'? ('\n' | EOF) -> skip ;

WS : [ \t\r\n]+ -> skip ;

// Unused lexer rule to help with autocomplete on variable names
DOLLAR : '$' ;
