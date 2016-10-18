grammar Graql;

queryEOF       : query EOF ;
query          : matchQuery | askQuery | insertQuery | deleteQuery | aggregateQuery | computeQuery ;

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
insertQuery    : matchQuery? insert varPatterns ;
deleteQuery    : matchQuery 'delete' varPatterns ;
aggregateQuery : matchQuery 'aggregate' aggregate ';' ;
computeQuery   : 'compute' id ('of' statTypes)? ('in' subgraph)? ';' ;

statTypes      : idList ;
subgraph       : idList ;
idList         : id (',' id)* ;

aggregate      : id argument*                     # customAgg
               | '(' namedAgg (',' namedAgg)* ')' # selectAgg
               ;
argument       : VARIABLE  # variableArgument
               | aggregate # aggregateArgument
               ;
namedAgg       : aggregate 'as' id ;

patterns       : (pattern ';')+ ;
pattern        : varPattern                    # varPatternCase
               | pattern 'or' pattern          # orPattern
               | '{' patterns '}'              # andPattern
               ;

varPatterns    : (varPattern ';')+ ;
varPattern     : variable | variable? property (','? property)* ;

property       : 'isa' variable                   # isa
               | 'ako' variable                   # ako
               | 'has-role' variable              # hasRole
               | 'plays-role' variable            # playsRole
               | 'has-scope' variable             # hasScope
               | 'id' STRING                      # propId
               | 'value' predicate?               # propValue
               | 'lhs' '{' patterns '}'           # propLhs
               | 'rhs' '{' patterns '}'           # propRhs
               | 'has' id (predicate | VARIABLE)? # propHas
               | 'has-resource' variable          # propResource
               | '(' casting (',' casting)* ')'   # propRel
               | 'is-abstract'                    # isAbstract
               | 'datatype' DATATYPE              # propDatatype
               | 'regex' REGEX                    # propRegex
               ;

casting        : variable (':' variable)?
               | variable variable         {notifyErrorListeners("expecting {',', ':'}");};

variable       : id | VARIABLE ;

predicate      : '='? value                # predicateEq
               | '!=' value                # predicateNeq
               | '>' value                 # predicateGt
               | '>=' value                # predicateGte
               | '<' value                 # predicateLt
               | '<=' value                # predicateLte
               | 'contains' STRING         # predicateContains
               | REGEX                     # predicateRegex
               | predicate 'and' predicate # predicateAnd
               | predicate 'or' predicate  # predicateOr
               | '(' predicate ')'         # predicateParens
               ;
value          : STRING  # valueString
               | INTEGER # valueInteger
               | REAL    # valueReal
               | BOOLEAN # valueBoolean
               ;

// These rules are used for parsing streams of patterns separated by semicolons
insert         : 'insert' ;
patternSep     : pattern ';' ;

id             : ID | STRING ;

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