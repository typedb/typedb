grammar Graql;

queryEOF       : query EOF ;
query          : matchQuery | askQuery | insertQuery | deleteQuery | aggregateQuery | computeQuery ;

matchEOF       : matchQuery EOF ;
askEOF         : askQuery EOF ;
insertEOF      : insertQuery EOF ;
deleteEOF      : deleteQuery EOF ;
aggregateEOF   : aggregateQuery EOF ;
computeEOF     : computeQuery EOF ;

matchQuery     : 'match' patterns modifiers ;
askQuery       : matchQuery 'ask' ;
insertQuery    : matchQuery? 'insert' insertPatterns ;
deleteQuery    : matchQuery 'delete' deletePatterns ;
aggregateQuery : matchQuery 'aggregate' aggregate ;
computeQuery   : 'compute' id ('in' subgraph)? ;

subgraph       : id (',' id) * ;

selectors      : selector (',' selector)* ;
selector       : VARIABLE ('(' (getter ','?)* ')')? ;
getter         : 'isa'      # getterIsa
               | 'id'       # getterId
               | 'value'    # getterValue
               | 'has' id   # getterHas
               | 'lhs'      # getterLhs
               | 'rhs'      # getterRhs
               ;

aggregate      : id argument*                     # customAgg
               | '(' namedAgg (',' namedAgg)* ')' # selectAgg
               ;
argument       : VARIABLE  # variableArgument
               | aggregate # aggregateArgument
               ;
namedAgg       : aggregate 'as' id ;

patterns       : pattern (';' pattern)* ';'? ;
pattern        : variable? property (','? property)*  # varPattern
               | pattern 'or' pattern                 # orPattern
               | '{' patterns '}'                     # andPattern
               ;

property       : edge
               | propId
               | propValFlag
               | propValPred
               | propLhs
               | propRhs
               | propHasFlag
               | propHasPred
               | propResource
               | propRel
               | isAbstract
               | propDatatype
               ;

insertPatterns : insertPattern (';' insertPattern)* ';'? ;
insertPattern  : variable? insert (','? insert)* ;
insert         : edge
               | propId
               | propVal
               | propLhs
               | propRhs
               | insertRel
               | propHas
               | propResource
               | isAbstract
               | propDatatype
               ;

deletePatterns : deletePattern (';' deletePattern)* ';'? ;
deletePattern  : VARIABLE (delete ','?)* delete? ;
delete         : edge | propHasFlag | propHas ;

propId         : 'id' STRING ;

propValFlag    : 'value' ;
propVal        : 'value' value ;
propValPred    : 'value' predicate ;

propLhs        : 'lhs' '{' query '}' ;
propRhs        : 'rhs' '{' query '}' ;

propHasFlag    : 'has' id ;
propHas        : 'has' id value ;
propHasPred    : 'has' id predicate ;

propResource   : 'has-resource' variable ;

propDatatype   : 'datatype' DATATYPE ;

propRel        : '(' roleOpt (',' roleOpt)* ')' ;
insertRel      : '(' roleplayerRole (',' roleplayerRole)* ')' ;

roleOpt        : roleplayerRole | roleplayerOnly ;
roleplayerRole : variable variable ;
roleplayerOnly : variable ;

isAbstract     : 'is-abstract' ;

edge           : 'isa' variable        # isa
               | 'ako' variable        # ako
               | 'has-role' variable   # hasRole
               | 'plays-role' variable # playsRole
               | 'has-scope' variable  # hasScope
               ;

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

modifiers      : (','? modifier)* ;
modifier       : 'select' selectors                               # modifierSelect
               | 'limit' INTEGER                                  # modifierLimit
               | 'offset' INTEGER                                 # modifierOffset
               | 'distinct'                                       # modifierDistinct
               | 'order' 'by' VARIABLE ('(' 'has' id ')')? ORDER? # modifierOrderBy
               ;

// This rule is used for parsing streams of patterns separated by semicolons
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