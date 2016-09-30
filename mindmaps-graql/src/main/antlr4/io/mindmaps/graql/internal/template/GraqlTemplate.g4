grammar GraqlTemplate;

template
 : block EOF
 ;

block
 : (replace | gvar | statement | WORD | STRING | WS | ALLOWABLE)*
 ;

statement
 : forStatement
 | ifStatement
 | macro
 ;

forStatement
 : 'for' WS? '{' WS? resolve WS? '}' WS? 'do' WS? '{' block '}' WS?
 ;

ifStatement
 : ifPartial elsePartial?
 ;

ifPartial
 : 'id' '{' resolve '}' 'do' '{' block '}'
 ;

macro
 : MACRO '{' block '}'
 ;

elsePartial
 : 'else' '{' block '}'
 ;

// no formatting
replace
 : WORD? '<' (WORD|STRING) '>' WORD?
 ;

// get value
resolve
 : (WORD | '.')+ | STRING
 ;

gvar
 : GVAR
 ;

MACRO       : '@' [a-zA-Z0-9_-]+;
GVAR        : '$' [a-zA-Z0-9_-]+;

ALLOWABLE   : ';' | '>' | '<' | ')' | '(' | ',';
WS          : [ \t\r\n]+;
WORD        : [a-zA-Z0-9_-]+;
STRING      : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';

fragment ESCAPE_SEQ : '\\' .;
