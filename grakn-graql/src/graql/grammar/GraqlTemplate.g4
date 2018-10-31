grammar GraqlTemplate;

template
 : blockContents EOF
 ;

block
 : '{' blockContents '}'
 ;

//TODO remove escaped expression and handle escaping here
blockContents
 : (statement | escapedExpression | var | keyword | ID)*
 ;

statement
 : forInStatement
 | forEachStatement
 | ifStatement
 ;

forInStatement   : FOR LPAREN ID IN list RPAREN DO block ;
forEachStatement : FOR LPAREN list RPAREN DO block ;

ifStatement   : ifPartial elseIfPartial* elsePartial? ;
ifPartial     : IF LPAREN bool RPAREN DO block ;
elseIfPartial : ELSEIF LPAREN bool RPAREN DO block ;
elsePartial   : ELSE block ;

expression    : untypedExpression | nil | string | number | BOOLEAN;
number        : untypedExpression | int_ | double_;
int_          : untypedExpression | INT;
double_       : untypedExpression | DOUBLE;
string        : untypedExpression | STRING;
list          : untypedExpression ;
nil           : NULL;
bool
 : LPAREN bool RPAREN         #groupExpression
 | NOT bool                   #notExpression
 | expression EQ expression   #eqExpression
 | expression NEQ expression  #notEqExpression
 | bool OR bool               #orExpression
 | bool AND bool              #andExpression
 | number GREATER number      #greaterExpression
 | number GREATEREQ number    #greaterEqExpression
 | number LESS number         #lessExpression
 | number LESSEQ number       #lessEqExpression
 | untypedExpression          #booleanExpression
 | BOOLEAN                    #booleanConstant
 ;

escapedExpression : untypedExpression;
untypedExpression
 : '<' id accessor* '>'                                   #idExpression
 | ID_MACRO LPAREN expression? (',' expression)* RPAREN   #macroExpression
 ;

accessor
 : '.' id            #mapAccessor
 | '[' int_ ']'      #listAccessor
 ;

id: ID | STRING;

var
 : DOLLAR (untypedExpression)+   #varResolved
 | VAR_GRAQL                     #varLiteral
 ;

keyword
: ','
| ';'
| RPAREN
| LPAREN
| LBR
| RBR
| ':'
| FOR
| IF
| ELSEIF
| ELSE
| DO
| IN
| BOOLEAN
| AND
| OR
| NOT
| NULL
| EQ
| NEQ
| GREATER
| GREATEREQ
| LESS
| LESSEQ
| QUOTE
| SQOUTE
| STRING
;

FOR         : 'for';
IF          : 'if';
ELSEIF      : 'elseif';
ELSE        : 'else';
DO          : 'do';
IN          : 'in';

EQ          : '=';
NEQ         : '!=';
AND         : 'and';
OR          : 'or';
NOT         : 'not';
GREATER     : '>';
GREATEREQ   : '>=';
LESS        : '<';
LESSEQ      : '<=';

LPAREN      : '(';
RPAREN      : ')';
LBR         : '[';
RBR         : ']';
DOLLAR      : '$';
AT          : '@';
QUOTE       : '"';
SQOUTE      : '\'';

NULL        : 'null';
INT         : [0-9]+;
DOUBLE      : [0-9.]+;
BOOLEAN     : 'true' | 'false' ;
ID          : [a-zA-Z0-9_-]+;
STRING      : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';

VAR_GRAQL   : DOLLAR ID;
ID_MACRO    : AT ID;

// hidden channels
WS          : [ \t\r\n]                  -> channel(HIDDEN);
COMMENT     : '#' .*? '\r'? ('\n' | EOF) -> channel(HIDDEN);

fragment ESCAPE_SEQ : '\\' . ;