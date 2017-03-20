grammar GraqlTemplate;

@lexer::members {
    public static final int WHITESPACE = 1;
    public static final int COMMENTS = 2;
}

template
 : blockContents EOF
 ;

block
 : '{' blockContents '}'
 ;

blockContents
 : (statement | graqlVariable | keyword | ID)*
 ;

statement
 : forStatement
 | ifStatement
 | replaceStatement
 ;

forStatement : FOR LPAREN (ID IN expr | expr) RPAREN DO block ;

ifStatement   : ifPartial elseIfPartial* elsePartial? ;
ifPartial     : IF LPAREN expr RPAREN DO block ;
elseIfPartial : ELSEIF LPAREN expr RPAREN DO block ;
elsePartial   : ELSE block ;

macro
 : ID_MACRO LPAREN expr? (',' expr)* RPAREN
 ;

// evaluate and return value
expr
 : LPAREN expr RPAREN     #groupExpression
 | NOT expr               #notExpression
 | expr EQ expr           #eqExpression
 | expr NEQ expr          #notEqExpression
 | expr OR expr           #orExpression
 | expr AND expr          #andExpression
 | expr GREATER expr      #greaterExpression
 | expr GREATEREQ expr    #greaterEqExpression
 | expr LESS expr         #lessExpression
 | expr LESSEQ expr       #lessEqExpression
 | STRING                 #stringExpression
 | INT                    #intExpression
 | DOUBLE                 #doubleExpression
 | BOOLEAN                #booleanExpression
 | NULL                   #nullExpression
 | resolve                #resolveExpression
 | macro                  #macroExpression
 ;

resolve
 : '<' (ID | STRING) '>'
 ;

replaceStatement
 : DOLLAR? (resolve | macro)+
 ;

graqlVariable
 : ID_GRAQL
 ;

keyword
: ','
| ';'
| RPAREN
| LPAREN
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
DOLLAR      : '$';
QUOTE       : '"';
SQOUTE      : '\'';

NULL        : 'null';
INT         : [0-9]+;
DOUBLE      : [0-9.]+;
BOOLEAN     : 'true' | 'false' ;
ID          : [a-zA-Z0-9_-]+ ('.' [a-zA-Z0-9_-]+ )*;
STRING      : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';

ID_GRAQL    : '$' ID;
ID_MACRO    : '@' ID;

// hidden channels
WS          : [ \t\r\n]                  -> channel(1);
COMMENT     : '#' .*? '\r'? ('\n' | EOF) -> channel(2);

fragment ESCAPE_SEQ : '\\' . ;