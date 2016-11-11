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

forStatement
 : FOR LPAREN (ID IN expr | expr) RPAREN DO block
 ;

ifStatement
 : ifPartial elseIfPartial* elsePartial?
 ;

ifPartial
 : IF LPAREN expr RPAREN DO block
 ;

elseIfPartial
 : ELSEIF LPAREN expr RPAREN DO block
 ;

elsePartial
 : ELSE block
 ;

macro
 : ID_MACRO LPAREN expr* RPAREN
 ;

// evaluate and return value
expr
 : ID                     #idExpression
 | NOT expr               #notExpression
 | LPAREN expr RPAREN     #groupExpression
 | EQ expr expr           #eqExpression
 | NEQ expr expr          #notEqExpression
 | OR expr expr           #orExpression
 | AND expr expr          #andExpression
 | GREATER expr expr      #greaterExpression
 | GREATEREQ expr expr    #greaterEqExpression
 | LESS expr expr         #lessExpression
 | LESSEQ expr expr       #lessEqExpression
 | STRING                 #stringExpression
 | BOOLEAN                #booleanExpression
 | NULL                   #nullExpression
 | macro                  #macroExpression
 ;

replaceStatement
 : DOLLAR? (REPLACE | macro)
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
| TRUE
| FALSE
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

NULL        : 'null';
STRING      : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';
BOOLEAN     : TRUE | FALSE;
TRUE        : 'true';
FALSE       : 'false';

EQ          : 'eq';
NEQ         : 'ne';
AND         : 'and';
OR          : 'or';
NOT         : 'not';
GREATER     : 'gt';
GREATEREQ   : 'ge';
LESS        : 'lt';
LESSEQ      : 'le';

LPAREN      : '(';
RPAREN      : ')';
DOLLAR      : '$';
QUOTE       : '"';
SQOUTE      : '\'';

ID          : [a-zA-Z0-9_-]+ ('.' [a-zA-Z0-9_-]+ )*;
ID_GRAQL    : '$' ID;
ID_MACRO    : '@' ID;

REPLACE     : ID? '<' ID '>' ID? ;

// hidden channels
WS          : [ \t\r\n]                  -> channel(1);
COMMENT     : '#' .*? '\r'? ('\n' | EOF) -> channel(2);

fragment ESCAPE_SEQ : '\\' . ;