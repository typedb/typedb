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
 : ID
 | NOT expr          // not expression
 | expr OR expr      // or expression
 | expr AND expr     // and expression
 | BOOLEAN           // boolean expression
 | macro
 ;

replaceStatement
 : REPLACE | macro
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
;

FOR         : 'for';
IF          : 'if';
ELSEIF      : 'elseif';
ELSE        : 'else';
DO          : 'do';
IN          : 'in';

BOOLEAN     : TRUE | FALSE;
TRUE        : 'true';
FALSE       : 'false';

AND         : 'and';
OR          : 'or';
NOT         : 'not';

LPAREN      : '(';
RPAREN      : ')';

ID          : [a-zA-Z0-9_-]+ ('.' [a-zA-Z0-9_-]+ )*;
ID_GRAQL    : '$' ID;
ID_MACRO    : '@' ID;

REPLACE     : ID? '<' ID '>' ID? ;

// hidden channels
WHITESPACE  : [ \t\r\n]                  -> channel(1);
COMMENT     : '#' .*? '\r'? ('\n' | EOF) -> channel(2);