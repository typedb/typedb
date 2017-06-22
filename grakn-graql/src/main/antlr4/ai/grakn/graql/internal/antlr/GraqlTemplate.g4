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

forStatement : FOR LPAREN (ID IN list | list) RPAREN DO block ;

ifStatement   : ifPartial elseIfPartial* elsePartial? ;
ifPartial     : IF LPAREN bool RPAREN DO block ;
elseIfPartial : ELSEIF LPAREN bool RPAREN DO block ;
elsePartial   : ELSE block ;

macro
 : ID_MACRO LPAREN literal? (',' literal)* RPAREN
 ;

literal       : resolve | macro | nil | string | number | BOOLEAN;
number        : resolve | macro | INT | DOUBLE;
string        : resolve | macro | STRING;
list          : resolve | macro;
nil           : NULL;
bool
 : LPAREN bool RPAREN         #groupExpression
 | NOT bool                   #notExpression
 | literal EQ literal         #eqExpression
 | literal NEQ literal        #notEqExpression
 | bool OR bool               #orExpression
 | bool AND bool              #andExpression
 | number GREATER number      #greaterExpression
 | number GREATEREQ number    #greaterEqExpression
 | number LESS number         #lessExpression
 | number LESSEQ number       #lessEqExpression
 | resolve                    #booleanResolve
 | macro                      #booleanMacro
 | BOOLEAN                    #booleanConstant
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
AT          : '@';
QUOTE       : '"';
SQOUTE      : '\'';

NULL        : 'null';
INT         : [0-9]+;
DOUBLE      : [0-9.]+;
BOOLEAN     : 'true' | 'false' ;
ID          : [a-zA-Z0-9_-]+ ('.' [a-zA-Z0-9_-]+ )*;
STRING      : '"' (~["\\] | ESCAPE_SEQ)* '"' | '\'' (~['\\] | ESCAPE_SEQ)* '\'';

ID_GRAQL    : DOLLAR ID;
ID_MACRO    : AT ID;

// hidden channels
WS          : [ \t\r\n]                  -> channel(1);
COMMENT     : '#' .*? '\r'? ('\n' | EOF) -> channel(2);

fragment ESCAPE_SEQ : '\\' . ;