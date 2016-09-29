grammar GraqlTemplate;

@lexer::members {
    public static final int WHITESPACE = 1;
    public static final int COMMENTS = 2;
}

template
 : block EOF
 ;

block
 : (statement | graql)*
 ;

statement
 : forStatement
 | ifStatement
 | macro
 ;

forStatement
 : FOR LBRACKET resolve RBRACKET DO LBRACKET block RBRACKET
 ;

ifStatement
 : ifPartial elsePartial?
 ;

ifPartial
 : IF LBRACKET resolve RBRACKET DO LBRACKET block RBRACKET
 ;

macro
 : MACRO LBRACKET block RBRACKET
 ;

elsePartial
 : ELSE LBRACKET block RBRACKET
 ;

replaceVal
 : LTRIANGLE resolve RTRIANGLE
 ;

replaceVar
 : LTRIANGLE resolve RTRIANGLE
 ;

resolve
 : (DO | NOT_WS | '.')+
 ;

gvar
 : GVAR | '$' replaceVar
 ;

graql
 : gvar
 | replaceVal
 | IF
 | FOR
 | DO
 | LPAREN
 | RPAREN
 | NOT_WS
 ;

// reserved
FOR         : 'for' ;
IF          : 'if' ;
DO          : 'do';
ELIF        : 'elif';
ELSE        : 'else';

MACRO       : '@' [a-zA-Z0-9_-]+;
GVAR        : '$' [a-zA-Z0-9_-]+;
DVAR        : '.';

LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '{';
RBRACKET    : '}';
LTRIANGLE   : '<';
RTRIANGLE   : '>';
NOT_WS      : ~[ \t\r\n];

WS : [ \t\r\n] -> channel(1) ;