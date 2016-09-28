grammar GraqlTemplate;

@lexer::members {
    public static final int WHITESPACE = 1;
    public static final int COMMENTS = 2;
}

template
 : block EOF
 ;

block
 : (statement | graql)+
 ;

statement
 : forStatement
 | ifStatement
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

elsePartial
 : ELSE LBRACKET block RBRACKET
 ;

replace
 : LTRIANGLE resolve RTRIANGLE
 ;

resolve
 : (DO | NOT_WS | '.')+
 ;

gvar
 : GVAR | '$' replace
 ;

graql
 : gvar
 | replace
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
