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
 : FOR LBRACKET variable RBRACKET DO LBRACKET block RBRACKET
 ;

ifStatement
 : ifPartial elifPartial* elsePartial?
 ;

ifPartial
 : IF LBRACKET variable RBRACKET DO LBRACKET block RBRACKET
 ;

elifPartial
 : ELIF LBRACKET variable RBRACKET DO LBRACKET block RBRACKET
 ;

elsePartial
 : ELSE LBRACKET block RBRACKET
 ;

replace
 : LTRIANGLE variable RTRIANGLE
 ;

variable
 : NOT_WS+
 ;

graql
 : GVAR
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
