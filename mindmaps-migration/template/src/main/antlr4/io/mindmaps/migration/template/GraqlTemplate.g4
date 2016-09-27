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
 : LPAREN FOR variable IN resolve RPAREN LBRACKET block RBRACKET
 ;

ifStatement
 : LPAREN IF resolve RPAREN LBRACKET block RBRACKET
 ;

element     : TVAR;
resolve     : TVAR (DVAR)*;
replace     : resolve;

type        : TVAR | CVAR | GVAR;

gvar : GVAR VAR;
tvar : GVAR{0,1} TVAR VAR (DVAR VAR)*;

graql
 : gvar | tvar
 | IN
 | FOR
 | LPAREN
 | RPAREN
 | NOT_WS
 ;

// reserved
FOR         : 'for' ;
IN          : 'in' ;
IF          : 'if' ;

VAR         : [a-zA-Z0-9_-]+;
GVAR        : '$';
TVAR        : '%';
DVAR        : '.';
CVAR        : GVAR TVAR;

LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '{';
RBRACKET    : '}';
NOT_WS      : ~[ \t\r\n];

WS : [ \t\r\n] -> channel(1) ;
