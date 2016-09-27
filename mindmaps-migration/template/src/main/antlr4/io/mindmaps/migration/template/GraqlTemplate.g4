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
 : LPAREN FOR tvar IN resolve RPAREN LBRACKET block RBRACKET
 ;

ifStatement
 : LPAREN IF resolve RPAREN LBRACKET block RBRACKET
 ;

gvar : GVAR VAR;
tvar : TVAR VAR (DVAR VAR)*;

resolve     : tvar;
replace     : resolve;

graql
 : gvar | replace
 | IN
 | IF
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

LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '{';
RBRACKET    : '}';
NOT_WS      : ~[ \t\r\n];

WS : [ \t\r\n] -> channel(1) ;
