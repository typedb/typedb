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
 ;

forStatement
 : LPAREN FOR element IN resolve RPAREN LBRACKET block RBRACKET
 ;

element     : TVAR;
resolve     : TVAR (DVAR)*;
replace     : resolve;
variable    : replace | DOLLAR resolve | GVAR;

graql
 : variable
 | IN
 | FOR
 | DOLLAR
 | PERCENT
 | DOT
 | LPAREN
 | RPAREN
 | LBRACKET
 | RBRACKET
 | NOT_WS
 ;

// reserved
FOR         : 'for' ;
IN          : 'in' ;

GVAR        : '$' [a-zA-Z0-9_-]+;
TVAR        : '%' [a-zA-Z0-9_-]+;
DVAR        : '.' [a-zA-Z0-9_-]+;
LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '{';
RBRACKET    : '}';
DOLLAR      : '$';
PERCENT     : '%';
DOT         : '.';
NOT_WS      : ~[ \t\r\n];

WS : [ \t\r\n] -> channel(1) ;
