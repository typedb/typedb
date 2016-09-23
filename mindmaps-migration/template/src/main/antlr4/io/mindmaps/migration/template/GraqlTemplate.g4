grammar GraqlTemplate;

@lexer::members {
    public static final int WHITESPACE = 1;
    public static final int COMMENTS = 2;
}

template
 : block EOF
 ;

block
 : (statement | replace | graql)+
 ;

statement
 : forStatement
 | nullableStatement
 | noescpStatement
 ;

forStatement
 : LPAREN FOR variable IN resolve RPAREN LBRACKET block RBRACKET
 ;

nullableStatement
 : NULLABLE
 ;

noescpStatement
 : NOESCP
 ;

variable    : IDENTIFIER | GRAQLVAR;
resolve     : IDENTIFIER;
replace     : IDENTIFIER;

graql
 : variable
 | IN
 | FOR
 | NULLABLE
 | NOESCP
 | LPAREN
 | RPAREN
 | LBRACKET
 | RBRACKET
 | NOT_WS;

// reserved
FOR         : 'for' ;
IN          : 'in' ;
NULLABLE    : 'nullable' ;
NOESCP      : 'noescp' ;

IDENTIFIER  : '%' [a-zA-Z0-9_-]+;
GRAQLVAR    : '$' [a-zA-S0-9_-]+;
LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '{';
RBRACKET    : '}';
NOT_WS      : ~[ \t\r\n];

WS : [ \t\r\n] -> channel(1) ;
