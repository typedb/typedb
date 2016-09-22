grammar GraqlTemplate;

@lexer::members {
    public static final int WHITESPACE = 1;
    public static final int COMMENTS = 2;
}

template
 : block EOF
 ;

block
 : (statement | filler)+
 ;

statement
 : forStatement
 | nullableStatement
 | noescpStatement
 ;

forStatement
 : '(' FOR variable 'in' resolve ')' LBRACKET block RBRACKET
 ;

nullableStatement
 : NULLABLE
 ;

noescpStatement
 : NOESCP
 ;

filler      : (statement | replace | any)+;

variable    : IDENTIFIER;
resolve     : IDENTIFIER;
replace     : IDENTIFIER;

any
 : IN
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
LPAREN      : '(';
RPAREN      : ')';
LBRACKET    : '{';
RBRACKET    : '}';
NOT_WS      : ~[ \t\r\n];

WS : [ \t\r\n] -> channel(1) ;
