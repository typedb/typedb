grammar GraqlTemplate;

@lexer::members {
    public static final int WHITESPACE = 1;
    public static final int COMMENTS = 2;
}

template
 : block EOF
 ;

block
 : (statement | replace | gvar | WORD | ALLOWABLE)*
 ;

statement
 : forStatement
 | ifStatement
 | macro
 ;

forStatement
 : 'for' '{' expression '}' 'do' '{' block '}'
 ;

ifStatement
 : ifPartial elsePartial?
 ;

ifPartial
 : 'if' '{' expression '}' 'do' '{' block '}'
 ;

elsePartial
 : 'else' '{' block '}'
 ;

macro
 : MACRO '{' block '}'
 ;

expression
 : (WORD | '.')+
 ;

replace
 : DOLLAR? REPLACE
 ;

gvar
 : GVAR
 ;

WORD        : [a-zA-Z0-9_-]+;
DOLLAR      : '$';
MACRO       : '@' WORD;
GVAR        : '$' WORD;
REPLACE     : WORD? '<' (WORD | '.')+ '>' WORD? ;
ALLOWABLE   : ';' | ')' | '(' | ',' | ':';

WS : [ \t\r\n] -> channel(1) ;