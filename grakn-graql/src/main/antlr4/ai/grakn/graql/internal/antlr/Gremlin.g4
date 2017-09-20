grammar Gremlin;

traversal : expr EOF ;

expr : ID                #idExpr
     | list              #listExpr
     | step              #stepExpr
     | ID '.' call       #methodExpr
     | '!' expr          #negExpr
     | '~' expr          #squigglyExpr
     | '{' (mapEntry (',' mapEntry)*)? '}' #mapExpr
     ;

mapEntry : ID '=' expr ;

step : call ('@' ids)? ;

call : ID ('(' (expr (',' expr)*)? ')')? ;

list : '[' (expr (',' expr)*)? ']' ;

ids : '[' (ID (',' ID) *)? ']' ;

ID : '!'? [a-zA-Z_0-9-]+ ;

WS : [ \t\r\n]+ -> skip ;
