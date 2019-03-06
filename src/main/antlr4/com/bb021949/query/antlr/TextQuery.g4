grammar TextQuery;

query  : search_command (' '+ '|' ' '+ (stats_command) )? ;

stats_command : 'stats' ' '+ stats_keyword (' '+ 'by' ' '+ VALUE (',' ' '* VALUE)* )? ;

stats_keyword
    : COUNT
    | sumFunction
    | minFunction
    | maxFunction
    ;

sumFunction : SUM '(' VALUE ')' ;
minFunction : MIN '(' VALUE ')' ;
maxFunction : MAX '(' VALUE ')' ;

COUNT : 'count' ;
SUM : 'sum' ;
MIN : 'min' ;
MAX : 'max' ;

search_command : statement (' '+ statement)* ;

statement
    : select
    | keyword
    | encapsulatedQuery
    ;

encapsulatedQuery : '(' ' '* query ' '* ')' ;

select : VALUE ' '* valueOperator ' '* VALUE ;

valueOperator
    : GREATER_THAN
    | GREATER_THAN_OR_EQUAL_TO
    | LESS_THAN
    | LESS_THAN_OR_EQUAL_TO
    | EQUALS
    ;

keyword
    : AND
    | OR
    | NOT
    ;

AND : 'AND' ;
OR : 'OR' ;
NOT : 'NOT' ;

GREATER_THAN : '>' ;
GREATER_THAN_OR_EQUAL_TO : '>=' ;
LESS_THAN : '<' ;
LESS_THAN_OR_EQUAL_TO : '<=' ;
EQUALS : '=' ;

fragment ALLOWED_CHAR
    : '-'
    | '_'
    | '.'
    | '/'
    | ':'
    | [0-9a-zA-Z]
    ;

VALUE : ALLOWED_CHAR+ ;