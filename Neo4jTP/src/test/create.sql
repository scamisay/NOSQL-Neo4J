//
create (l1:LineItem {shipDate:19970716 ,lineNumber:'15'})

Books
create (liBook1:LineItem {lineNumber:1, quantity:12, expendedPrice: 37.3, discount: 3.2,
 tax: 3.73, returnFlag:'r', lineStatus: 'r', shipDate:20140525,
 commitDate:20140525 , receipDate:20140530})

items:
schema(LINENUMBER,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,
COMMITDATE,RECEIPTDATE,SHIPINSTRUCT,SHIPMODE,COMMENT)
-libros(7)
-relojes(4)
-televisores(2)
-tablets(4)

https://github.com/recogemoras/CBDE/blob/master/Neo4j-Normalized/src/DataInserter.java