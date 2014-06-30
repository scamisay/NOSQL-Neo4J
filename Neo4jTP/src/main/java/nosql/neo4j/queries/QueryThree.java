package nosql.neo4j.queries;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

public class QueryThree extends QueryDB{

	public QueryThree(GraphDatabaseService db) {
		super(db);
	
	}

	@Override
	public void execute(List<String> arguments) {
		ExecutionEngine engine = new ExecutionEngine( db );
		String mktsegment=arguments.get(0);
		String date1string=arguments.get(1);
        int date1year=Integer.parseInt(date1string.substring(0, 4));
        int date1month=Integer.parseInt(date1string.substring(5, 7));
        int date1day=Integer.parseInt(date1string.substring(8, 10));
		Calendar calendar = new GregorianCalendar();
        calendar.set(date1year, date1month, date1day);
        long date1=calendar.getTime().getTime();
		String date2string=arguments.get(2);
        int date2year=Integer.parseInt(date2string.substring(0, 4));
        int date2month=Integer.parseInt(date2string.substring(5, 7));
        int date2day=Integer.parseInt(date2string.substring(8, 10));
		Calendar calendar2 = new GregorianCalendar();
        calendar.set(date2year, date2month, date2day);
        long date2=calendar2.getTime().getTime();		
        
        
        ExecutionResult result;
        result = engine.execute( "MATCH (c:Customer{C_MKTSEGMENT:\""+mktsegment+"\"})-[:HAS_ORDER]->(o:Order)-[:HAS_LINEITEM]->(l:LineItem)  WHERE o.O_ORDERDATE<"+date1+" AND l.L_SHIPDATE> "+date2+" RETURN  o.O_ORDERKEY,sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) as revenue,o.O_ORDERDATE,o.O_SHIPPRIORITY ORDER BY revenue desc,o.O_ORDERDATE" );
        printResults(result, 3);


		
	}

}
