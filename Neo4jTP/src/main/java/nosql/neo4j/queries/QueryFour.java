package nosql.neo4j.queries;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

public class QueryFour extends QueryDB{

	public QueryFour(GraphDatabaseService db) {
		super(db);

	}

	@Override
	public void execute(List<String> arguments) {
		ExecutionEngine engine = new ExecutionEngine( db );
		String region=arguments.get(0);
		String date1string=arguments.get(1);
		System.out.println(date1string);
        int date1year=Integer.parseInt(date1string.substring(0, 4));
        int date1month=Integer.parseInt(date1string.substring(5, 7));
        int date1day=Integer.parseInt(date1string.substring(8, 10));
        System.out.println(date1year+" "+date1month+" "+date1day);
		Calendar calendar = new GregorianCalendar();
        calendar.set(date1year, date1month, date1day);
        
        long date1=Math.abs(calendar.getTime().getTime());
		calendar.add(Calendar.YEAR, 1);
        long date2=calendar.getTime().getTime();	
        ExecutionResult result;
        try ( Transaction ignored = db.beginTx() )
        {
        	String rows = "";
            result = engine.execute( "match (r:region{name:\""+region+"\"})-[:HAS_NATION]->(n:nation)-[:HAS_CUSTOMER]->(c:customer)-[:HAS_ORDER]->(o:order)-[:HAS_LINEITEM]->(l:lineItem)-[:SUPPLIED_BY]->(s:supplier),(s:supplier)<-[:HAS_NATION]-(n)  where (o.orderDate>="+date1+") and (o.orderDate<"+date2+") return  n.name,sum(l.extendedPrice*(1-l.discount)) as revenue order by revenue desc" );
            // END SNIPPET: execute
            // START SNIPPET: items
           System.out.println(result.iterator().hasNext());
            for ( Map<String, Object> row : result )
            {
                for ( Entry<String, Object> column : row.entrySet() )
                {
                    rows += column.getKey() + ": " + column.getValue() + "; ";
                }
                rows += "\n";
            }
            
            
            // END SNIPPET: items
        }

		
	}

}
