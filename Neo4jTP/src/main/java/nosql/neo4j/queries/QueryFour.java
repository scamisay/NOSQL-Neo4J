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

public class QueryFour extends QueryDB{

	public QueryFour(GraphDatabaseService db) {
		super(db);

	}

	@Override
	public void execute(List<String> arguments) {
		ExecutionEngine engine = new ExecutionEngine( db );
		String region=arguments.get(0);
		String date1string=arguments.get(1);
        int date1year=Integer.parseInt(date1string.substring(0, 3));
        int date1month=Integer.parseInt(date1string.substring(5, 6));
        int date1day=Integer.parseInt(date1string.substring(8, 9));
		Calendar calendar = new GregorianCalendar();
        calendar.set(date1year, date1month, date1day);
        
        long date1=calendar.getTime().getTime();
		calendar.add(Calendar.YEAR, 1);
        long date2=calendar.getTime().getTime();	
        ExecutionResult result;
        try ( Transaction ignored = db.beginTx() )
        {
            result = engine.execute( "start r=node:region{name:\""+region+"\"} match r-[:HASNATION]->(n:nation)-[:HAS_CUSTOMER]->(c:customer)-[:HAS_ORDER]->(o:order)-[:HAS_LINEITEM]->(l:listItem)-[:SUPPLIED_BY]->(s:supplier)-[:HAS_NATION]-(n)  where o.orderdate>="+date1+" and o.orderdate<"+date2+"return  n.name,sum(l.extendedprice*(1-l.discount)) as revenue order by revenue desc" );
            // END SNIPPET: execute
            // START SNIPPET: items
            Iterator<Node> n_column = result.columnAs( "n" );
            for ( Node node : IteratorUtil.asIterable( n_column ) )
            {
                // note: we're grabbing the name property from the node,
                // not from the n.name in this case.
                //nodeResult = node + ": " + node.getProperty( "name" );
            }
            // END SNIPPET: items
        }

		
	}

}
