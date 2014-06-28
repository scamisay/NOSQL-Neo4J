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
        int date1year=Integer.parseInt(date1string.substring(0, 3));
        int date1month=Integer.parseInt(date1string.substring(5, 6));
        int date1day=Integer.parseInt(date1string.substring(8, 9));
		Calendar calendar = new GregorianCalendar();
        calendar.set(date1year, date1month, date1day);
        long date1=calendar.getTime().getTime();
		String date2string=arguments.get(2);
        int date2year=Integer.parseInt(date2string.substring(0, 3));
        int date2month=Integer.parseInt(date2string.substring(5, 6));
        int date2day=Integer.parseInt(date2string.substring(8, 9));
		Calendar calendar2 = new GregorianCalendar();
        calendar.set(date2year, date2month, date2day);
        long date2=calendar2.getTime().getTime();		
        
        
        ExecutionResult result;
        try ( Transaction ignored = db.beginTx() )
        {
            result = engine.execute( "start c=node:customer{mktsegment:\""+mktsegment+"\"} match c-[:HASORDER]->(o:order)-[:HAS_LINEITEM]-(l:lineItem)  where o.orderdate<"+date1+" and l.shipdate> "+date2+"yCost=min(p.supplyCost) return  l.orderkey,sum(l.extendedprice*(1-l.discount)) as revenue,o.orderdate,o.shippriority order by revenue desc,o.orderdate" );
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
