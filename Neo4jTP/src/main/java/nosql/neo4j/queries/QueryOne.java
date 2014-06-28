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

public class QueryOne extends QueryDB{

	
	public QueryOne(GraphDatabaseService db) {
		super(db);
	}

	@Override
	public void execute(List<String> arguments) {
		 ExecutionEngine engine = new ExecutionEngine( db );
		 String date1string=arguments.get(0);
	        int date1year=Integer.parseInt(date1string.substring(0, 3));
	        int date1month=Integer.parseInt(date1string.substring(5, 6));
	        int date1day=Integer.parseInt(date1string.substring(8, 9));
			Calendar calendar = new GregorianCalendar();
	        calendar.set(date1year, date1month, date1day);
	        long date1=calendar.getTime().getTime();
	        ExecutionResult result;
	        try ( Transaction ignored = db.beginTx() )
	        {
	            result = engine.execute( "start l=node:lineItem where l.shipDate <="+date1+" return l.returnflag,l.linestatus,sum(l.quantity) AS sum_qty,sum(l.extendedprice) AS sum_base_price,sum(l.extendedprice*(1-l.discount))AS sum_disc_price,sum(l.extendedprice*(1+l.tax)) AS sum_charge,avg(l.quantity) AS avg_qty,avg(l.extendedprice) AS avg_price,avg(l.discount) AS avg_disc,count() AS count_order order by l.returnflag,l.linestatus" );
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