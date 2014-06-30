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
		
        int date1year=Integer.parseInt(date1string.substring(0, 4));
        int date1month=Integer.parseInt(date1string.substring(5, 7));
        int date1day=Integer.parseInt(date1string.substring(8, 10));
        Calendar calendar = new GregorianCalendar();
        calendar.set(date1year, date1month, date1day);
        
        long date1=Math.abs(calendar.getTime().getTime());
		calendar.add(Calendar.YEAR, 1);
        long date2=calendar.getTime().getTime();	
        ExecutionResult result;
    /*    try ( Transaction ignored = db.beginTx() )
        {*/
        	result = engine.execute( "MATCH (r:Region{R_Name:\""+region+"\"})-[:HAS_NATION]->(n:Nation)-[:HAS_CUSTOMER]->(c:Customer)-[:HAS_ORDER]->(o:Order)-[:HAS_LINEITEM]->(l:LineItem)<-[:PARTSUPP_HAS_LINEITEM]-(ps:PartSupplier),(ps)<-[:SUPPLIER_HAS_PARTSUPP]-(s:Supplier)<-[:HAS_SUPPLIER]-(n)  where (o.O_OrderDate>="+date1+") and (o.O_OrderDate<"+date2+") return  n.N_Name,sum(l.L_ExtendedPrice*(1-l.L_Discount)) as revenue order by revenue desc" );
        	 printResults(result);
             
            
        //}

		
	}

}
