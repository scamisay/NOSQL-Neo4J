package nosql.neo4j.queries;

import nosql.neo4j.loaders.LabelTypes;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

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
        result = engine.execute( "MATCH (r:"+ LabelTypes.Region.getDescription()+"{R_NAME:\""+region+"\"})-[:HAS_NATION]->(n:"+LabelTypes.Nation.getDescription()+")-[:HAS_CUSTOMER]->(c:"+LabelTypes.Customer.getDescription()+")-[:HAS_ORDER]->(o:"+LabelTypes.Order.getDescription()+")-[:HAS_LINEITEM]->(l:"+LabelTypes.LineItem.getDescription()+")<-[:PARTSUPP_HAS_LINEITEM]-(ps:"+LabelTypes.PartSupplier.getDescription()+"),(ps)<-[:SUPPLIER_HAS_PARTSUPP]-(s:"+LabelTypes.Supplier.getDescription()+")<-[:HAS_SUPPLIER]-(n)  where (o.O_ORDERDATE>="+date1+") and (o.O_ORDERDATE<"+date2+") return  n.N_NAME,sum(l.L_EXTENDEDPRICE*(1-l.L_DISCOUNT)) as revenue order by revenue desc" );
        printResults(result, 4);
	}

}
