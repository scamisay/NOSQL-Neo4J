package nosql.neo4j.queries;

import java.util.*;

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
        Calendar calendar = new GregorianCalendar(2014,6,30);
        ExecutionResult result = engine.execute(
                "MATCH (n:`LineItem`)\n" +
                "   WHERE n.L_ShipDate <= "+ calendar.getTime().getTime() +"\n" +
                "        RETURN n.L_RETURNFLAG ,n.L_LINESTATUS, sum(n.L_Quantity) as sum_qty, sum(n.L_ExtendedPrice) as sum_base_price, sum(n.L_ExtendedPrice*(1-n.L_Discount)) as sum_disc_price,\n" +
                "        sum(n.L_ExtendedPrice*(1-n.L_Discount)*(1+n.L_Tax)) as sum_charge,\n" +
                "        avg(n.L_Quantity) as avg_qty, avg(n.L_ExtendedPrice) as avg_price, avg(n.L_Discount)as avg_disc, count(*) as count_order " +
                " ORDER BY n.L_RETURNFLAG, n.L_LINESTATUS"
        );
        printResults(result);

	}

}
