package nosql.neo4j.queries;

import java.util.Iterator;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

public class QueryTwo extends QueryDB{

	public QueryTwo(GraphDatabaseService db) {
		super(db);
	
	}

	@Override
	public void execute(List<String> arguments) {
		ExecutionEngine engine = new ExecutionEngine( db );
		String region=arguments.get(0);
		int size=Integer.parseInt(arguments.get(1));
		String type=arguments.get(2);
        ExecutionResult result;
       /* try ( Transaction ignored = db.beginTx() )
        {*/
        result = engine.execute(
                "match (r:region{name:\""+region+"\"})-[:HAS_NATION]-()-[:HAS_SUPPLIER]-(sup:supplier)-[p:PROVIDE]-(par:part)  where par.type=~'.*"+type+".*' and par.size="+size+" WITH p, min(p.supplyCost)  return l.returnflag,l.linestatus,sum(l.quantity) AS sum_qty,sum(l.extendedprice) AS sum_base_price,sum(l.extendedprice*(1-l.discount))AS sum_disc_price,sum(l.extendedprice*(1+l.tax)) AS sum_charge,avg(l.quantity) AS avg_qty,avg(l.extendedprice) AS avg_price,avg(l.discount) AS avg_disc,count() AS count_order"
        );
        printResults(result);
       // }

		
	}

}
