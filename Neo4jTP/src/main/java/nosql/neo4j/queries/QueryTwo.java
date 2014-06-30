package nosql.neo4j.queries;

import java.util.Iterator;
import java.util.List;

import nosql.neo4j.loaders.LabelTypes;
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
       
        result = engine.execute(
                "MATCH (r:"+ LabelTypes.Region.getDescription()+"{R_Name:\""+region+"\"})-[:HAS_NATION]->(n:"+LabelTypes.Nation.getDescription()+")-[:HAS_SUPPLIER]-(s:"+LabelTypes.Supplier.getDescription()+")-[:SUPPLIER_HAS_PARTSUPP]->(ps:PartSupplier)-[:BELONGS_TO_PART]->(p:Part)  where p.P_Type=~'.*"+type+".*' and p.P_Size="+size+" WITH ps, min(ps.PS_SupplyCost) AS min_supp_cost, s, n, p RETURN s.S_AcctBal, s.S_Name, n.N_Name, s.S_Address,p.P_PartKey, p.P_Mfgr, s.S_Phone, s.S_Comment ORDER BY s.S_AcctBal DESC, n.N_Name, s.S_Name, p.P_PartKey"
        );
        
        printResults(result);
       		
	}

}
