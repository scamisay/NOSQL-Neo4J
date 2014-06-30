package nosql.neo4j.queries;

import java.util.List;

import nosql.neo4j.loaders.LabelTypes;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;

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
                "MATCH (r:"+ LabelTypes.Region.getDescription()+"{R_Name:\""+region+"\"})-[:HAS_NATION]->(n:"+LabelTypes.Nation.getDescription()+")-[:HAS_SUPPLIER]-(s:"+LabelTypes.Supplier.getDescription()+")-[:SUPPLIER_HAS_PARTSUPP]->(ps:"+LabelTypes.PartSupplier.getDescription()+")-[:BELONGS_TO_PART]->(p:"+LabelTypes.Part.getDescription()+")  where p.P_TYPE=~'.*"+type+".*' and p.P_SIZE="+size+" WITH ps, min(ps.PS_SUPPLYCOST) AS min_supp_cost, s, n, p RETURN s.S_ACCTBAL, s.S_NAME, n.N_NAME, s.S_ADDRESS,p.P_PARTKEY, p.P_MFGR, s.S_PHONE, s.S_COMMENT ORDER BY s.S_ACCTBAL DESC, n.N_NAME, s.S_NAME, p.P_PARTKEY"
        );
        
        printResults(result, 2);
       		
	}

}
