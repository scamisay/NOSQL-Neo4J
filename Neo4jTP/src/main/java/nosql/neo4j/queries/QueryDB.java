package nosql.neo4j.queries;

import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;

public abstract class QueryDB {

	GraphDatabaseService db;
	
	public QueryDB(GraphDatabaseService db){
		this.db=db;
	}
	
	public abstract void execute(List<String> arguments);

    public void printResults(ExecutionResult result){
        for ( Map<String, Object> row : result )
        {
            String rows = "";
            for ( Map.Entry<String, Object> column : row.entrySet() )
            {
                rows += column.getKey() + ": " + column.getValue() + "; ";
            }
            rows += "\n";
            System.out.print(rows);
        }
    }
	
}
