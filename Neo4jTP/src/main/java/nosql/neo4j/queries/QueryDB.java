package nosql.neo4j.queries;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;

public abstract class QueryDB {

	GraphDatabaseService db;
	
	public QueryDB(GraphDatabaseService db){
		this.db=db;
	}
	
	public abstract void execute(List<String> arguments);
	
	
}
