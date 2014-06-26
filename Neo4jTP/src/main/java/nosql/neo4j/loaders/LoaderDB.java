package nosql.neo4j.loaders;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public abstract class LoaderDB {

	protected GraphDatabaseService db;

	public LoaderDB(String db_path) {
	
		this.db = new GraphDatabaseFactory().newEmbeddedDatabase(db_path);
	
	}
	
	public LoaderDB(GraphDatabaseService db){
		super();
		this.db=db;
	}
	
	public abstract void loadData();

	
}
