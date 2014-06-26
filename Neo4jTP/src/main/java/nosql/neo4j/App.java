package nosql.neo4j;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import nosql.neo4j.loaders.LoaderNation;
import nosql.neo4j.loaders.LoaderRegion;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final String DB_PATH = "target/neo4j-hello-db";
	
	
    public static void main( String[] args )
    {
    	GraphDatabaseService graphDb;
    	graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
    	
    	registerShutdownHook( graphDb );
    	System.out.println( "Hello World!" );
    	
    	LoaderRegion regionLoader=new LoaderRegion(graphDb);
    	LoaderNation nationLoader=new LoaderNation(graphDb);
    	regionLoader.loadData();
    	nationLoader.loadData();
    	Transaction tx=graphDb.beginTx();
    	Iterable<Node> iterable=graphDb.findNodesByLabelAndProperty(DynamicLabel.label("region"), "name", "Oceanía");
    	Iterator<Node> it=iterable.iterator();
    	while(it.hasNext()){
    		Node node=it.next();
    		System.out.println((String)node.getProperty("name")+"  "+(String)node.getProperty("comment"));
    	}
    	
    	tx.success();
    	System.out.println( "Goodbye Cruel World!" );
    	graphDb.shutdown();
    }
    
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    
    private static enum RelTypes implements RelationshipType
    {
        KNOWS
    }
    
    
    
}
