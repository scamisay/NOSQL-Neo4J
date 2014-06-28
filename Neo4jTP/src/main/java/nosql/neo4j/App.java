package nosql.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import nosql.neo4j.loaders.GraphCreator;
import nosql.neo4j.loaders.LoaderNation;
import nosql.neo4j.loaders.LoaderPart;
import nosql.neo4j.loaders.LoaderRegion;
import nosql.neo4j.loaders.LoaderSupplier;

import nosql.neo4j.queries.QueryOne;
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
import org.neo4j.kernel.impl.util.FileUtils;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final String DB_PATH = "target/neo4j-hello-db";

    private static GraphCreator graphCreator = new GraphCreator();
	
	
    public static void main( String[] args )
    {
        /*elimino y creo la base*/
        clearDb();
    	GraphDatabaseService graphDb;
    	graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
    	registerShutdownHook( graphDb );

System.out.println( "Hello World!" );

        graphCreator.initialInsert(graphDb);

        QueryOne queryOne = new QueryOne(graphDb);
        queryOne.execute(new ArrayList<String>());
    	/*LoaderRegion regionLoader=new LoaderRegion(graphDb);
    	LoaderNation nationLoader=new LoaderNation(graphDb);
    	LoaderSupplier supplierLoader=new LoaderSupplier(graphDb);
    	regionLoader.loadData();
    	nationLoader.loadData();
    	supplierLoader.loadData();
    	Transaction tx=graphDb.beginTx();
    	Iterable<Node> iterable=graphDb.findNodesByLabelAndProperty(DynamicLabel.label("region"), "name", "Oceanía");
    	Iterator<Node> it=iterable.iterator();
    	while(it.hasNext()){
    		Node node=it.next();
    		System.out.println((String)node.getProperty("name")+"  "+(String)node.getProperty("comment"));
    	}
    	
    	tx.success();*/
System.out.println( "Goodbye Cruel World!" );
    	graphDb.shutdown();
    }



    private static void clearDb() {
        try {
            FileUtils.deleteRecursively(new File(DB_PATH));
        }
        catch ( IOException e ) {
            throw new RuntimeException( e );
        }
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
