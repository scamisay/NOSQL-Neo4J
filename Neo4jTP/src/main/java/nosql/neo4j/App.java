package nosql.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nosql.neo4j.loaders.*;
import nosql.neo4j.queries.QueryFour;
import nosql.neo4j.queries.QueryOne;
import nosql.neo4j.queries.QueryThree;
import nosql.neo4j.queries.QueryTwo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

/**
 * Hello world!
 * 
 */
public class App {
	private static final String DB_PATH = "dataTPNOSQL/neo4j-hello-db";

	private static GraphCreator graphCreator = new GraphCreator();

	public static void main(String[] args) {
		/* elimino y creo la base */
		dropDatabase();
		GraphDatabaseService graphDb;
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook(graphDb);

		System.out.println("Hello World!");

        /***************************
         * create and load database
         **************************/
        Transaction tx = graphDb.beginTx();

        try {
            LoaderRegion loaderRegion = new LoaderRegion(graphDb);
            loaderRegion.loadData();

            LoaderNation loaderNation = new LoaderNation(graphDb);
            loaderNation.loadData();

            LoaderSupplier loaderSupplier = new LoaderSupplier(graphDb);
            loaderSupplier.loadData();

            LoaderPart loaderPart = new LoaderPart(graphDb);
            loaderPart.loadData();

            LoaderPartSupplier loaderPartSupplier = new LoaderPartSupplier(graphDb);
            loaderPartSupplier.loadData();

            LoaderCustomer loaderCustomer = new LoaderCustomer(graphDb);
            loaderCustomer.loadData();

            LoaderOrder loaderOrder = new LoaderOrder(graphDb);
            loaderOrder.loadData();

            LoaderListItem loaderListItem = new LoaderListItem(graphDb);
            loaderListItem.loadData();

            tx.success();
        }finally {
            tx.finish();
        }


        /***************
        * query 1
        ***************/
        QueryOne queryOne = new QueryOne(graphDb);
        queryOne.execute(new ArrayList<String>());

        /***************
         * query 2
         ***************/
         QueryTwo queryTwo = new QueryTwo(graphDb);
         List<String> argumentsTwo=new ArrayList<String>();
         argumentsTwo.add("Asia");
         argumentsTwo.add("97765");
         argumentsTwo.add("nk");
         queryTwo.execute(argumentsTwo);
        
         /***************
          * query 3
          ***************/
         QueryThree queryThree = new QueryThree(graphDb);
         List<String> argumentsThree=new ArrayList<String>();
         argumentsThree.add("12345678901234567890123456789012");
         argumentsThree.add("2014-01-01");
         argumentsThree.add("2014-01-01");
         queryThree.execute(argumentsThree);
        
        /***************
         * query 4
         ***************/
        QueryFour q=new QueryFour(graphDb);
        List<String> argumentsFour=new ArrayList<String>();
        argumentsFour.add("Europa");
        argumentsFour.add("2014-01-01");
        q.execute(argumentsFour);

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



        graphDb.shutdown();
        System.out.println( "Goodbye Cruel World!" );
	}

	private static void dropDatabase() {
		try {
			FileUtils.deleteRecursively(new File(DB_PATH));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	private static enum RelTypes implements RelationshipType {
		KNOWS
	}

}
