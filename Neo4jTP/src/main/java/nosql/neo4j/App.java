package nosql.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import nosql.neo4j.loaders.*;
import nosql.neo4j.queries.QueryFour;
import nosql.neo4j.queries.QueryOne;
import nosql.neo4j.queries.QueryThree;
import nosql.neo4j.queries.QueryTwo;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
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
		createSchema(graphDb);
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
	
	private static void createSchema(GraphDatabaseService graphDb){
		ExecutionEngine engine = new ExecutionEngine( graphDb );	
			
		try ( Transaction tx = graphDb.beginTx() )
		{
		    ExecutionResult result = engine.execute("CREATE INDEX ON :Customer(C_MKTSEGMENT)");
		    tx.success();
		}
		
		engine= new ExecutionEngine( graphDb );
		
		try ( Transaction tx = graphDb.beginTx() )
		{
		    ExecutionResult result = engine.execute("CREATE INDEX ON :LineItem(L_SHIPDATE)");
		    tx.success();
		}
		
		engine= new ExecutionEngine( graphDb );
		
		try ( Transaction tx = graphDb.beginTx() )
		{
		    ExecutionResult result = engine.execute("CREATE INDEX ON :Order(O_ORDERDATE)");
		    tx.success();
		}
		
		engine= new ExecutionEngine( graphDb );
		
		try ( Transaction tx = graphDb.beginTx() )
		{
		    ExecutionResult result = engine.execute("CREATE INDEX ON :Part(P_SIZE)");
		    tx.success();
		}
		
		engine= new ExecutionEngine( graphDb );
		
		try ( Transaction tx = graphDb.beginTx() )
		{
		    ExecutionResult result = engine.execute("CREATE INDEX ON :Order(O_TYPE)");
		    tx.success();
		}
		
	}

	

}
