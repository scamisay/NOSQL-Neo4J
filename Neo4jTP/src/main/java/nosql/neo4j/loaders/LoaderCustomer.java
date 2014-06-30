package nosql.neo4j.loaders;

import java.util.Iterator;

import nosql.neo4j.loaders.GraphCreator.RelTypes;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LoaderCustomer extends LoaderDB {

	public LoaderCustomer(String db_path) {
		super(db_path);
	}
	
	public LoaderCustomer(GraphDatabaseService db){
		super(db);
	}
	
	@Override
	public void loadData() {

        Transaction tx = db.beginTx();

        Label customer=DynamicLabel.label(LabelTypes.Customer.name());
        Label nation=DynamicLabel.label(LabelTypes.Nation.name());

        String[] nations={"Argentina","Brasil","Egipto","Rusia","Italia","Japón","China","Sudáfrica","Estados Unidos","Inglaterra","Colombia","Arabia Saudita","Australia","Grecia","México","España","Nigeria","Tailandia","Nueva Zelanda","Perú","Noruega","Kazajstán","Venezuela","Puerto Rico","República del Congo"};

		int maxValues = (int) (SF * 150000);
		for (int i = 1; i <= maxValues; ++i) {
			Node customerNode = db.createNode(customer);
			
			Integer id = getRandomInteger();

			customerNode.setProperty("C_NAME", getRandomString(64));
			customerNode.setProperty("C_ADDRESS", getRandomString(64));
			
			String nationName=nations[random.nextInt(nations.length)];
			
			Iterable<Node> reg=db.findNodesByLabelAndProperty(nation, "N_NAME", nationName);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node nationNode=it.next();
				nationNode.createRelationshipTo(customerNode, RelTypes.HAS_CUSTOMER);
			}
			
			//customerNode.createRelationshipTo(nation, RelTypes.CUSTOMER_BELONGS_TO_NATION);
			
			customerNode.setProperty("C_PHONE", getRandomString(64));
			customerNode.setProperty("C_ACCTBAL", getRandomDouble(13));
            customerNode.setProperty("C_MKTSEGMENT", getRandomString(64));
			customerNode.setProperty("C_COMMENT", getRandomString(120));
		}

        tx.success();

	}

	
	
	

	
}
