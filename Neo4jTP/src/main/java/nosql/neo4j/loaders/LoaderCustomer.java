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
        Label customer=DynamicLabel.label("customer");
        Label nation=DynamicLabel.label("nation");
        String[] nations={"Argentina","Brasil","Egipto","Rusia","Italia","Japón","China","Sudáfrica","Estados Unidos","Inglaterra","Colombia","Arabia Saudita","Australia","Grecia","México","España","Nigeria","Tailandia","Nueva Zelanda","Perú","Noruega","Kazajstán","Venezuela","Puerto Rico","República del Congo"};
		// C_CustKey, C_Name, C_Address, C_NationKey, C_Phone, C_AcctBal, C_MktSegment, C_Comment, skip
		
		int maxValues = (int) (SF * 150000);
		for (int i = 1; i <= maxValues; ++i) {
			Node customerNode = db.createNode(customer);
			
			Integer id = getRandomInteger();

			customerNode.setProperty("name", getRandomString(64));
			customerNode.setProperty("address", getRandomString(64));
			
			String nationName=nations[random.nextInt(nations.length)];
			
			Iterable<Node> reg=db.findNodesByLabelAndProperty(nation, "name", nationName);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node nationNode=it.next();
				nationNode.createRelationshipTo(customerNode, RelTypes.HAS_CUSTOMER);
			}
			
			//customerNode.createRelationshipTo(nation, RelTypes.CUSTOMER_BELONGS_TO_NATION);
			
			customerNode.setProperty("phone", getRandomString(64));
			customerNode.setProperty("acctBal", getRandomDouble(13));
			// With probability 0.1, set the value to be queried
			if (random.nextInt(10) == 0)
				customerNode.setProperty("mktSegment", "12345678901234567890123456789012");
			else
				customerNode.setProperty("mktSegment", getRandomString(64));
			customerNode.setProperty("comment", getRandomString(120));
			
			
		}
		tx.success();
		
	}

	
	
	

	
}
