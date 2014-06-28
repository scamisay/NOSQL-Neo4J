package nosql.neo4j.loaders;

import java.util.ArrayList;
import java.util.Iterator;




import java.util.List;




import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class LoaderPart extends LoaderDB{
	
	public LoaderPart(String db_path) {
		super(db_path);
	}
	
	public LoaderPart(GraphDatabaseService db){
		super(db);
	}

	@Override
	public void loadData() {
		Transaction tx = db.beginTx();
		int maxValues = (int) (SF * 200000);
        Label part=DynamicLabel.label("part");
        Label supplier=DynamicLabel.label("supplier");
        List<Node> suppliersNodes=new ArrayList<Node>();
        Iterator<Node> it=GlobalGraphOperations.at(db).getAllNodesWithLabel(supplier).iterator();
        while(it.hasNext()){
        	suppliersNodes.add(it.next());
        }
        
        
        
        
        
        int suppliersperpart=(int) Math.ceil((SF*800000)/maxValues);
		for (int i = 1; i <= maxValues; ++i) {
            Node partNode = db.createNode(part);

            partNode.setProperty("name", getRandomString(64));
            partNode.setProperty("mfgr", getRandomString(64));
            partNode.setProperty("brand", getRandomString(64));
            // With probability 0.1, set the value to be queried
            if (random.nextInt(10) == 0) {
                partNode.setProperty("type", "12345678901234567890123456789012");
                partNode.setProperty("size", 1000);
            }
            else {
                partNode.setProperty("type", getRandomString(64));
                partNode.setProperty("size",  getRandomInteger());
            }

            partNode.setProperty("container", getRandomString(64));
            partNode.setProperty("retailPrice", getRandomDouble(13));
            partNode.setProperty("comment", getRandomString(64));
            
            
            List<Node> connectedSup=new ArrayList<Node>();
            
            int suppIndex = random.nextInt(suppliersNodes.size());
            Node supplierNode = suppliersNodes.get(suppIndex);
            while(connectedSup.contains(supplierNode)){
            	suppIndex = random.nextInt(suppliersNodes.size());
            	supplierNode = suppliersNodes.get(suppIndex);
            }
            
                     	
            Relationship provide=supplierNode.createRelationshipTo(partNode, RelTypes.PROVIDE);
            suppliersNodes.add(supplierNode);
            
            provide.setProperty("availQty", getRandomInteger());
            provide.setProperty("supplyCost", getRandomDouble(13));
            provide.setProperty("comment", getRandomString(200));
            
            
            
            
        }
		tx.success();
		
	}

	
	
}
