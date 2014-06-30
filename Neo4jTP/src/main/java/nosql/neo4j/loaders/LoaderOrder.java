package nosql.neo4j.loaders;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import nosql.neo4j.loaders.GraphCreator.RelTypes;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class LoaderOrder extends LoaderDB{

	public LoaderOrder(String db_path) {
		super(db_path);
	}
	
	public LoaderOrder(GraphDatabaseService db){
		super(db);
	}

	@Override
	public void loadData() {
        Transaction tx = db.beginTx();
		Label order=DynamicLabel.label(LabelTypes.Order.name());
		Label customer=DynamicLabel.label(LabelTypes.Customer.name());
		List<Node> customersNodes = new ArrayList<Node>();
        Iterator<Node> it=GlobalGraphOperations.at(db).getAllNodesWithLabel(customer).iterator();
        while(it.hasNext()){
        	customersNodes.add(it.next());
        }
		
        int maxValues = (int) (SF * 1500000);
        for (int i = 1; i <= maxValues; ++i) {
            Node orderNode = db.createNode(order);

            
            int index = random.nextInt(customersNodes.size());
            Node customerNode = customersNodes.get(index);
            customerNode.createRelationshipTo(orderNode, RelTypes.HAS_ORDER);
            
            orderNode.setProperty("O_ORDERSTATUS", getRandomString(64));
            orderNode.setProperty("O_TOTALPRICE", getRandomInteger());
            orderNode.setProperty("O_ORDERDATE", getRandomDate().getTime());
            orderNode.setProperty("O_ORDERPRIORITY", getRandomString(15));
            orderNode.setProperty("O_CLERK", getRandomString(64));
            orderNode.setProperty("O_SHIPPRIORITY", getRandomInteger());
            orderNode.setProperty("O_COMMENT", getRandomString(80));
        }
        tx.success();
	}

}
