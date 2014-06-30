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
		

		// O_OrderStatus, O_TotalPrice, O_OrderDate, O_OrderPriority, O_Clerk, O_ShipPriority, O_Comment, skip

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
            
            orderNode.setProperty("orderStatus", getRandomString(64));
            orderNode.setProperty("totalPrice", getRandomInteger());
            // With probability 0.05, set the date to be queried
            if (random.nextInt(20) == 0) {
                Calendar calendar = new GregorianCalendar(2013,4,29);
                orderNode.setProperty("orderDate", calendar.getTime().getTime());
            }
            else
                orderNode.setProperty("orderDate", getRandomDate().getTime());
            orderNode.setProperty("orderPriority", getRandomString(15));
            orderNode.setProperty("clerk", getRandomString(64));
            orderNode.setProperty("shipPriority", getRandomInteger());
            orderNode.setProperty("comment", getRandomString(80));
            

        }

		
		
		tx.success();
		
	}

}
