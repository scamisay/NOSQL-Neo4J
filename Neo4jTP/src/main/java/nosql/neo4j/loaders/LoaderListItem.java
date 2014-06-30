package nosql.neo4j.loaders;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class LoaderListItem extends LoaderDB{
	private Map<Integer, Integer> lineItemIds = new HashMap<Integer, Integer>();
	private List<Integer> orderIds = new ArrayList<Integer>();
	
	public LoaderListItem(String db_path) {
		super(db_path);
	}
	
	public LoaderListItem(GraphDatabaseService db){
		super(db);
	}

	@Override
	public void loadData() {
		Transaction tx = db.beginTx();

// L_OrderKey, L_PartKey, L_SuppKey, L_LineNumber, L_Quantity, L_ExtendedPrice, L_Discount,
// L_Tax, L_ReturnFlag, L_LineStatus, L_ShipDate, L_CommitDate, L_ReceiptDate, L_ShipInstruct, L_ShipMode, L_Comment, skip




        int maxValues = (int) (SF * 6000000);

        Label lineItem=DynamicLabel.label(LabelTypes.LineItem.name());
        Label order=DynamicLabel.label(LabelTypes.Order.name());
        Label supplier=DynamicLabel.label(LabelTypes.Supplier.name());
        Label part=DynamicLabel.label(LabelTypes.Part.name());

        List<Node> suppliersNodes=new ArrayList<Node>();
        Iterator<Node> it=GlobalGraphOperations.at(db).getAllNodesWithLabel(supplier).iterator();
        while(it.hasNext()){
        	suppliersNodes.add(it.next());
        }
        
        List<Node> ordersNodes=new ArrayList<Node>();
        it=GlobalGraphOperations.at(db).getAllNodesWithLabel(order).iterator();
        while(it.hasNext()){
        	ordersNodes.add(it.next());
        }
        

        for (int i = 1; i <= maxValues; ++i) {
            Node lineitemNode = db.createNode(lineItem);

            // L_OrderKey
            int index = random.nextInt(ordersNodes.size());
            Node orderNode = ordersNodes.get(index);
            orderNode.createRelationshipTo(lineitemNode, RelTypes.HAS_LINEITEM);


            //Integer id = orderIds.get(index);
            //Integer id = (Integer)orderNode.getProperty("O_OrderKey");

            /*if (lineItemIds.get(id) == null) lineItemIds.put(id, 1000);
            Integer lineId = lineItemIds.get(id) + 1;
            lineItemIds.put(id, lineId);
*/
            int suppIndex = random.nextInt(suppliersNodes.size());
            Node supplierNode = suppliersNodes.get(suppIndex);
            Iterator<Relationship> partIt=supplierNode.getRelationships().iterator();
            
            List<Node> partNodes=new ArrayList<Node>();
            while(partIt.hasNext()){
            	partNodes.add(partIt.next().getEndNode());
            }
            
            
            if(!partNodes.isEmpty()){
                int partIndex = random.nextInt(partNodes.size());
                Node partNode = partNodes.get(partIndex);

                lineitemNode.createRelationshipTo(partNode, RelTypes.IS_MADE_OF);
            }

            lineitemNode.createRelationshipTo(supplierNode,RelTypes.SUPPLIED_BY);

            //lineitemNode.setProperty("lineNumber", lineId);
            lineitemNode.setProperty("L_QUANTITY", generateRandomInteger(1, 10));
            lineitemNode.setProperty("L_EXTENDEDPRICE", generateRandomInteger(1, 1000));
            lineitemNode.setProperty("L_DISCOUNT", generateRandomDouble(1, 70));
            lineitemNode.setProperty("L_TAX", generateRandomDouble(1, 40));
            lineitemNode.setProperty("L_RETURNFLAG", "r");
            lineitemNode.setProperty("L_LINESTATUS", "a");
            // With probability 0.05, set the date to Apr 30 2013
            if (random.nextInt(20) == 0) {
                Calendar calendar = new GregorianCalendar(2014,5,25);
                lineitemNode.setProperty("L_SHIPDATE", calendar.getTime().getTime());
            }
            else
                lineitemNode.setProperty("L_SHIPDATE", getRandomDate().getTime());
            lineitemNode.setProperty("L_COMMITDATE", getRandomDate().getTime());
            if (random.nextInt(20) != 0)
                lineitemNode.setProperty("L_RECEIPTDATE", getRandomDate().getTime());
            lineitemNode.setProperty("L_SHIPINSTRUCT", getRandomString(64));
            lineitemNode.setProperty("L_SHIPMODE", getRandomString(64));
            if (random.nextInt(20) != 0)
                lineitemNode.setProperty("L_COMMENT", getRandomString(64));


        }

		tx.success();

	}

	
	
	
	
}
