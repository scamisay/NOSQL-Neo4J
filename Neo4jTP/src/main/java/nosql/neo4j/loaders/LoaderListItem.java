package nosql.neo4j.loaders;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;


import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class LoaderListItem extends LoaderDB{

    private Integer MAX_NODES = 6000000;
	
	public LoaderListItem(GraphDatabaseService db, float proportionalCoeficient){
        super(db, proportionalCoeficient);
	}

	@Override
	public void loadData() {
		Transaction tx = db.beginTx();

        Label lineItem=DynamicLabel.label(LabelTypes.LineItem.name());
        Label order=DynamicLabel.label(LabelTypes.Order.name());
        Label supplier=DynamicLabel.label(LabelTypes.Supplier.name());
        Label partSupplier=DynamicLabel.label(LabelTypes.PartSupplier.name());

        List<Node> suppliersNodes = getNodeListFromLabel(supplier);
        List<Node> ordersNodes = getNodeListFromLabel(order);
        List<Node> partSuppliersNodes = getNodeListFromLabel(partSupplier);

        int limit = (int) nodesToCreate();
        for (int i = 1; i <= limit; ++i) {
            Node lineitemNode = db.createNode(lineItem);

            Node orderNode = getRandomNode(ordersNodes);
            orderNode.createRelationshipTo(lineitemNode, RelTypes.HAS_LINEITEM);

            Node partSupp = getRandomNode(partSuppliersNodes);
            partSupp.createRelationshipTo(lineitemNode, RelTypes.PARTSUPP_HAS_LINEITEM);

            Node supplierNode = getRandomNode(suppliersNodes);
            Iterator<Relationship> partIt=supplierNode.getRelationships().iterator();
            
            List<Node> partNodes=new ArrayList<Node>();
            while(partIt.hasNext()){
            	partNodes.add(partIt.next().getEndNode());
            }
            
            if(!partNodes.isEmpty()){
                Node partNode = getRandomNode(partNodes);
                lineitemNode.createRelationshipTo(partNode, RelTypes.IS_MADE_OF);
            }

            lineitemNode.createRelationshipTo(supplierNode,RelTypes.SUPPLIED_BY);

            lineitemNode.setProperty("L_QUANTITY", generateRandomInteger(1, 10));
            lineitemNode.setProperty("L_EXTENDEDPRICE", generateRandomInteger(1, 1000));
            lineitemNode.setProperty("L_DISCOUNT", generateRandomDouble(1, 70));
            lineitemNode.setProperty("L_TAX", generateRandomDouble(1, 40));
            lineitemNode.setProperty("L_RETURNFLAG", generateVariableRandomString(2));
            lineitemNode.setProperty("L_LINESTATUS", generateVariableRandomString(2));

            if (chooseWithProbability(1)) {
                Calendar calendar = new GregorianCalendar(2014,5,25);
                lineitemNode.setProperty("L_SHIPDATE", calendar.getTime().getTime());
            }else{
                lineitemNode.setProperty("L_SHIPDATE", getRandomDate().getTime());
            }
            lineitemNode.setProperty("L_COMMITDATE", getRandomDate().getTime());
            if (chooseWithProbability(5)){
                lineitemNode.setProperty("L_RECEIPTDATE", getRandomDate().getTime());
            }
            lineitemNode.setProperty("L_SHIPINSTRUCT", generateVariableRandomString(64));
            lineitemNode.setProperty("L_SHIPMODE", generateVariableRandomString(64));
            if (chooseWithProbability(95))
                lineitemNode.setProperty("L_COMMENT", generateVariableRandomString(64));


        }

		tx.success();

	}

    @Override
    protected float nodesToCreate() {
        return MAX_NODES * proportionalCoeficient;
    }


}
