package nosql.neo4j.loaders;

import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;

public class LoaderOrder extends LoaderDB{

    private Integer MAX_NODES = 1500000;

	public LoaderOrder(GraphDatabaseService db, float proportionalCoeficient){
		super(db, proportionalCoeficient);
	}

    private List<Integer> orderIds = new ArrayList<>();

    @Override
    protected Integer generateUniqueKey(){
        Integer id = getRandomInteger();
        while(orderIds.contains(id)){
            id = getRandomInteger();
        }
        orderIds.add(id);
        return id;
    }

    @Override
    protected float nodesToCreate() {
        return MAX_NODES * proportionalCoeficient;
    }

    @Override
	public void loadData() {
        Transaction tx = db.beginTx();
		Label order=DynamicLabel.label(LabelTypes.Order.name());
		Label customer=DynamicLabel.label(LabelTypes.Customer.name());

        List<Node> customersNodes = getNodeListFromLabel(customer);

        int limit = (int) nodesToCreate();
        for (int i = 1; i <= limit; ++i) {
            Node orderNode = db.createNode(order);

            Node customerNode = getRandomNode(customersNodes);
            customerNode.createRelationshipTo(orderNode, RelTypes.HAS_ORDER);

            Integer id = generateUniqueKey();

            orderNode.setProperty("O_ORDERKEY", id);
            orderNode.setProperty("O_ORDERSTATUS", generateVariableRandomString(64));
            orderNode.setProperty("O_TOTALPRICE", getRandomInteger());
            orderNode.setProperty("O_ORDERDATE", getRandomDate().getTime());
            orderNode.setProperty("O_ORDERPRIORITY", generateVariableRandomString(15));
            orderNode.setProperty("O_CLERK", generateVariableRandomString(64));
            orderNode.setProperty("O_SHIPPRIORITY", getRandomInteger());
            orderNode.setProperty("O_COMMENT", generateVariableRandomString(80));
        }
        tx.success();
	}

}
