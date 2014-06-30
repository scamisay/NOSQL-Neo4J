package nosql.neo4j.loaders;

import java.util.ArrayList;


import java.util.List;


import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class LoaderPart extends LoaderDB{

    private Integer MAX_NODES = 200000;

    public LoaderPart(GraphDatabaseService db, float proportionalCoeficient){
        super(db, proportionalCoeficient);
    }

    private List<Integer> partIds = new ArrayList<Integer>();

    @Override
    protected Integer generateUniqueKey(){
        Integer id = getRandomInteger();
        while(partIds.contains(id)){
            id = getRandomInteger();
        }
        partIds.add(id);
        return id;
    }

    @Override
    protected float nodesToCreate() {
        return MAX_NODES * proportionalCoeficient;
    }

    @Override
    public void loadData() {
        Transaction tx = db.beginTx();

        Label part=DynamicLabel.label(LabelTypes.Part.name());
        Label supplier=DynamicLabel.label(LabelTypes.Supplier.name());

        List<Node> suppliersNodes = getNodeListFromLabel(supplier);

        int limit = (int) nodesToCreate();
        for (int i = 1; i <= limit; ++i) {
            Node partNode = db.createNode(part);

            Integer id = generateUniqueKey();
            partNode.setProperty("P_PARTKEY", id);
            partNode.setProperty("P_NAME", generateVariableRandomString(64));
            partNode.setProperty("P_MFGR", generateVariableRandomString(64));
            partNode.setProperty("P_BRAND", generateVariableRandomString(64));

            if (random.nextInt(10) == 0) {
                partNode.setProperty("P_TYPE", "12345678901234567890123456789012");
                partNode.setProperty("P_SIZE", 1000);
            }
            else {
                partNode.setProperty("P_TYPE", generateVariableRandomString(64));
                partNode.setProperty("P_SIZE",  getRandomInteger());
            }

            partNode.setProperty("P_CONTAINER", generateVariableRandomString(64));
            partNode.setProperty("P_RETAILPRICE", getRandomDouble(13));
            partNode.setProperty("P_COMMENT", generateVariableRandomString(64));

            List<Node> connectedSup = new ArrayList<Node>();

            Node supplierNode = getRandomNode(suppliersNodes);
            while(connectedSup.contains(supplierNode)){
                supplierNode = getRandomNode(suppliersNodes);
            }

            Relationship provide=supplierNode.createRelationshipTo(partNode, RelTypes.PROVIDE);
            suppliersNodes.add(supplierNode);

            provide.setProperty("PS_AVAILQTY", getRandomInteger());
            provide.setProperty("PS_SUPPLYCOST", getRandomDouble(13));
            provide.setProperty("PS_COMMENT", generateVariableRandomString(200));
        }

        tx.success();
    }



}
