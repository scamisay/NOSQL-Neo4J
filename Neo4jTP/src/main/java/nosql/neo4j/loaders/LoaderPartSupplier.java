package nosql.neo4j.loaders;

import java.util.*;

import nosql.neo4j.loaders.GraphCreator.RelTypes;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LoaderPartSupplier extends LoaderDB{

    private Integer MAX_NODES = 800000;

    public LoaderPartSupplier(GraphDatabaseService db, float proportionalCoeficient) {
        super(db, proportionalCoeficient);
    }

    @Override
    public void loadData() {
        Transaction tx = db.beginTx();

        Label partSupplier = DynamicLabel.label(LabelTypes.PartSupplier.name());
        Label supplierLabel = DynamicLabel.label(LabelTypes.Supplier.name());
        Label partLabel = DynamicLabel.label(LabelTypes.Part.name());

        List<Node> suppliersNodes = getNodeListFromLabel(supplierLabel);
        List<Node> partsNodes = getNodeListFromLabel(partLabel);

        int limit = (int) nodesToCreate();
        for (int i = 1; i <= limit; ++i) {

            Node partsuppNode = db.createNode(partSupplier);
            Node supplier = getRandomNode(suppliersNodes);
            Node part = getRandomNode(partsNodes);

            partsuppNode.createRelationshipTo(part, RelTypes.BELONGS_TO_PART);
            supplier.createRelationshipTo(partsuppNode, RelTypes.SUPPLIER_HAS_PARTSUPP);

            partsuppNode.setProperty("PS_AVAILQTY", getRandomInteger());
            partsuppNode.setProperty("PS_SUPPLYCOST", getRandomDouble(13));
            partsuppNode.setProperty("PS_COMMENT", generateVariableRandomString(200));

        }

        tx.success();
    }

    @Override
    protected float nodesToCreate() {
        return MAX_NODES * proportionalCoeficient;
    }


}
