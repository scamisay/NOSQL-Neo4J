package nosql.neo4j.loaders;

import java.util.*;

import nosql.neo4j.loaders.GraphCreator.RelTypes;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class LoaderPartSupplier extends LoaderDB{

    public static int SUP_PER_NATION = 4;

    public LoaderPartSupplier(String db_path) {
        super(db_path);
    }

    public LoaderPartSupplier(GraphDatabaseService db) {
        super(db);


    }

    private Map<Integer, List<Integer>> partSuppIds = new HashMap<Integer, List<Integer>>();

    @Override
    public void loadData() {
        Transaction tx = db.beginTx();

        Label partSupplier= DynamicLabel.label(LabelTypes.PartSupplier.name());
        Label supplierLabel =DynamicLabel.label(LabelTypes.Supplier.name());
        Label partLabel =DynamicLabel.label(LabelTypes.Part.name());

        List<Node> suppliersNodes = new ArrayList<Node>();
        Iterator<Node> it= GlobalGraphOperations.at(db).getAllNodesWithLabel(supplierLabel).iterator();

        while(it.hasNext()){
            suppliersNodes.add(it.next());
        }

        List<Node> partsNodes = new ArrayList<Node>();
        Iterator<Node> partIt= GlobalGraphOperations.at(db).getAllNodesWithLabel(partLabel).iterator();

        while(partIt.hasNext()){
            partsNodes.add(partIt.next());
        }

        int maxValues = (int) (SF * 800000);
        for (int i = 1; i <= maxValues; ++i) {

            Node partsuppNode = db.createNode(partSupplier);

            int suppIndex = random.nextInt(suppliersNodes.size());
            Node supplier = suppliersNodes.get(suppIndex);

            int partIndex = random.nextInt(partsNodes.size());
            Node part = partsNodes.get(partIndex);

         /*   Integer partid = partIds.get(partIndex);
            while(partSuppIds.get(suppid) != null && partSuppIds.get(suppid).contains(partid)) {
                suppid = supplierIds.get(random.nextInt(supplierIds.size()));
                partid = partIds.get(random.nextInt(partIds.size()));
            }
            if (partSuppIds.get(suppid) == null) partSuppIds.put(suppid, new ArrayList<Integer>());
            partSuppIds.get(suppid).add(partid);*/
            //document.put("PS_PartKey", partid);
          /*  Node part = parts.get(partIndex);*/
            //part.createRelationshipTo(partsuppNode, RelTypes.PART_HAS_PARTSUPP);
            partsuppNode.createRelationshipTo(part, RelTypes.BELONGS_TO_PART);

            // PS_PartKey
            /*Node supplier = suppliers.get(suppIndex);*/
            supplier.createRelationshipTo(partsuppNode, RelTypes.SUPPLIER_HAS_PARTSUPP);
            //partsuppNode.createRelationshipTo(supplier, RelTypes.BELONGS_TO_SUPPLIER);

/*            partsuppNode.setProperty("PS_PartKey", partid);*/
            partsuppNode.setProperty("PS_AVAILQTY", getRandomInteger());
            partsuppNode.setProperty("PS_SUPPLYCOST", getRandomDouble(13));
            partsuppNode.setProperty("PS_COMMENT", getRandomString(200));

           /* List<Node> suppParts = partSupps.get(suppid);
            if (suppParts == null) suppParts = new ArrayList<Node>();
            suppParts.add(partsuppNode);
            partSupps.put(suppid, suppParts);*/
        }

        tx.success();
    }



}
