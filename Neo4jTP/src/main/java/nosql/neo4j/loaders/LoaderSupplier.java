package nosql.neo4j.loaders;

import java.util.Iterator;

import nosql.neo4j.loaders.GraphCreator.RelTypes;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LoaderSupplier extends LoaderDB{

    private Integer MAX_NODES = 10000;

	public LoaderSupplier(GraphDatabaseService db, float proportionalCoeficient) {
        super(db, proportionalCoeficient);

	}

	@Override
	public void loadData() {
        Transaction tx = db.beginTx();
		String[] nations = { "Argentina", "Brasil", "Egipto", "Rusia",
				"Italia", "Japón", "China", "Sudáfrica", "Estados Unidos",
				"Inglaterra", "Colombia", "Arabia Saudita", "Australia",
				"Grecia", "México", "España", "Nigeria", "Tailandia",
				"Nueva Zelanda", "Perú", "Noruega", "Kazajstán", "Venezuela",
				"Puerto Rico", "República del Congo" };

		Label supplier = DynamicLabel.label(LabelTypes.Supplier.name());
		Label nation = DynamicLabel.label(LabelTypes.Nation.name());

        int limit = (int) nodesToCreate();
        for (int i = 1; i <= limit; ++i) {
            Node supplierNode = db.createNode(supplier);

            supplierNode.setProperty("S_NAME", generateVariableRandomString(64));
            supplierNode.setProperty("S_ADDRESS", generateVariableRandomString(64));

            int index = random.nextInt(nations.length);
            String nationName=nations[index];
            Iterable<Node> reg=db.findNodesByLabelAndProperty(nation, "N_NAME", nationName);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node nationNode=it.next();
				nationNode.createRelationshipTo(supplierNode, RelTypes.HAS_SUPPLIER);
			}

            supplierNode.setProperty("S_PHONE", generateVariableRandomString(18));
            supplierNode.setProperty("S_ACCTBAL", getRandomDouble(13));
            supplierNode.setProperty("S_COMMENT", generateVariableRandomString(105));
        }
		
        tx.success();
	}

    @Override
    protected float nodesToCreate() {
        return MAX_NODES * proportionalCoeficient;
    }


}
