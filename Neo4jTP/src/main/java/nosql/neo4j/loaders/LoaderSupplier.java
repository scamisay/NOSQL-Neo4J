package nosql.neo4j.loaders;

import java.util.Iterator;

import nosql.neo4j.loaders.GraphCreator.RelTypes;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LoaderSupplier extends LoaderDB{

	public static int SUP_PER_NATION = 4;

	public LoaderSupplier(String db_path) {
		super(db_path);
	}

	public LoaderSupplier(GraphDatabaseService db) {
		super(db);
		
		
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
		
        int maxValues = (int) (SF * 10000);
        for (int i = 1; i <= maxValues; ++i) {
            Node supplierNode = db.createNode(supplier);

            Integer id = getRandomInteger();
            supplierNode.setProperty("S_NAME", getRandomString(64));
            supplierNode.setProperty("S_ADDRESS", getRandomString(64));

            
            int index = random.nextInt(nations.length);
            String nationName=nations[index];
            Iterable<Node> reg=db.findNodesByLabelAndProperty(nation, "N_NAME", nationName);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node nationNode=it.next();
				nationNode.createRelationshipTo(supplierNode, RelTypes.HAS_SUPPLIER);
			}

            supplierNode.setProperty("S_PHONE", getRandomString(18));
            supplierNode.setProperty("S_ACCTBAL", getRandomDouble(13));
            supplierNode.setProperty("S_COMMENT", getRandomString(105));
        }
		
        tx.success();
	}

	
	
}
