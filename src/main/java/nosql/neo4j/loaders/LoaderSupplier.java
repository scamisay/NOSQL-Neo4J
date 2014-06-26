package nosql.neo4j.loaders;

import java.util.Iterator;



import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class LoaderSupplier extends LoaderDB {

	private static int SUP_PER_NATION = 4;

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

		Label supplier = DynamicLabel.label("supplier");
		Label nation = DynamicLabel.label("nation");
		for (int i = 0; i < nations.length; i++) {
			for (int j = 0; j < SUP_PER_NATION; j++) {
				String n = "sup"+nations[i];
				Node node = db.createNode(supplier);
				node.setProperty("name", n+j);
				node.setProperty("address", n);
				node.setProperty("phone", "1547565655556".concat(Integer.toString(i*10+j)));
				node.setProperty("acctbal", j*10+i);
				node.setProperty("comment", n.concat("Región Comentario")+i);
				
				Iterable<Node> reg=db.findNodesByLabelAndProperty(nation, "name", nations[i]);
				Iterator<Node> it=reg.iterator();
				if(it.hasNext()){
					Node regionNode=it.next();
					regionNode.createRelationshipTo(node, RelTypes.IS_NATION_OF);
				}
			}
		}

		tx.success();
	}

    private static enum RelTypes implements RelationshipType
    {
        IS_NATION_OF
    }

}
