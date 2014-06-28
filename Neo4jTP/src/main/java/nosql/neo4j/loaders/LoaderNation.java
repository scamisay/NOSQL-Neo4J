package nosql.neo4j.loaders;

import java.util.Iterator;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;


public class LoaderNation extends LoaderDB{

	public LoaderNation(String db_path) {
		super(db_path);
	}
	
	public LoaderNation(GraphDatabaseService db){
		super(db);
	}

	@Override
	public void loadData() {
		Transaction tx = db.beginTx();
		
		
		String[] nations={"Argentina","Brasil","Egipto","Rusia","Italia","Japón","China","Sudáfrica","Estados Unidos","Inglaterra","Colombia","Arabia Saudita","Australia","Grecia","México","España","Nigeria","Tailandia","Nueva Zelanda","Perú","Noruega","Kazajstán","Venezuela","Puerto Rico","República del Congo"};
		String[] regions={"América","América","África","Europa","Europa","Asia","Asia","África","América","Europa","América","Asia","Oceanía","Europa","América","Europa","África","Asia","Oceanía","América","Europa","Asia","América","América","África"};
		Label nation= DynamicLabel.label("nation");
		Label region= DynamicLabel.label("region");
		for(int i=0;i<nations.length;i++){
			String s=nations[i];
			String r=regions[i];
			Node node=db.createNode(nation);
			node.setProperty("name", s);
			node.setProperty("comment", s.concat("Pais Comentario"));
			Iterable<Node> reg=db.findNodesByLabelAndProperty(region, "name", r);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node regionNode=it.next();
				regionNode.createRelationshipTo(node, RelTypes.HAS_NATION);
			}
		
		}
		tx.success();
		
	}


}
