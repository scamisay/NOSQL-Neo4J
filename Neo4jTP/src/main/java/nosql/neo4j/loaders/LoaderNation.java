package nosql.neo4j.loaders;

import java.util.Iterator;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;


public class LoaderNation extends LoaderDB{

	public LoaderNation(GraphDatabaseService db, float proportionalCoeficient){
        super(db, proportionalCoeficient);
	}

	@Override
	public void loadData() {
        Transaction tx = db.beginTx();
		String[] nations={"Argentina","Brasil","Egipto","Rusia","Italia","Japón","China","Sudáfrica","Estados Unidos","Inglaterra","Colombia","Arabia Saudita","Australia","Grecia","México","España","Nigeria","Tailandia","Nueva Zelanda","Perú","Noruega","Kazajstán","Venezuela","Puerto Rico","República del Congo"};
		String[] regions={"América","América","África","Europa","Europa","Asia","Asia","África","América","Europa","América","Asia","Oceanía","Europa","América","Europa","África","Asia","Oceanía","América","Europa","Asia","América","América","África"};
		Label nation= DynamicLabel.label(LabelTypes.Nation.name());
		Label region= DynamicLabel.label(LabelTypes.Region.name());

		for(int i=0;i<nations.length;i++){
			String s=nations[i];
			String r=regions[i];
			Node node=db.createNode(nation);
			node.setProperty("N_NAME", s);
			node.setProperty("N_COMMENT", s.concat("Pais Comentario"));
			Iterable<Node> reg=db.findNodesByLabelAndProperty(region, "R_NAME", r);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node regionNode=it.next();
				regionNode.createRelationshipTo(node, RelTypes.HAS_NATION);
			}
		
		}
        tx.success();
	}

    @Override
    protected float nodesToCreate() {
        return 0;
    }


}
