package nosql.neo4j.loaders;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LoaderRegion extends LoaderDB{

	public LoaderRegion(GraphDatabaseService db, float proportionalCoeficient){
		super(db, proportionalCoeficient);
	}
	
	@Override
	public void loadData() {
		Transaction tx = db.beginTx();
		String[] regions={"América","Europa","Asia","África","Oceanía","12345678901234567890123456789012"};
		
		Label region= DynamicLabel.label(LabelTypes.Region.name());
		for(int i=0;i<regions.length;i++){
			String r=regions[i];
			Node node=db.createNode(region);
			node.setProperty("R_NAME", r);
			node.setProperty("R_COMMENT", r.concat("Región Comentario"));
			
		}
		
		tx.success();
	}

    @Override
    protected float nodesToCreate() {
        return 0;
    }

}
