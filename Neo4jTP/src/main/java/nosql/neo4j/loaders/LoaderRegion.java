package nosql.neo4j.loaders;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class LoaderRegion extends LoaderDB{
	public LoaderRegion(String db_path) {
		super(db_path);
	}

	public LoaderRegion(GraphDatabaseService db){
		super(db);
	}
	
	@Override
	public void loadData() {
		Transaction tx = db.beginTx();
		String[] regions={"América","Europa","Asia","África","Oceanía"};
		
		Label region= DynamicLabel.label(LabelTypes.Region.name());
		for(int i=0;i<regions.length;i++){
			String r=regions[i];
			Node node=db.createNode(region);
			node.setProperty("R_NAME", r);
			node.setProperty("R_COMMENT", r.concat("Región Comentario"));
			
		}
		
		tx.success();
	}

}
