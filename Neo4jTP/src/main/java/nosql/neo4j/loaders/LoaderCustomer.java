package nosql.neo4j.loaders;

import org.neo4j.graphdb.*;

import java.util.Iterator;

public class LoaderCustomer extends LoaderDB {

    private Integer MAX_NODES = 150000;

	public LoaderCustomer(GraphDatabaseService db, float proportionalCoeficient){
        super(db, proportionalCoeficient);
	}
	
	@Override
	public void loadData() {

        Transaction tx = db.beginTx();

        Label customer=DynamicLabel.label(LabelTypes.Customer.name());
        Label nation=DynamicLabel.label(LabelTypes.Nation.name());

        String[] nations={"Argentina","Brasil","Egipto","Rusia","Italia","Japón","China","Sudáfrica","Estados Unidos","Inglaterra","Colombia","Arabia Saudita","Australia","Grecia","México","España","Nigeria","Tailandia","Nueva Zelanda","Perú","Noruega","Kazajstán","Venezuela","Puerto Rico","República del Congo"};

        int limit = (int) nodesToCreate();
        for (int i = 1; i <= limit; ++i) {
			Node customerNode = db.createNode(customer);

			customerNode.setProperty("C_NAME", generateVariableRandomString(64));
			customerNode.setProperty("C_ADDRESS", generateVariableRandomString(64));
			
			String nationName=nations[random.nextInt(nations.length)];
			
			Iterable<Node> reg=db.findNodesByLabelAndProperty(nation, "N_NAME", nationName);
			Iterator<Node> it=reg.iterator();
			if(it.hasNext()){
				Node nationNode=it.next();
				nationNode.createRelationshipTo(customerNode, RelTypes.HAS_CUSTOMER);
			}

			customerNode.setProperty("C_PHONE", generateVariableRandomString(64));
			customerNode.setProperty("C_ACCTBAL", getRandomDouble(13));

            if (chooseWithProbability(10)){
                customerNode.setProperty("C_MKTSEGMENT", "12345678901234567890123456789012");
            }else{
                customerNode.setProperty("C_MKTSEGMENT", generateVariableRandomString(64));
            }

            customerNode.setProperty("C_COMMENT", generateVariableRandomString(120));
		}

        tx.success();

	}

    @Override
    protected float nodesToCreate() {
        return MAX_NODES * proportionalCoeficient;
    }


}
