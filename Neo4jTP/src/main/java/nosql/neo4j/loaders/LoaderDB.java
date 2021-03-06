package nosql.neo4j.loaders;

import java.util.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public abstract class LoaderDB {

	protected GraphDatabaseService db;

    protected Random random = new Random(3l);

    protected float proportionalCoeficient;
    
	public LoaderDB(GraphDatabaseService db, float proportionalCoeficient){
		super();
		this.db=db;
        this.proportionalCoeficient = proportionalCoeficient;
	}
	
    protected Integer getRandomInteger() {
        // int must have 4 digits
        return random.nextInt(100000 - 1000) + 1000;
    }

    protected double getRandomDouble( int x ) {
        double sum = random.nextInt(9) + 1;
        for ( int i = 1; i < x/2; ++i ) {
            sum *= 10;
            sum += random.nextInt(10);
        }
        return sum;
    }

    protected String generateVariableRandomString(int size) {
        String result = "";
        for ( int i = 0; i < size/2; ++i ) {
            int number = random.nextInt(20);
            char chara = (char) ( 'a' + number );
            result += chara;
        }
        return result;
    }

    protected java.sql.Date getRandomDate() {
        Calendar calendar = new GregorianCalendar();
        calendar.set(2013, 4, 30);
        calendar.add(Calendar.DAY_OF_YEAR, random.nextInt(10000)-5000);
        return new java.sql.Date(calendar.getTimeInMillis());
    }

    protected Integer generateRandomInteger(int min, int max){
        Random rand = new Random();

        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }
	
	public abstract void loadData();

    protected double generateRandomDouble(int min, int max){
        Random rand = new Random();

        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum/100.;
    }

    protected Node getRandomNode(List<Node> list){
        int index = random.nextInt(list.size());
        return list.get(index);
    }

    protected List<Node> getNodeListFromLabel(Label label){
        List<Node> list=new ArrayList<Node>();
        Iterator<Node> it=GlobalGraphOperations.at(db).getAllNodesWithLabel(label).iterator();
        while(it.hasNext()){
            list.add(it.next());
        }
        return list;
    }

    protected Integer generateUniqueKey(){
        return null;
    }

    protected boolean chooseWithProbability(Integer probability){
        probability = probability % (101);

        return random.nextInt(100/probability) == 0;
    }

    protected abstract float nodesToCreate();

}
