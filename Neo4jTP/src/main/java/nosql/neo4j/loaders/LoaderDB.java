package nosql.neo4j.loaders;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public abstract class LoaderDB {

	protected GraphDatabaseService db;

    protected Random random = new Random(3l);

    protected float SF = 0.00333333f;
    
	public LoaderDB(String db_path) {
	
		this.db = new GraphDatabaseFactory().newEmbeddedDatabase(db_path);
	
	}
	
	public LoaderDB(GraphDatabaseService db){
		super();
		this.db=db;
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

    protected String getRandomString( int size ) {
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
	
	
	public abstract void loadData();

	
}
