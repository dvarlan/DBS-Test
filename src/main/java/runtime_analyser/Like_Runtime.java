package runtime_analyser;

import java.time.Instant;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Like_Runtime {
	public static void main(String args[]) {
		String filterpath = "Filterpath";
		String tweetpath = "Tweetpath";
		
		long unixstart = Instant.now().getEpochSecond(); //Startzeit
		SparkConf sparkConf = new SparkConf().setAppName("Stichprobentest");
		sparkConf.setMaster("local");
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate(); 
		Dataset<Row> insultset = spark.read()
											.option("inferSchema", true)
											.option("sep", ",")
											.option("header", true)
											.csv(filterpath);
		insultset.createOrReplaceTempView("insults");
		
		Dataset<Row> tweetset = spark.read().json(tweetpath);
		tweetset.createOrReplaceTempView("tweets");
		
		Dataset<Row> result = spark.sql("SELECT text FROM tweets, insults WHERE tweets.text LIKE ('%' || ' ' || insults.term || ' ' || '%')");
		result.show((int)result.count(), false);
		
		long unixend = Instant.now().getEpochSecond(); // Endzeit
		
		System.out.println("Unix Start:" + unixstart);
		System.out.println("Unix End:" + unixend);
		System.out.println("Unix Runtime: " + (unixend - unixstart));
		System.out.println("Runtime in regular format: ");
		timeConverter(unixend - unixstart);
	}
	
	public static void timeConverter(long unixruntime) {
		int x = (int) unixruntime / 60;
			System.out.println("Stunden: " + (x / 60) + " Minuten: " + (x % 60) + " Sekunden: " + (unixruntime % 60));
	}
}
