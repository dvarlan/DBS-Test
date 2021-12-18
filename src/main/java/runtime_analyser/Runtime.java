package runtime_analyser;

import java.time.Instant;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Runtime {
	public static void main(String args[]) {
		String path = "INSERT PATH HERE";
		
		long unixstart = Instant.now().getEpochSecond(); //Startzeit
		
		SparkConf sparkConf = new SparkConf().setAppName("Runtimetest");
		sparkConf.setMaster("local");
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate(); 
		Dataset<Row> set = spark.read().json(path);
		set.show();
		
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
