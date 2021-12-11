package language_detection;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Stichprobe {
	public static void main(String[] args) {
		
		String path = "C:\\Users\\PC\\Documents\\Uni\\WS 21-22\\Forschungsmodul Datenbanken\\Twitter Testdata\\Auszug json EM\\Stichproben\\recorded-cascades1623441180257.json";
		
		SparkConf sparkConf = new SparkConf().setAppName("Stichprobentest");
		sparkConf.setMaster("local");
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate(); 
		
		Dataset<Row> set = spark.read().json(path);
		set.createOrReplaceTempView("tweets");
		Dataset<Row> set2 = spark.sql("SELECT COUNT(*) AS Anzahl, lang FROM tweets WHERE lang <> 'null' AND lang <> 'und' GROUP BY lang ORDER BY Anzahl desc");
		set2.show(200);
	}
}
