package json_test_data;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Rt1_json {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Test");
		sparkConf.setMaster("local");
		
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
		
		Dataset<Row> set = spark.read().json("C:\\Users\\PC\\Documents\\Uni\\WS 21-22\\Forschungsmodul Datenbanken\\Twitter Testdata\\testdata\\rt-1.json");
		set.createOrReplaceTempView("settemp");
		Dataset<Row> sett = spark.sql("SELECT count(*) FROM settemp WHERE user.default_profile_image = true");
		sett.show();	
	}
}
