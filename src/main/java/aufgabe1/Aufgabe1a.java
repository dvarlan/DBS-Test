package aufgabe1;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Aufgabe1a {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Test");
		sparkConf.setMaster("local");
		
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
		
		Dataset<Row> set = spark.read().format("csv")
				.option("sep", "|")
				.option("inferSchema", true)
				.load("C:/Users/PC/Documents/Uni/WS 21-22/Forschungsmodul Datenbanken/Blatt 1/part_supplier_partsupp_sf0.1/supplier.tbl");
		
		set.select("_c1").sort("_c5").show(25);	
	}
}
