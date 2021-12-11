package aufgabe1;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Aufgabe1d {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Test");
		sparkConf.setMaster("local");
		
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
		
		Dataset<Row> set = spark.read().format("csv")
				.option("sep", "|")
				.option("inferSchema", true)
				.load("C:/Users/PC/Documents/Uni/WS 21-22/Forschungsmodul Datenbanken/Blatt 1/part_supplier_partsupp_sf0.1/part.tbl");
		
		set.filter("(LENGTH(_c1) - LENGTH(replace(_c1, ' ', ''))) = 2").show();
	}
}
