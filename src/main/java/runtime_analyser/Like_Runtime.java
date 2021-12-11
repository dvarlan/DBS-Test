package runtime_analyser;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

public class Like_Runtime {
	public static void main(String args[]) {
		String filterpath = "C:\\Users\\PC\\Documents\\Uni\\WS 21-22\\Forschungsmodul Datenbanken\\Hatespeech Listen\\Liste Beleidigungen - Ethnic.csv";
		String tweetpath = "C:\\Users\\PC\\Documents\\Uni\\WS 21-22\\Forschungsmodul Datenbanken\\Twitter Testdata\\Auszug json EM\\Stichproben\\recorded-cascades1623346911099.json";
		
		LocalTime timestart = LocalDateTime.now().toLocalTime(); //Startzeit
		
		SparkConf sparkConf = new SparkConf().setAppName("Stichprobentest");
		sparkConf.setMaster("local");
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate(); 
		Dataset<Row> insultset = spark.read()
											.option("inferSchema", true)
											.option("sep", ",")
											.option("header", true)
											.csv(filterpath);
		//insultset.show();
		insultset.createOrReplaceTempView("insults");
		
		
		Dataset<Row> tweetset = spark.read().json(tweetpath);
		//tweetset.show();
		tweetset.createOrReplaceTempView("tweets");
		
		Dataset<Row> result = spark.sql("SELECT text FROM tweets, insults WHERE tweets.text LIKE ('%' || ' ' || insults.term || ' ' || '%')");
		result.show((int)result.count(), false);
		
		LocalTime timeend = LocalDateTime.now().toLocalTime(); //Endzeit
		System.out.println("Timestart: " + timestart);
		System.out.println("Timeend: " + timeend);
		long difhour = timeend.getHourOfDay() - timestart.getHourOfDay();
		long difmin = timeend.getMinuteOfHour() - timestart.getMinuteOfHour();
		long difsec = timeend.getSecondOfMinute() - timestart.getSecondOfMinute();
		if(difsec < 0) { //Sekunden berichtigen wenn negativ
			difmin = difmin - 1;
			difsec = (60 - timestart.getSecondOfMinute()) + timeend.getSecondOfMinute();
		} else if (difmin < 0) { //Minuten berichtigen wenn negativ
			difhour = difhour - 1;
			difmin = (60 - timestart.getMinuteOfHour()) + timeend.getSecondOfMinute();
		}
		System.out.println("Runtime: " + "h: " + difhour + " min: " + difmin + " s: " + difsec);
	}
}
