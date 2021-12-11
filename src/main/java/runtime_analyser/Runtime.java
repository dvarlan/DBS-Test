package runtime_analyser;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;

public class Runtime {
	public static void main(String args[]) {
		String path = "C:\\Users\\PC\\Documents\\Uni\\WS 21-22\\Forschungsmodul Datenbanken\\Twitter Testdata\\Auszug json EM\\Stichproben\\recorded-cascades1623441180257.json";
		
		LocalTime timestart = LocalDateTime.now().toLocalTime(); //Startzeit
		
		SparkConf sparkConf = new SparkConf().setAppName("Stichprobentest");
		sparkConf.setMaster("local");
		sparkConf.set("spark.driver.host", "localhost");
		
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate(); 
		Dataset<Row> set = spark.read().json(path);
		set.show();
		
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
