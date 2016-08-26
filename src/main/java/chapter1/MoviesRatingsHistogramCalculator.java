package chapter1;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * Calculates movies ratings histogram out of movielens ratings data
 */
public final class MoviesRatingsHistogramCalculator {
	public static void main(String[] args) {
	
		final Logger logger = Logger.getLogger(MoviesRatingsHistogramCalculator.class);
		
		if(args.length < 1) {
			logger.error("Illegal number of arguments: at least 1");
		}
		final String inputFile = args[0];
		
		try(JavaSparkContext sc = new JavaSparkContext("local", "MoviesRatingsHistogramCalculator")) {
			
			final JavaRDD<String> lines = sc.textFile(inputFile);
			
			final Map<String, Integer> ratingsFreq = lines.mapToPair(line -> {
				String[] arr = line.split("\\t");
				return new Tuple2<String, Integer>(arr[2], 1);
			}).reduceByKey((i1, i2) -> i1 + i2).collectAsMap();
			 
			ratingsFreq.entrySet().stream()
			   .sorted((e1, e2) -> Integer.compare(e1.getValue(), e2.getValue()))
			   .forEach(entry -> {
				   System.out.println(entry.getKey() + ": " + entry.getValue());
			   });
			
		} catch(Exception e) {
			logger.fatal("Exception", e);
		}
	}
}
