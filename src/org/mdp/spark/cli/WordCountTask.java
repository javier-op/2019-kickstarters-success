package org.mdp.spark.cli;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * WordCountTask class, we will call this class with the test WordCountTest.
 */
public class WordCountTask {
	/**
	 * This is the entry point when the task is called from command line with spark-submit.sh.
	 * See {@see http://spark.apache.org/docs/latest/submitting-applications.html}
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new WordCountTask().run(args[0],args[1]);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath, String outputFilePath) {
		/*
		 * This is the address of the Spark cluster. We will call the task from WordCountTest and we
		 * use a local standalone cluster. [*] means use all the cores available.
		 * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
		 */
		String master = "local[*]";

		/*
		 * Initialises a Spark context.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(WordCountTask.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Performs a work count sequence of tasks and prints the output with a logger.
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);
		JavaRDD<String> wordRDD = inputRDD.flatMap(text -> Arrays.<String>asList(text.split(" ")).iterator());
		JavaPairRDD<String,Integer> wordOneRDD = wordRDD.mapToPair(word -> new Tuple2<String,Integer>(word, 1));
		JavaPairRDD<String,Integer> wordCount = wordOneRDD.reduceByKey((a, b) -> a + b);
		wordCount.saveAsTextFile(outputFilePath);
		
		context.close();
	}
}
