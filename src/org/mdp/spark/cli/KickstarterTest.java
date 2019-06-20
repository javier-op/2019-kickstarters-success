package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Get the average ratings of TV series from IMDb.
 *
 * This is the Java 8 version with lambda expressions.
 */
public class KickstarterTest {
    /**
     * This will be called by spark
     */
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");

        if(args.length != 2) {
            System.err.println("Usage arguments: inputPath outputPath");
            System.exit(0);
        }
        new KickstarterTest().run(args[0],args[1]);
    }

    /**
     * The task body
     */
    public void run(String inputFilePath, String outputFilePath) {
        /*
         * This is the address of the Spark cluster.
         * [*] means use all the cores available.
         * This can be overridden later when we call the application from the cluster.
         * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
         */
        String master = "local[*]";

        /*
         * Initialises a Spark context with the name of the application
         *   and the (default) master settings.
         */
        SparkConf conf = new SparkConf()
                .setAppName(KickstarterTest.class.getName())
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        /*
         * Load the first RDD from the input location (a local file, HDFS file, etc.)
         */
        JavaRDD<String> inputRDD = context.textFile(inputFilePath);

        JavaRDD<String> success = inputRDD.filter(
                line -> line.split(",")[9].equals("successful")
        );


        
        /*
         * Write the output to local FS or HDFS
         */
        success.saveAsTextFile(outputFilePath);

        context.close();
    }
}