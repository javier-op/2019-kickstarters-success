package org.mdp.spark.cli;

import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.functions;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Get the average ratings of TV series from IMDb.
 *
 * This is the Java 8 version with lambda expressions.
 */
public class MostSuccessfulCategory {

    public static int toMonths(String end, String start){
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
        Date endDate = format.parse(end);
        Date startDate = format.parse(start);
        int end = (int) (end.getTime()/1000);
        int start = (int) (start.getTime()/1000);
        return (end - start)/2592000;
    }

    /**
     * This will be called by spark
     */
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");

        if(args.length != 2) {
            System.err.println("Usage arguments: inputPath outputPath");
            System.exit(0);
        }
        new MostSuccessfulCategory().run(args[0],args[1]);
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
                .setAppName(MostSuccessfulCategory.class.getName())
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        /*
         * Load the first RDD from the input location (a local file, HDFS file, etc.)
         */
        JavaRDD<String> inputRDD = context.textFile(inputFilePath).cache();


        //// TOP 3 MOST SUCCESSFUL CATEGORIES  ////////////////////
        JavaRDD<String> successful = inputRDD.filter(
                line -> line.split(",")[9].equals("successful")
        ).cache();
        JavaPairRDD<String, Integer> successfulCatOne = successful.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[2], 1)
        );
        JavaPairRDD<String,Integer> successfulCatCount =
                successfulCatOne.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String,Integer> top3successfullCatCount =
                successfulCatCount.sortByKey().take(3);
        /*
         * Write the output to local FS or HDFS
         */
        top3successfullCatCount.saveAsTextFile("top3SuccCat");
        ////////////////////////////////////////////////////////////


        /// MOST SUCCESSFUL MAIN_CAT ///////////////////////////////
        JavaPairRDD<String, Integer> successfulMainCatOne = successful.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[3], 1)
        );
        JavaPairRDD<String,Integer> successMainCatCount =
                successfulMainCatOne.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String,Integer> mainCatSuccess =
                successMainCatCount.sortByKey().take(1);
        mainCatSuccess.saveAsTextFile("top1SuccMainCat");
        ///////////////////////////////////////////////////////////

        /// TOP 10 USD REAL GOAL SUCCESSFUL PROYECTS  /////////////////
        JavaPairRDD<String, Integer> proyectsUSDGR = successful.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[1], line.split(",")[14])
        );
        JavaPairRDD<String,Integer> top10proyectsUSDGR =
                proyectsUSDGR.sortByKey().take(10);
        top10proyectsUSDGR.saveAsTextFile("top10proyectsUSDGR");
        ///////////////////////////////////////////////////////////////////

        /// USDGR PERCENT OF EACH CAT  ////////////////////////////
        JavaPairRDD<String, Integer> categoriesUSDGR = inputRDD.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[2], line.split(",")[14])
        );
        JavaPairRDD<String,Integer> categoriesUSDGRCount =
                categoriesUSDGR.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String,Integer> categoriesUSDGRPercent = categoriesUSDGRCount.mapToPair(
                line -> new Tuple2<String, Integer>(line._1,line._2/sum(line._2))
        );
        JavaPairRDD<String,Integer> categoriesUSDGRCountSorted =
                categoriesUSDGRPercent.sortByKey();

        categoriesUSDGRCountSorted.saveAsTextFile("categoriesUSDGRCountSorted");
        ///////////////////////////////////////////////////////////////////

        /// COUNT BACKERS FOR EVERY CAT  ///////////////////////////////////
        JavaPairRDD<String, Integer> backersCat = inputRDD.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[2], line.split(",")[10])
        );
        JavaPairRDD<String,Float> backersCatCount =
                backersCat.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String,Float> top3successfulCatCount =
                backersCatCount.sortByKey();

        top3successfulCatCount.saveAsTextFile("top3successfulCatCount");
        ////////////////////////////////////////////////////////////////////


        /// PROYECT'S DATE-USDGR RELATION  /////////////////////////////////
        JavaPairRDD<Integer, Integer> monthsUSDGR = inputRDD.map(
                line -> new Tuple2<Date,Integer> (
                        toMonths(line.split(",")[5], line.split(",")[7]),
                        line.split(",")[14]
                )
        );
        JavaPairRDD<String,Integer> reducedMonthsUSDGR =
                monthsUSDGR.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String,Integer> reducedMonthsUSDGRsorted =
                reducedMonthsUSDGR.sortByKey();

        reducedMonthsUSDGRsorted.saveAsTextFile("reducedMonthsUSDGRsorted");
        ////////////////////////////////////////////////////////////////////

        context.close();
    }
}