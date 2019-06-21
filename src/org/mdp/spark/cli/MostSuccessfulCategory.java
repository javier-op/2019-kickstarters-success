package org.mdp.spark.cli;

import java.text.ParseException;
import java.lang.ArrayIndexOutOfBoundsException;
import java.util.Date;
import java.io.*;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.functions;

import scala.Tuple2;

/**
 * Get the average ratings of TV series from IMDb.
 *
 * This is the Java 8 version with lambda expressions.
 */
public class MostSuccessfulCategory {

    public static int toMonths(String end, String start){
        SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
        int endInt = 0;
        int startInt = 0;
        try {
            Date endDate = format.parse(end);
            Date startDate = format.parse(start);
            endInt = (int) (endDate.getTime() / 1000);
            startInt = (int) (startDate.getTime() / 1000);
        }catch (ParseException p){
            System.out.println(p);
        }
        return (endInt - startInt) / 2592000;
    }

    public static int parsealo(String s){
        int a=0;
        try {
            a = Math.round(Float.parseFloat(s));
        }catch (NumberFormatException | ArrayIndexOutOfBoundsException p){
            System.out.println(p);
        }
        return a;
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

        JavaPairRDD<Integer,String> succCountCat = successfulCatCount.mapToPair(
                line -> new Tuple2<Integer,String>(line._2, line._1)
        );

        Iterator<Tuple2<Integer,String>> top3successfullCatCount =
                succCountCat.sortByKey(false).take(3).iterator();

        try{
            FileWriter fr = new FileWriter("top3SuccCat");
            BufferedWriter br = new BufferedWriter(fr);
            PrintWriter out = new PrintWriter(br);
            while (top3successfullCatCount.hasNext()) {
                out.write(top3successfullCatCount.next().toString());
                out.write("\n");
            }
            out.close();
        }
        catch(IOException e){
            System.out.println(e);
        }
        //top3successfullCatCount.saveAsTextFile("top3SuccCat");
        ////////////////////////////////////////////////////////////


        /// MOST SUCCESSFUL MAIN_CAT ///////////////////////////////
        JavaPairRDD<String, Integer> successfulMainCatOne = successful.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[3], 1)
        );
        JavaPairRDD<String,Integer> successMainCatCount =
                successfulMainCatOne.reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer,String> successCountMainCat = successMainCatCount.mapToPair(
                line -> new Tuple2<Integer,String>(line._2, line._1)
        );

        Iterator<Tuple2<Integer,String>> mainCatSuccess =
                successCountMainCat.sortByKey(false).take(1).iterator();
        try{
            FileWriter fr = new FileWriter("top1SuccMainCat");
            BufferedWriter br = new BufferedWriter(fr);
            PrintWriter out = new PrintWriter(br);
            while (mainCatSuccess.hasNext()) {
                out.write(mainCatSuccess.next().toString());
                out.write("\n");
            }
            out.close();
        }
        catch(IOException e){
            System.out.println(e);
        }
        //mainCatSuccess.saveAsTextFile("top1SuccMainCat");
        ///////////////////////////////////////////////////////////

        /// TOP 10 USD REAL GOAL SUCCESSFUL PROYECTS  /////////////////
        JavaPairRDD<Integer, String> proyectsUSDGR = successful.mapToPair(
                line -> new Tuple2<Integer, String>((Math.round(Float.parseFloat(line.split(",")[14]))) , line.split(",")[1])
        );
        JavaPairRDD<Integer,String> sortedProyectsUSDGR =
                proyectsUSDGR.sortByKey();
        Iterator<Tuple2<Integer,String>> top10proyectsUSDGR =
                sortedProyectsUSDGR.sortByKey(false).take(10).iterator();
        try{
            FileWriter fr = new FileWriter("top10proyectsUSDGR");
            BufferedWriter br = new BufferedWriter(fr);
            PrintWriter out = new PrintWriter(br);
            while (top10proyectsUSDGR.hasNext()) {
                out.write(top10proyectsUSDGR.next().toString());
                out.write("\n");
            }
            out.close();
        }
        catch(IOException e){
            System.out.println(e);
        }
        //top10proyectsUSDGR.saveAsTextFile("top10proyectsUSDGR");
        ///////////////////////////////////////////////////////////////////

        /// USDGR PERCENT OF EACH CAT  ////////////////////////////
        /*
        JavaPairRDD<String, Integer> categoriesUSDGR = inputRDD.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[2], Integer.parseInt(line.split(",")[14]))
        );
        JavaPairRDD<String,Integer> categoriesUSDGRCount =
                categoriesUSDGR.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String,Integer> categoriesUSDGRPercent = categoriesUSDGRCount.mapToPair(
                line -> new Tuple2<String, Integer>(line._1,line._2/ functions.sum())
        );
        JavaPairRDD<String,Integer> categoriesUSDGRCountSorted =
                categoriesUSDGRPercent.sortByKey();

        categoriesUSDGRCountSorted.saveAsTextFile("categoriesUSDGRCountSorted");
        ///////////////////////////////////////////////////////////////////

         */

        /// COUNT BACKERS FOR EVERY MAIN CAT  ///////////////////////////////////
        JavaPairRDD<String, Integer> backersCat = inputRDD.mapToPair(
                line -> new Tuple2<String, Integer>(line.split(",")[3], parsealo(line.split(",")[10]))
        );
        JavaPairRDD<String,Integer> backersCatCount =
                backersCat.reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer,String> CatCountBackers = backersCatCount.mapToPair(
                line -> new Tuple2<Integer, String>(line._2, line._1)
        );

        JavaPairRDD<Integer,String> backersCatCountSorted =
                CatCountBackers.sortByKey(false);

        backersCatCountSorted.saveAsTextFile("backersCatCountSorted");
        ////////////////////////////////////////////////////////////////////

        /// PROYECT'S DATE-USDGR RELATION  /////////////////////////////////
        JavaPairRDD<Integer, Integer> monthsUSDGR = inputRDD.mapToPair(
                line -> new Tuple2<Integer,Integer> (
                        toMonths(line.split(",")[5], line.split(",")[7]),
                        parsealo(line.split(",")[14])
                )
        );
        JavaPairRDD<Integer,Integer> reducedMonthsUSDGR =
                monthsUSDGR.reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer,Integer> reducedMonthsUSDGRinv = reducedMonthsUSDGR.mapToPair(
                line -> new Tuple2<Integer,Integer>(line._2,line._1)
        );

        JavaPairRDD<Integer,Integer> reducedMonthsUSDGRsorted =
                reducedMonthsUSDGRinv.sortByKey(false);

        reducedMonthsUSDGRsorted.saveAsTextFile("reducedMonthsUSDGRsorted");
        ////////////////////////////////////////////////////////////////////





        context.close();
    }
}