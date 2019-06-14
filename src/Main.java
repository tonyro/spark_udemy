import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

//        first();
//        second();
//        third();
        fourth();
        fifth();
    }

    // Section 7



    // Section 6
    public static void fifth() {
        // simplifying the code from fourth()
        List<String> inputData = new ArrayList<>();

        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(inputData)
          .mapToPair(rawMessage -> new Tuple2<>(rawMessage.split(":")[0], 1L))
          .reduceByKey(Long::sum)
          .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        sc.close();
    }


    public static void fourth() {
        List<String> inputData = new ArrayList<>();

        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

        JavaPairRDD<String, Long> pairRDD = originalLogMessages.mapToPair(rawMessage -> {
           String[] columns = rawMessage.split(":");

           return new Tuple2<>(columns[0], 1L);
        });

        JavaPairRDD<String, Long> sumsRdd = pairRDD.reduceByKey(Long::sum);

        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        sc.close();
    }

    public static void third() {
        List<Integer> inputData = new ArrayList<>();

        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));

        sqrtRdd.collect().forEach(System.out::println);
    }


    public static void second() {
        List<Integer> inputData = new ArrayList<>();

        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(IntegerWithSquareRoot::new);

        sqrtRdd.collect().forEach(System.out::println);

        sc.close();
    }

    public static void first() {
        List<Integer> inputData = new ArrayList<>();

        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);

        Integer result = myRdd.reduce(Integer::sum);

        System.out.println("Reduce result: " + result);


        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);

        sqrtRdd.collect().forEach(System.out::println);

        //how many elements in sqrtRdd (java way)
        System.out.println("Number of elements in sqrtRdd: " + sqrtRdd.count());

        //how many elements in sqrtRdd (spark way -- map and reduce)
        JavaRDD<Long> countRdd = sqrtRdd.map(value -> 1L);
        countRdd.collect().forEach(System.out::println);
        Long count = countRdd.reduce(Long::sum);

        System.out.println("Number of elements in count: " + count);


        sc.close();
    }
}
