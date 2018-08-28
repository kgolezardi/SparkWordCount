package ir.sahab.nimbo.jimbo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    public static void wordCount() {
        SparkConf conf = new SparkConf().setAppName("Work Count App");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("hdfs://hitler:9000/testSarb/luremIpsum.txt");

        JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts =
                words.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int)x + (int)y);

        counts.saveAsTextFile("hdfs://hitler:9000/output");
    }

    public static void main(String[] args) {
        wordCount();
    }
}
