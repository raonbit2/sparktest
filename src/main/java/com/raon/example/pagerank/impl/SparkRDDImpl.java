
package com.raon.example.pagerank.impl;

import com.google.common.collect.Iterables;
import com.raon.example.pagerank.PageRank;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class SparkRDDImpl extends PageRank {
    static SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[*]");
    static JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    public SparkRDDImpl(JavaSparkContext ctx) {
        super(ctx);
    }

    // 1. 점수가 높은 순서대로 TOP 5 노드 출력 구현.
    protected void displayTop5(JavaPairRDD<String, Double> ranks) {
        JavaPairRDD<Double, String> countLinks = ranks.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });
        List<Tuple2<Double, String>> top5 = countLinks.sortByKey(false).take(5);
        for (Tuple2<?,?> tuple : top5) {
            System.out.println("Url: " + tuple._2() + ", Score:" + tuple._1());
        }
    }

    // 2. 평균값 출력 구현.
    protected void displayAvg(JavaPairRDD<String, Double> ranks) {
        JavaRDD<Double> scores = ranks.map(new Function<Tuple2<String, Double>, Double>() {
            public Double call(Tuple2<String, Double> tuple) throws Exception {
                return tuple._2();
            }
        });

        double count = scores.count();
        double sum = scores.fold(0.0, new Function2<Double, Double, Double>() {
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("Avg: " + sum / count);
    }

    public static void main(String[] args) throws Exception {
        new SparkRDDImpl(ctx);
    }
}
