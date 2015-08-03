
package com.raon.example.pagerank.impl;

import com.google.common.collect.Iterables;
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

public final class SparkRDDImpl {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // File Loading
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = ctx.textFile("pagerank-simple.txt", 1);

        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] parts = SPACES.split(s);
                return new Tuple2<String, String>(parts[0], parts[1]);
            }
        }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
            @Override
            public Double call(Iterable<String> rs) {
                return 1.0;
            }
        });

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < 10; current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaRDD<Tuple2<Iterable<String>, Double>> values = links.join(ranks).values();
            JavaPairRDD<String, Double> contribs = values
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                        @Override
                        public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
                            int urlCount = Iterables.size(s._1());
                            List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
                            for (String n : s._1()) {
                                results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
                            }
                            return results;
                        }
                    });
            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double sum) {
                    return 0.15 + sum * 0.85;
                }
            });
        }

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }



        // 1. 점수가 높은 순서대로 TOP 5 노드 출력.
        displayTop5(ranks);


        // 2. 평균값 출력
        displayAvg(ranks);


        ctx.stop();
    }


    // 1. 점수가 높은 순서대로 TOP 5 노드 출력 구현.
    private static void displayTop5(JavaPairRDD<String, Double> ranks) {
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

    // 3. 평균값 출력 구현.
    private static void displayAvg(JavaPairRDD<String, Double> ranks) {
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
}
