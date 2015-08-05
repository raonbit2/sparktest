
package com.raon.example.pagerank;

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class PageRank implements Serializable {
    private static final Pattern SPACES = Pattern.compile("\\s+");

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public JavaPairRDD<String, Double> getRank(JavaRDD<String> lines) {

        JavaPairRDD<String, Double> ranks = null;
        // File Loading
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...

        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] parts = SPACES.split(s);
                return new Tuple2<String, String>(parts[0], parts[1]);
            }
        }).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        ranks = links.mapValues(new Function<Iterable<String>, Double>() {
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

        return ranks;
    }

}
