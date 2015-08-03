
package sparktest;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaPageRank {
  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank").setMaster("local[*]");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    // 파일 로딩
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
    JavaRDD<String> lines = ctx.textFile("web-Google.txt", 1);

    //
    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();

/*
    JavaPairRDD<String, Integer> linkCounts = links.mapValues(new Function<Iterable<String>, Integer>() {
      @Override
      public Integer call(Iterable<String> strings) throws Exception {
        return Iterables.size(strings);
      }
    });
    JavaPairRDD<Integer, String> countLinks = linkCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
      @Override
      public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return stringIntegerTuple2.swap();
      }
    });
    List<Tuple2<Integer, String>> top10Links = countLinks.sortByKey(false).take(10);
    for (Tuple2<?,?> tuple : top10Links) {
        System.out.println(tuple._2() + " link 갯수: " + tuple._1());
    }
*/

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      @Override
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < 1; current++) {
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
    List<Tuple2<String, Double>> output = ranks.take(10);
    System.out.println("node count:" + output.size());
    for (Tuple2<?,?> tuple : output) {
        System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
    }

      // Create schema
      List<StructField> fields = new ArrayList<StructField>();
      fields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
      fields.add(DataTypes.createStructField("score", DataTypes.DoubleType, true));
      StructType schema = DataTypes.createStructType(fields);

      // Convert records of the RDD (people) to Rows.
      JavaRDD<Row> rowRDD = ranks.map(
              new Function<Tuple2<String,Double>, Row>() {
                  public Row call(Tuple2<String,Double> tuple) throws Exception {
                      return RowFactory.create(tuple._1(), tuple._2());
                  }
              });

      SQLContext sqlContext = new org.apache.spark.sql.SQLContext(ctx);
      // Apply the schema to the RDD.
      DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

      // Register the DataFrame as a table.
      peopleDataFrame.registerTempTable("PageRank");

      // Top 10
      DataFrame results = sqlContext.sql("SELECT url, score FROM PageRank order by score desc").limit(10);
      List<String> top10 = results.javaRDD().map(new Function<Row, String>() {
          public String call(Row row) {
              return "Url: " + row.getString(0) + ", Score:" + row.getDouble(1);
          }
      }).collect();
      for(String result : top10) {
          System.out.println(result);
      }

      // Age
      results = sqlContext.sql("SELECT avg(score) FROM PageRank");
      List<String> age = results.javaRDD().map(new Function<Row, String>() {
          public String call(Row row) {
              return "Age: " + row.getDouble(0);
          }
      }).collect();
      for(String result : age) {
          System.out.println(result);
      }

      ctx.stop();
  }
}
