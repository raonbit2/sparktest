
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
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class SparkSQL {
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




    SQLContext sqlContext = new SQLContext(ctx);

    // 1. RDD를 DataFrame로 변환.
    DataFrame dataFrame = convertDataFrame(sqlContext, ranks);


    // 2. 점수가 높은 순서대로 TOP 5 노드 출력.
    displayTop5(sqlContext);


    // 3. 평균값 출력
    displayAvg(sqlContext);


    ctx.stop();
  }

  // 1. RDD를 DataFrame로 변환 구현.
  // http://spark.apache.org/docs/latest/sql-programming-guide.html 참고
  private static DataFrame convertDataFrame(SQLContext sqlContext, JavaPairRDD<String, Double> ranks) {
      // Create schema
      List<StructField> fields = new ArrayList<StructField>();
      fields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
      fields.add(DataTypes.createStructField("score", DataTypes.DoubleType, true));
      StructType schema = DataTypes.createStructType(fields);

      // Convert records of the RDD (rank) to Rows.
      JavaRDD<Row> rowRDD = ranks.map(
              new Function<Tuple2<String, Double>, Row>() {
                  public Row call(Tuple2<String, Double> tuple) throws Exception {
                      return RowFactory.create(tuple._1(), tuple._2());
                  }
              });


      // Apply the schema to the RDD.
      DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);

      // Register the DataFrame as a table.
      dataFrame.registerTempTable("PageRank");

      return dataFrame;
  }

  // 2. 점수가 높은 순서대로 TOP 5 노드 출력 구현.
  private static void displayTop5(SQLContext sqlContext) {
      DataFrame results = sqlContext.sql("SELECT url, score FROM PageRank order by score desc").limit(5);
      List<String> top5 = results.javaRDD().map(new Function<Row, String>() {
          public String call(Row row) {
              return "Url: " + row.getString(0) + ", Score:" + row.getDouble(1);
          }
      }).collect();
      for(String result : top5) {
          System.out.println(result);
      }
  }

  // 3. 평균값 출력 구현.
  private static void displayAvg(SQLContext sqlContext) {
      DataFrame results = sqlContext.sql("SELECT avg(score) FROM PageRank");
      List<String> age = results.javaRDD().map(new Function<Row, String>() {
          public String call(Row row) {
              return "Avg: " + row.getDouble(0);
          }
      }).collect();
      for(String result : age) {
          System.out.println(result);
      }
  }
}
