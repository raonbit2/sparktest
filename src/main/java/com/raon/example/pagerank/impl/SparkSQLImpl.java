
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

public class SparkSQLImpl {

    static SparkConf sparkConf = new SparkConf();
    static JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    static SQLContext sqlContext = new SQLContext(ctx);

    // 0. RDD를 DataFrame로 변환 구현.
    // http://spark.apache.org/docs/latest/sql-programming-guide.html 참고
    private static void convertDataFrame(JavaPairRDD<String, Double> ranks) {
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
    }

    // 1. 점수가 높은 순서대로 TOP 5 노드 출력 구현.
    public static void displayTop5(JavaPairRDD<String, Double> ranks) {
        convertDataFrame(ranks);

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

    // 2. 평균값 출력 구현.
    public static void displayAvg(JavaPairRDD<String, Double> ranks) {
        convertDataFrame(ranks);
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

    public static void main(String[] args) throws Exception {

        JavaRDD<String> lines = ctx.textFile("pagerank-simple.txt");

        PageRank pagerank = new PageRank();

        JavaPairRDD<String, Double> ranks = pagerank.getRank(lines);

        displayTop5(ranks);

        displayAvg(ranks);


    }
}
