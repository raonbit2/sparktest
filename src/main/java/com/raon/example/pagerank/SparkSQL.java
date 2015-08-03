
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

public class SparkSQL extends PageRank {

    static SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[*]");
    static JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    static SQLContext sqlContext = new SQLContext(ctx);

    public SparkSQL(JavaSparkContext ctx) {
        super(ctx);
    }

    // RDD를 DataFrame로 변환 구현.
    // 참고: http://spark.apache.org/docs/latest/sql-programming-guide.html
    private void convertDataFrame(JavaPairRDD<String, Double> ranks) {

    }

    // 1. 점수가 높은 순서대로 TOP 5 노드 출력 구현.
    protected void displayTop5(JavaPairRDD<String, Double> ranks) {
        convertDataFrame(ranks);
        // 구현 ..
    }

    // 2. 평균값 출력 구현.
    protected void displayAvg(JavaPairRDD<String, Double> ranks) {
        convertDataFrame(ranks);
        // 구현 ..

    }

    public static void main(String[] args) throws Exception {
        new SparkSQL(ctx);
    }

}
