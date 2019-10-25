package com.pash.ai.etl;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import  org.apache.spark.sql.functions.*;
import org.apache.spark.sql.catalog.Function;

import java.sql.Timestamp;

public class Tokenizer {


    public void read(){

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[1]")
                .getOrCreate();
        final DataFrameReader dataFrameReader = spark.sqlContext().read().format("csv").option("inferSchema", "true").option("delimiter", " ").option("escape", "\"").option("quote", "\"");
        final Dataset<Row> df = dataFrameReader.load("/Users/prasad.shetkar/Documents/workspace/personal/repos/DataEngineerChallenge/data/2015_07_22_mktplace_shop_web_log_sample.log");

        final String column = df.columns()[5];
        df.registerTempTable("logs");
        //final Dataset<Row> sql = df.sqlContext().sql("SELECT count(*) ,session FROM (SELECT unix_timestamp(_c0) as epoch_time,*, floor(unix_timestamp(_c0)/(60*15)) as session  FROM logs) as t group by session");

        final Dataset<Row> sessionize = df.sqlContext().sql("SELECT unix_timestamp(_c0) as epoch_time,*,unix_timestamp(_c0) as timeInSeconds , floor(unix_timestamp(_c0)/(60*15)) as session, split(_c2,':')[0] as user_ip  FROM logs  DISTRIBUTE BY session,user_ip");

        sessionize.registerTempTable("sessionize");

        //sessionize.sqlContext().sql("SELECT * FROM sessionize WHERE _c11 is like '%tcp%'").show();
        sessionize.sqlContext().sql("SELECT * FROM sessionize WHERE _c11  like '%tcp%' ").show(); /*showing the details of the records being sessionized*/



    }


}
