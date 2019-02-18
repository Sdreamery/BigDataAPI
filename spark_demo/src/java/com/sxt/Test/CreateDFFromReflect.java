package com.sxt.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.HashSet;

public class CreateDFFromReflect {

    public static final Person PERSON = new Person();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("jsonfile");

        JavaSparkContext context = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(context);

        JavaRDD<String> lineRDD = context.textFile("data/person.txt");

        JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                Person person = new Person();
                String[] split = line.split(",");
                person.setAge(Long.valueOf(split[2]));
                person.setName(split[1]);
                person.setId(Long.valueOf(split[0]));
                return person;
            }
        });

        DataFrame dataFrame = sqlContext.createDataFrame(personRDD, Person.class);

        dataFrame.show();


    }
}
