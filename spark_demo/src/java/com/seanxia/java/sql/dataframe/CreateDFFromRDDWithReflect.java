package com.seanxia.spark.java.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDFFromRDDWithReflect {
    public static void main(String[] args){

        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("RDD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lineRDD = sc.textFile("./data/person.txt");

        JavaRDD<Person> personRDD = lineRDD.map(new Function<String, Person>() {
            @Override
            public Person call(String s) throws Exception {
                Person p = new Person();
                p.setId(s.split(",")[0]);
                p.setName(s.split(",")[1]);
                p.setAge(Integer.valueOf(s.split(",")[2]));
                return p;
            }
        });

        /**
         * 传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
         * 在底层通过反射的方式获得Person的所有field，结合RDD本身，就生成了DataFrame
         */
        DataFrame df = sqlContext.createDataFrame(personRDD, Person.class);
        df.show();

        df.registerTempTable("person");
        sqlContext.sql("select name from person where id = 2").show();

        /**
         * 将DataFrame转成JavaRDD
         * 注意：
         * 1.可以使用row.getInt(0),row.getString(1)...
         *   通过下标获取返回Row类型的数据，但是要注意列顺序问题---不常用
         * 2.可以使用row.getAs("列名")来获取对应的列值
         */
        JavaRDD<Row> javaRDD = df.javaRDD();

        JavaRDD<Person> map = javaRDD.map(new Function<Row, Person>() {
            @Override
            public Person call(Row row) throws Exception {
                Person p = new Person();

                //p.setId(row.getString(1));
                //p.setName(row.getString(2));
                //p.setAge(row.getInt(0));

                p.setId((String) row.getAs("id"));
                p.setName((String) row.getAs("name"));
                p.setAge((Integer) row.getAs("age"));

                return p;
            }
        });

        map.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

        sc.stop();
    }
}
