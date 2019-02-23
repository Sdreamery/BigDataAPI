package com.seanxia.spark.scala.sql.udf_dfaf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MyUDAF extends UserDefinedAggregateFunction {
  // 聚合操作时，所处理的数据的类型
  def bufferSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("aaa",
      IntegerType, true)))
  }
  // 最终函数返回值的类型
  def dataType: DataType = {
    DataTypes.IntegerType
  }
  def deterministic: Boolean = {
    true
  }
  // 最后返回一个最终的聚合值 要和dataType的类型一一对应
  def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
  // 为每个分组的数据执行初始化值
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }
  //输入数据的类型
  def inputSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("input",
      StringType, true)))
  }
  // 最后merger的时候，在各个节点上的聚合值，要进行merge，也就是合并
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }
  // 每个组，有新的值进来的时候，进行分组对应的聚合值的计算
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0)+1
  }
}
object UDAF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local").setAppName("udaf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rdd = sc.makeRDD(Array("zhangsan","lisi","wangwu","zhangsan","lisi"))
    val rowRDD = rdd.map { x => {RowFactory.create(x)} }
    val  schema  = DataTypes.createStructType(Array(
      DataTypes.createStructField("name", StringType, true)))
    val df = sqlContext.createDataFrame(rowRDD, schema)

    df.show()

    df.registerTempTable("user")
    /**
      * 注册一个udaf函数
      */
    sqlContext.udf.register("StringCount", new MyUDAF())
    sqlContext.sql("select name ,StringCount(name) from user group byname").show()
      sc.stop()
      }
}