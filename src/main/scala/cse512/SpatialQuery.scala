package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {

    // split the rectangle string into individual coordinates
    val recArr = queryRectangle.split(",");

    // first point
    val x1 = recArr(0).toDouble;
    val y1 = recArr(1).toDouble;

    // second point
    val x2 = recArr(2).toDouble;
    val y2 = recArr(3).toDouble;

    // point under test
    val point = pointString.split(",")
    val pointx = point(0).toDouble;
    val pointy = point(1).toDouble;

    return x1 <= pointx && pointx <= x2 &&
           y1 <= pointy && pointy <= y2;
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean ={
    // split the rectangle string into individual coordinates
    val recArr = pointString1.split(",");

    // point1
    val point1 = pointString1.split(",")
    val x1 = point1(0).toDouble;
    val y1 = point1(1).toDouble;

    // point2
    val point2 = pointString2.split(",")
    val x2 = point2(0).toDouble;
    val y2 = point2(1).toDouble;

    // distance formulat √((x2-x1)^2 + (y2-y1)^2)
    val distanceFromAtoB = Math.sqrt(Math.pow(x2-x1, 2) + Math.pow(y2-y1,2))

    return distanceFromAtoB <= distance;
  }
}
