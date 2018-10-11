package com.test

import org.apache.spark.sql.SparkSession

object FormatCheck {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;
    import spark.implicits._

    val data = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("F:/Ubuntu_Shared_Folder/daily_pos_850321_20160801104254.csv").toDF()
    var rcdMatch = true
    
    data.foreach { record =>
      var iMdmArr = record.getAs("iMDM_Unique_Identifier").toString.split("-")
      if (iMdmArr.size == 2 && iMdmArr(0).toString.equalsIgnoreCase(record.getAs("source_system").toString)
        && iMdmArr(0).toString.equalsIgnoreCase(record.getAs("source_system_record_id").toString)) {
        rcdMatch = false
      }
    }
    rcdMatch

    /*val data = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", "|").load("F:/Ubuntu_Shared_Folder/daily_pos_850321_20160801104254.csv").toDF()
    val unqRow = data.groupBy("DDD").count.sort($"count".desc).toDF()

    var result = true
    unqRow.foreach { r =>
      if(r.getAs("count").asInstanceOf[Long] > 1) {
        result = false
      }
    }
    result*/
  }
}