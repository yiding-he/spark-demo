package demo

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

object Lesson_01_初始化SparkSession {

  def main(args: Array[String]): Unit = {

    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("SparkDemo")
      .master("local[*]")
      .getOrCreate()

    println("SparkSession 已经初始化完成。")

    // 从 Seq 创建 DataFrame，可以有任意多列
    val data = Seq(
      ("Alice", 20),
      ("Bob", 30),
      ("Charlie", 40)
    )
    val df   = spark.createDataFrame(data).toDF("name", "age")
    df.show()

    // 从 Map 创建 DataFrame，只能有两个列
    val mapData = Map(
      "Alice" -> 20,
      "Bob" -> 30,
      "Charlie" -> 40
    )
    val mapDf   = spark.createDataFrame(
      mapData.map(kv => (kv._1, kv._2)).toSeq
    ).toDF("name", "age")
    mapDf.show()

    // 完整的创建 DataFrame 示例
    val rows       = java.util.List.of(
      Row("Alice", 20),
      Row("Bob", 30),
      Row("Charlie", 40)
    )
    val structType = new StructType()
      .add("name", "string")
      .add("age", "integer")
    val df2        = spark.createDataFrame(rows, structType)
    df2.show()

    // 关闭 SparkSession
    spark.close()
    println("SparkSession 已经关闭。")
  }
}
