package demo

import org.apache.spark.sql.SparkSession

object Lesson_02_读取JSON数据文件 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkDemo").master("local[*]").getOrCreate()

    // 读取考生数据
    val studentDf = spark.read.json("data/student.json")

    // 显示考生数据
    studentDf.show(20)

    // 只选择需要的字段
    studentDf.select("student_id", "student_name").show(20)

    // 打印 DataFrame 的结构
    studentDf.printSchema()
  }
}
