package demo

import org.apache.spark.sql.SparkSession

object Lesson_03_连接Join {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkDemo").master("local[*]").getOrCreate()

    // 读取考生、班级、学校数据
    val studentDf = spark.read.json("data/student.json")
    val classDf   = spark.read.json("data/class.json")
    val schoolDf  = spark.read.json("data/school.json")

    // 连接考生、班级、学校数据
    // 演示两种不同的 join 方法调用方式
    val joinedDf = studentDf
      .join(classDf, studentDf("class_id") === classDf("class_id")) // inner join
      .join(schoolDf, Seq("school_id"), "left") // left join

    // 显示结果
    joinedDf
      .select("school_name", "class_name", "student_name")
      .show(20)
  }
}
