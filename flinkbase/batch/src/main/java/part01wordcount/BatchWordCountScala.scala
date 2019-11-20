package part01wordcount

import org.apache.flink.api.java.ExecutionEnvironment

//出现类型推断错误 代码调试经验不足暂时无法解决
//所以注释掉问题代码
object BatchWordCountScala {


  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\hello.txt"
    val outPath = "D:\\hello1.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)
    import org.apache.flink.streaming.api.scala._
//    val word = text.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
//    word.print()
//    word.writeAsText(outPath)
    env.execute("flink wordcount demo")
    //引入隐式转换

//    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
//      .filter(_.nonEmpty)
//      .map((_,1))
//      .groupBy(0)
//      .sum(1)
//    counts.writeAsCsv(outPut,"\n"," ").setParallelism(1)
    env.execute("batch word count")
  }
}
