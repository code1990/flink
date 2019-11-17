package part01wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*使用scala实现窗口的 单词统计*/
object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    //获取端口号
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port");
    } catch {
      case e: Exception => {
        System.err.println("not port set");
      }
        9000
    }

    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //获取socket连接输入的内容
    val text = env.socketTextStream("hadoop100", port, '\n');
    //解析数据 先把数据打平 分组 窗口计算 并且聚合求和

    //注意 必须添加如下的一行隐式转行 否则下面的flatMap方法执行会报错
    import org.apache.flink.api.scala._

    val windowCounts = text.flatMap(line => line.split("\\s")) //数据打平 把每一个单词都切开
      .map(w => new WordWithCount(w, 1)) //把单词换成word 1 这种形式
      .timeWindowAll(Time.seconds(2), Time.seconds(1)) //指定窗口大小 指定时间间隔
      .sum("sum"); //sum 或者reduce都可以
    //.reduce((a,b)=>WordWithCount(a.word,a.count+b.count))

    //打印到控制台 设置单线程打印
    windowCounts.print().setParallelism(1);
    //执行任务
    env.execute("socket window count");
  }

  case class WordWithCount(word: String, cunt: Long);
}
