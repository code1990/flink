package part02datasource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object StreamingFromCollectionScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    import org.apache.flink.api.scala._

    val data = List(1,11,111)

    val text = env.fromCollection(data)

    val num = text.map(_+1)

    num.print().setParallelism(1)

    env.execute("StreamingFromCollectionScala")
  }

}
