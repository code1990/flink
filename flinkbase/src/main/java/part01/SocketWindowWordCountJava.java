package part01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @program: flinkbase
 * @Date: 2019-11-04 21:18
 * @Author: code1990
 * @Description: 通过socket模拟产生数据 flink对数据执行实时计算
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
        //运行环境的准备
        //00获取需要的端口号
        int port =9000;
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.out.println(" not port set use default port");
            e.printStackTrace();
        }
        //获取需要的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "hadoop100";
        String delimiter = "\n";
        //连接socket获取数据的数据 例如 a b c
        DataStreamSource<String> text = env.socketTextStream(hostname,port,delimiter);
        //对a b c数据执行统计 a 1 b 1 c 1
        DataStream<WordWithCount> windowCounts =
                //先设置拆分的方法
                text.flatMap(new FlatMapFunction<String, WordWithCount>() {

                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                        String[] splits = value.split("\\s");
                        for(String word:splits){
                            out.collect(new WordWithCount(word,1L));
                        }
                    }
                    //设置分组的方式
                }).keyBy("word")
                        //指定时间窗口大小为2 指定时间间隔为1秒 即为没2秒执行一次
                .timeWindow(Time.seconds(2),Time.seconds(1))
                        //指定统计的方式
                .sum("count");
                //不使用sum函数 使用reduce函数来实现的方式如下
//                .reduce(new ReduceFunction<WordWithCount>(){
//                    @Override
//                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
//                        return new WordWithCount(a.word,a.count+b.count);
//                    }
//                });
        //设置打印的并行度
        windowCounts.print().setParallelism(1);
        //flink延迟加载 手动执行execute
        env.execute("socket with window");
    }
    public static class WordWithCount{
        //被统计的单词
        private String word;
        //被统计的单词数量
        private long count;

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public WordWithCount() {
        }
    }
}
