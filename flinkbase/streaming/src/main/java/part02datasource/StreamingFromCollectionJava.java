package part02datasource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @program: flinkbase
 * @Date: 2019-11-17 20:24
 * @Author: code1990
 * @Description: 读取集合的数据
 */
public class StreamingFromCollectionJava {
    public static void main(String[] args) throws Exception {
        //获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //声明一个数组
        ArrayList<Integer> data = new ArrayList<>();
        data.add(1);
        data.add(11);
        data.add(111);
        //从集合中读取数据
        DataStreamSource<Integer> collectionData = env.fromCollection(data);
        //通过Map对数据进行处理
        DataStream<Integer> num = collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                //遍历每一个数字+1
                return integer+1;
            }
        });
        //直接打印数据 并设置并行度
        num.print().setParallelism(1);
        //必须执行 否则无结果
        env.execute("collection sources");
    }
}
