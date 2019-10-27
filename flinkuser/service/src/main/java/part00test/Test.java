package part00test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 9:54
 * @Author: code1990
 * @Description:
 */
public class Test {
    public static void main(String[] args) {
        //把main方法的参数设置为params
        final ParameterTool params = ParameterTool.fromArgs(args);
        //设置启动环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //设置参数为全局参数
        env.getConfig().setGlobalJobParameters(params);
        //文件输入
        DataSet<String> text = env.readTextFile(params.get("input"));
        //
        DataSet map = text.flatMap(null);
        DataSet reduce = map.groupBy("groupbyfield").reduce(null);
        try {
            env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
