package part23year;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import part22keyword.KeyWordEntity;
import part22keyword.KeyWordMapfinal;
import part22keyword.KeywordMap;
import part22keyword.KeywordReduce;
import part22keyword2.KeyWordReduce2;
import part22keyword2.KeywordMap2;

/**
 * Created by li on 2019/1/6.
 */
public class YearKeyWordTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<KeyWordEntity> mapresult = text.map(new KeywordMap());
        DataSet<KeyWordEntity> reduceresutl = mapresult.groupBy("userid").reduce(new KeywordReduce());
        DataSet<KeyWordEntity> mapresult2 = reduceresutl.map(new KeywordMap2());
        DataSet<KeyWordEntity> reduceresult2 = mapresult2.reduce(new KeyWordReduce2());
        Long totaldoucment = 0L;
        try {
            totaldoucment = reduceresult2.collect().get(0).getTotaldocumet();
            DataSet<KeyWordEntity> mapfinalresult = mapresult.map(new KeyWordMapfinal(totaldoucment,3,"year"));
            mapfinalresult.writeAsText("hdfs://youfan/test/year");//hdfs的路径
            env.execute("YearKeyWordTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}