package part21idf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * Created by li on 2019/1/5.
 */
public class IdfTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<IdfEntity> mapresult = text.map(new IdfMap());
        DataSet<IdfEntity> reduceresult = mapresult.reduce(new IdfReduce());
        Long totaldoucment = 0L;
        try {
            totaldoucment = reduceresult.collect().get(0).getTotaldocumet();
            DataSet<IdfEntity> mapfinalresult = mapresult.map(new IdfMapfinal(totaldoucment,3));
            mapfinalresult.writeAsText("hdfs://youfan/test");//hdfs的路径
            env.execute("TFIDFTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
