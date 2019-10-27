package part03email;

import org.apache.flink.api.java.utils.ParameterTool;
import util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.bson.Document;

import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:52
 * @Author: code1990
 * @Description:
 */
public class EmailTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<EmailEntity> mapresult = text.map(new EmailMap());
        DataSet<EmailEntity> reduceresutl = mapresult.groupBy("groupfield").reduce(new EmailReduce());
        try {
            List<EmailEntity> reusltlist = reduceresutl.collect();
            for (EmailEntity emaiInfo : reusltlist) {
                String emailtype = emaiInfo.getEmailtype();
                Long count = emaiInfo.getCount();

                Document doc = MongoUtils.findOneBy("emailstatics", "test", emailtype);
                if (doc == null) {
                    doc = new Document();
                    doc.put("info", emailtype);
                    doc.put("count", count);
                } else {
                    Long countpre = doc.getLong("count");
                    Long total = countpre + count;
                    doc.put("count", total);
                }
                MongoUtils.saveOrUpdateMongo("emailstatics", "test", doc);
            }
            env.execute("email analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
