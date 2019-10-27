package part02carrier;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import util.MongoUtils;

import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:26
 * @Author: code1990
 * @Description:
 */
public class CarrierInfoTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<CarrierInfoEntity> mapresult = text.map(new CarrierInfoMap());
        DataSet<CarrierInfoEntity> reduceresutl = mapresult.groupBy("groupfield").reduce(new CarrierInfoReduce());
        try {
            List<CarrierInfoEntity> reusltlist = reduceresutl.collect();
            for (CarrierInfoEntity carrierInfo : reusltlist) {
                String carrier = carrierInfo.getCarrier();
                Long count = carrierInfo.getCount();

                Document doc = MongoUtils.findOneBy("carrierstatics", "test", carrier);
                if (doc == null) {
                    doc = new Document();
                    doc.put("info", carrier);
                    doc.put("count", count);
                } else {
                    Long countpre = doc.getLong("count");
                    Long total = countpre + count;
                    doc.put("count", total);
                }
                MongoUtils.saveOrUpdateMongo("carrierstatics", "test", doc);
            }
            env.execute("carrier analyse");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
