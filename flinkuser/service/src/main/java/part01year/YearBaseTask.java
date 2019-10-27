package part01year;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import util.MongoUtils;

import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 11:08
 * @Author: code1990
 * @Description:年代标签的开发
 */
public class YearBaseTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<YearBaseEntity> mapResult = text.map(new YearBaseMap());
        DataSet<YearBaseEntity> reduceResult = mapResult.groupBy("groupfield")
                .reduce(new YearBaseReduce());
        try {
            List<YearBaseEntity> resultList = reduceResult.collect();
            for (YearBaseEntity yearBaseEntity : resultList) {
                String yearType = yearBaseEntity.getYeartype();
                Long count = yearBaseEntity.getCount();
                Document document = MongoUtils.findOneBy("yearbasestatics", "test", yearType);
                if (document == null) {
                    document = new Document();
                    document.put("info", yearType);
                    document.put("count", count);
                } else {
                    Long countPre = document.getLong("count");
                    Long total = countPre + count;
                    document.put("count", total);
                }
                MongoUtils.saveOrUpdateMongo("yearbasestatics", "test", document);
            }
            env.execute("year base analyse");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
