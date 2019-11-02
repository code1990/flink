package part10usertype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import util.MongoUtils;
import org.bson.Document;
/**
 * @program: flinkuser
 * @Date: 2019-11-02 7:33
 * @Author: code1990
 * @Description:
 */
public class UseTypeSink implements SinkFunction<UseTypeEntity> {
    @Override
    public void invoke(UseTypeEntity value, Context context) throws Exception {
        String usetype = value.getUsetype();
        long count = value.getCount();
        Document doc = MongoUtils.findOneBy("usetypestatics","youfanPortrait",usetype);
        if(doc == null){
            doc = new Document();
            doc.put("info",usetype);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre+count;
            doc.put("count",total);
        }
        MongoUtils.saveOrUpdateMongo("usetypestatics","youfanPortrait",doc);
    }
}
