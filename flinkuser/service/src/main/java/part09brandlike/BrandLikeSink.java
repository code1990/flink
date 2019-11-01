package part09brandlike;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;
import util.MongoUtils;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 5:05
 * @Author: code1990
 * @Description:
 */
public class BrandLikeSink implements SinkFunction<BrandLikeEntity> {
    @Override
    public void invoke(BrandLikeEntity value, Context context) throws Exception {
        String brand = value.getBrand();
        long count = value.getCount();
        Document doc = MongoUtils.findOneBy("brandlikestatics","youfanPortrait",brand);
        if(doc == null){
            doc = new Document();
            doc.put("info",brand);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre+count;
            doc.put("count",total);
        }
        MongoUtils.saveOrUpdateMongo("brandlikestatics","youfanPortrait",doc);
    }
}
