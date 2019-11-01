package part09brandlike;

import com.alibaba.fastjson.JSONObject;
import log.ScanProductLog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import part09kafkaevent.KafkaEvent;
import util.HbaseUtils;
import org.apache.commons.lang.StringUtils;
import util.MapUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: flinkuser
 * @Date: 2019-10-31 23:14
 * @Author: code1990
 * @Description:
 */
public class BrandLikeMap implements FlatMapFunction<KafkaEvent, BrandLikeEntity> {

    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<BrandLikeEntity> collector) throws Exception {
        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data,ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        String brand = scanProductLog.getBrand();
        String tablename = "userflaginfo";
        String rowkey = userid+"";
        String famliyname = "userbehavior";
        String colum = "brandlist";//运营
        String mapdata = HbaseUtils.getData(tablename,rowkey,famliyname,colum);
        Map<String,Long> map = new HashMap<String,Long>();
        if(StringUtils.isNotBlank(mapdata)){
            map = JSONObject.parseObject(mapdata,Map.class);
        }
        //获取之前的品牌偏好
        String maxprebrand = MapUtils.getmaxbyMap(map);

        long prebarnd = map.get(brand)==null?0L:map.get(brand);
        map.put(brand,prebarnd+1);
        String finalstring = JSONObject.toJSONString(map);
        HbaseUtils.putData(tablename,rowkey,famliyname,colum,finalstring);

        String maxbrand = MapUtils.getmaxbyMap(map);
        if(StringUtils.isNotBlank(maxbrand)&&!maxprebrand.equals(maxbrand)){
            BrandLikeEntity brandLike = new BrandLikeEntity();
            brandLike.setBrand(maxprebrand);
            brandLike.setCount(-1L);
            brandLike.setGroupbyfield("==brandlik=="+maxprebrand);
            collector.collect(brandLike);
        }

        BrandLikeEntity brandLike = new BrandLikeEntity();
        brandLike.setBrand(maxbrand);
        brandLike.setCount(1L);
        collector.collect(brandLike);
        brandLike.setGroupbyfield("==brandlik=="+maxbrand);
        colum = "brandlike";
        HbaseUtils.putData(tablename,rowkey,famliyname,colum,maxbrand);

    }

}

