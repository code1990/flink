package part10usertype;

import com.alibaba.fastjson.JSONObject;
import log.ScanProductLog;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import part09kafkaevent.KafkaEvent;
import util.HbaseUtils;
import util.MapUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 7:33
 * @Author: code1990
 * @Description:
 */
public class UseTypeMap implements FlatMapFunction<KafkaEvent, UseTypeEntity> {

    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<UseTypeEntity> collector) throws Exception {
        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        int usetype = scanProductLog.getUsetype();////终端类型：0、pc端；1、移动端；2、小程序端
        String usetypename = usetype == 0 ? "pc端" : usetype == 1 ? "移动端" : "小程序端";
        String tablename = "userflaginfo";
        String rowkey = userid + "";
        String famliyname = "userbehavior";
        String colum = "usetypelist";//运营
        String mapdata = HbaseUtils.getData(tablename, rowkey, famliyname, colum);
        Map<String, Long> map = new HashMap<String, Long>();
        if (StringUtils.isNotBlank(mapdata)) {
            map = JSONObject.parseObject(mapdata, Map.class);
        }
        //获取之前的终端偏好
        String maxpreusetype = MapUtils.getmaxbyMap(map);

        long preusetype = map.get(usetypename) == null ? 0L : map.get(usetypename);
        map.put(usetypename, preusetype + 1);
        String finalstring = JSONObject.toJSONString(map);
        HbaseUtils.putData(tablename, rowkey, famliyname, colum, finalstring);

        String maxusetype = MapUtils.getmaxbyMap(map);
        if (StringUtils.isNotBlank(maxusetype) && !maxpreusetype.equals(maxusetype)) {
            UseTypeEntity useTypeInfo = new UseTypeEntity();
            useTypeInfo.setUsetype(maxpreusetype);
            useTypeInfo.setCount(-1L);
            useTypeInfo.setGroupbyfield("==usetypeinfo==" + maxpreusetype);
            collector.collect(useTypeInfo);
        }

        UseTypeEntity useTypeInfo = new UseTypeEntity();
        useTypeInfo.setUsetype(maxusetype);
        useTypeInfo.setCount(1L);
        useTypeInfo.setGroupbyfield("==usetypeinfo==" + maxusetype);
        collector.collect(useTypeInfo);
        colum = "usetype";
        HbaseUtils.putData(tablename, rowkey, famliyname, colum, maxusetype);

    }

}
