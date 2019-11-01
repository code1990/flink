package util;

import java.util.*;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 5:13
 * @Author: code1990
 * @Description:
 */
public class MapUtils {

    public static String getmaxbyMap(Map<String,Long> datamap){
        if(datamap.isEmpty()){
            return  null;
        }
        TreeMap<Long,String> map = new TreeMap<Long, String>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }
        });
        Set<Map.Entry<String,Long>> set = datamap.entrySet();
        for(Map.Entry<String,Long> entry :set){
            String key = entry.getKey();
            Long value = entry.getValue();
            map.put(value,key);
        }
        return map.get(map.firstKey());
    }
}
