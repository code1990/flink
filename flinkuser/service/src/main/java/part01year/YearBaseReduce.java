package part01year;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 13:54
 * @Author: code1990
 * @Description:
 */
public class YearBaseReduce implements ReduceFunction<YearBaseEntity> {

    @Override
    public YearBaseEntity reduce(YearBaseEntity yearBaseEntity, YearBaseEntity t1) throws Exception {
        String yearType = yearBaseEntity.getYeartype();
        Long count1 = yearBaseEntity.getCount();
        Long count2 = t1.getCount();
        YearBaseEntity entity = new YearBaseEntity();
        entity.setYeartype(yearType);
        entity.setCount(count1+count2);
        return entity;
    }
}
