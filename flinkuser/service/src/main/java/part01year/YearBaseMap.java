package part01year;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import util.DateUtils;
import util.HbaseUtils;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 11:07
 * @Author: code1990
 * @Description:年代标签的开发
 */
public class YearBaseMap implements MapFunction<String,YearBaseEntity>{

    @Override
    public YearBaseEntity map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String username = userinfos[1];
        String sex = userinfos[2];
        String telphone = userinfos[3];
        String email = userinfos[4];
        String age = userinfos[5];
        String registerTime = userinfos[6];
        String usetype = userinfos[7];//'终端类型：0、pc端；1、移动端；2、小程序端'
        String yearBaseType = DateUtils.getYearbasebyAge(age);
        String tableName = "userflaginfo";
        String rowKey = userid;
        String familyName ="baseInfo";
        String column = "yearbase";
        HbaseUtils.putData(tableName,rowKey,familyName,column,yearBaseType);
        HbaseUtils.putData(tableName,rowKey,familyName,"age",age);
        YearBaseEntity yearBase = new YearBaseEntity();
        String groupField = "yearbase=="+yearBaseType;
        yearBase.setYeartype(yearBaseType);
        yearBase.setCount(1L);
        yearBase.setGroupfield(groupField);
        return yearBase;
    }
}
