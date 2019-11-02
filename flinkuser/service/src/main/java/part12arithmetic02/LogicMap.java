package part12arithmetic02;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 13:37
 * @Author: code1990
 * @Description:
 */
public class LogicMap implements MapFunction<String, LogicEntity> {
    @Override
    public LogicEntity map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        Random random = new Random();
        String [] temps = s.split(",");
        String variable1 = temps[0];
        String variable2 = temps[1];
        String variable3 = temps[2];
        String labase = temps[3];
        LogicEntity logicInfo = new LogicEntity();
        logicInfo.setVariable1(variable1);
        logicInfo.setVariable2(variable2);
        logicInfo.setVariable3(variable3);
        logicInfo.setLabase(labase);
        logicInfo.setGroupbyfield("logic=="+random.nextInt(10));
        return logicInfo;
    }
}
