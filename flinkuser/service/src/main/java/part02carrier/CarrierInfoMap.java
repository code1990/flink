package part02carrier;

import util.CarrierInfoUtils;
import util.HbaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:26
 * @Author: code1990
 * @Description:
 */
public class CarrierInfoMap implements MapFunction<String, CarrierInfoEntity> {
    @Override
    public CarrierInfoEntity map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String telphone = userinfos[3];

        int carriertype = CarrierInfoUtils.getCarrierByTel(telphone);
        String carriertypestring = carriertype == 0 ? "未知运营商" : carriertype == 1 ? "移动用户" : carriertype == 2 ? "联通用户" : "电信用户";

        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "baseinfo";
        String colum = "carrierinfo";//运营商
        HbaseUtils.putData(tablename, rowkey, famliyname, colum, carriertypestring);
        CarrierInfoEntity carrierInfo = new CarrierInfoEntity();
        String groupfield = "carrierInfo==" + carriertype;
        carrierInfo.setCount(1L);
        carrierInfo.setCarrier(carriertypestring);
        carrierInfo.setGroupfield(groupfield);
        return carrierInfo;
    }
}
