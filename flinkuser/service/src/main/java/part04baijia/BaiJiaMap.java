package part04baijia;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 16:41
 * @Author: code1990
 * @Description:
 */
public class BaiJiaMap implements MapFunction<String, BaiJiaEntity> {
    @Override
    public BaiJiaEntity map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }

        String[] orderinfos = s.split(",");
        String createtime = orderinfos[3];
        String amount = orderinfos[4];
        String paytype = orderinfos[5];
        String paytime = orderinfos[6];
        String paystatus = orderinfos[7];
        String couponamount = orderinfos[8];
        String totalamount = orderinfos[9];
        String refundamount = orderinfos[10];
        String userid = orderinfos[12];

        BaiJiaEntity baiJiaInfo = new BaiJiaEntity();
        baiJiaInfo.setUserid(userid);
        baiJiaInfo.setCreatetime(createtime);
        baiJiaInfo.setAmount(amount);
        baiJiaInfo.setPaytype(paytype);
        baiJiaInfo.setPaytime(paytime);
        baiJiaInfo.setPaystatus(paystatus);
        baiJiaInfo.setCouponamount(couponamount);
        baiJiaInfo.setTotalamount(totalamount);
        baiJiaInfo.setRefundamount(refundamount);
        String groupfield = "baijia==" + userid;
        baiJiaInfo.setGroupfield(groupfield);
        List<BaiJiaEntity> list = new ArrayList<BaiJiaEntity>();
        list.add(baiJiaInfo);
        return baiJiaInfo;
    }
}

