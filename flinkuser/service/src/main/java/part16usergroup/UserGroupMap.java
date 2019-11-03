package part16usergroup;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-11-03 10:03
 * @Author: code1990
 * @Description:
 */
public class UserGroupMap implements MapFunction<String, UserGroupEntity> {

    @Override
    public UserGroupEntity map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] orderinfos = s.split(",");
        String id= orderinfos[0];
        String productid = orderinfos[1];
        String producttypeid = orderinfos[2];
        String createtime = orderinfos[3];
        String amount = orderinfos[4];
        String paytype = orderinfos[5];
        String paytime = orderinfos[6];
        String paystatus = orderinfos[7];
        String couponamount = orderinfos[8];
        String totalamount = orderinfos[9];
        String refundamount = orderinfos[10];
        String num = orderinfos[11];
        String userid = orderinfos[12];

        UserGroupEntity userGroupInfo = new UserGroupEntity();
        userGroupInfo.setUserid(userid);
        userGroupInfo.setCreatetime(createtime);
        userGroupInfo.setAmount(amount);
        userGroupInfo.setPaytype(paytype);
        userGroupInfo.setPaytime(paytime);
        userGroupInfo.setPaystatus(paystatus);
        userGroupInfo.setCouponamount(couponamount);
        userGroupInfo.setTotalamount(totalamount);
        userGroupInfo.setRefundamount(refundamount);
        userGroupInfo.setCount(Long.valueOf(num));
        userGroupInfo.setProducttypeid(producttypeid);
        userGroupInfo.setGroupfield(userid+"==userGroupinfo");
        List<UserGroupEntity> list = new ArrayList<UserGroupEntity>();
        list.add(userGroupInfo);
        userGroupInfo.setList(list);
        return userGroupInfo;
    }
}
