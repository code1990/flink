package part04baijia;

import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 16:41
 * @Author: code1990
 * @Description:
 */
public class BaiJiaReduce implements ReduceFunction<BaiJiaEntity> {

    @Override
    public BaiJiaEntity reduce(BaiJiaEntity baiJiaInfo, BaiJiaEntity t1) throws Exception {
        String userid = baiJiaInfo.getUserid();
        List<BaiJiaEntity> baijialist1 = baiJiaInfo.getList();
        List<BaiJiaEntity> baijialist2 = t1.getList();
        List<BaiJiaEntity> finallist = new ArrayList<BaiJiaEntity>();
        finallist.addAll(baijialist1);
        finallist.addAll(baijialist2);

        BaiJiaEntity baiJiaInfofinal = new BaiJiaEntity();
        baiJiaInfofinal.setUserid(userid);
        baiJiaInfofinal.setList(finallist);
        return baiJiaInfofinal;
    }
}

