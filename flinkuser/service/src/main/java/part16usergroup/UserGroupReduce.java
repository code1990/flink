package part16usergroup;

import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: flinkuser
 * @Date: 2019-11-03 10:03
 * @Author: code1990
 * @Description:
 */
public class UserGroupReduce implements ReduceFunction<UserGroupEntity> {


    @Override
    public UserGroupEntity reduce(UserGroupEntity userGroupInfo1, UserGroupEntity userGroupInfo2) throws Exception {
        String userid = userGroupInfo1.getUserid();
        List<UserGroupEntity> list1 = userGroupInfo1.getList();
        List<UserGroupEntity> list2 = userGroupInfo2.getList();

        UserGroupEntity userGroupInfofinal = new UserGroupEntity();
        List<UserGroupEntity> finallist = new ArrayList<UserGroupEntity>();
        finallist.addAll(list1);
        finallist.addAll(list2);
        userGroupInfofinal.setList(finallist);
        return userGroupInfofinal;
    }
}
