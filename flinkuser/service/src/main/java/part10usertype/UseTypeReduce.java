package part10usertype;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 7:33
 * @Author: code1990
 * @Description:
 */
public class UseTypeReduce implements ReduceFunction<UseTypeEntity> {

    @Override
    public UseTypeEntity reduce(UseTypeEntity useTypeInfo, UseTypeEntity t1) throws Exception {
        String usertype = useTypeInfo.getUsetype();
        Long count1 = useTypeInfo.getCount();

        Long count2 = t1.getCount();

        UseTypeEntity useTypeInfofinal = new UseTypeEntity();
        useTypeInfofinal.setUsetype(usertype);
        useTypeInfofinal.setCount(count1+count2);
        return useTypeInfofinal;
    }
}
