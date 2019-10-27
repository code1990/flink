package part02carrier;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:26
 * @Author: code1990
 * @Description:
 */
public class CarrierInfoReduce implements ReduceFunction<CarrierInfoEntity> {

    @Override
    public CarrierInfoEntity reduce(CarrierInfoEntity carrierInfo, CarrierInfoEntity t1) throws Exception {
        String carrier = carrierInfo.getCarrier();
        Long count1 = carrierInfo.getCount();
        Long count2 = t1.getCount();

        CarrierInfoEntity carrierInfofinal = new CarrierInfoEntity();
        carrierInfofinal.setCarrier(carrier);
        carrierInfofinal.setCount(count1 + count2);
        return carrierInfofinal;
    }
}
