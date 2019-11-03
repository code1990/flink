package part21idf;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by li on 2019/1/5.
 */
public class IdfReduce implements ReduceFunction<IdfEntity>{


    @Override
    public IdfEntity reduce(IdfEntity idfEntity1, IdfEntity idfEntity2) throws Exception {

        long count1 = idfEntity1.getTotaldocumet();
        long count2 = idfEntity2.getTotaldocumet();
        IdfEntity idfEntity = new IdfEntity();
        idfEntity.setTotaldocumet(count1 + count2);
        return idfEntity;
    }
}
