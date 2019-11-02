package part12arithmetic02;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import part11arithmetic01.CreateDataSet;
import part11arithmetic01.Logistic;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 13:37
 * @Author: code1990
 * @Description:
 */
public class LogicReduce implements GroupReduceFunction<LogicEntity,ArrayList<Double>> {
    @Override
    public void reduce(Iterable<LogicEntity> iterable, Collector<ArrayList<Double>> collector) throws Exception {
        Iterator<LogicEntity> iterator = iterable.iterator();
        CreateDataSet trainingSet = new CreateDataSet();
        while(iterator.hasNext()){
            LogicEntity logicInfo = iterator.next();
            String variable1 = logicInfo.getVariable1();
            String variable2 = logicInfo.getVariable2();
            String variable3 = logicInfo.getVariable3();
            String label = logicInfo.getLabase();


            ArrayList<String> as = new ArrayList<String>();
            as.add(variable1);
            as.add(variable2);
            as.add(variable3);

            trainingSet.data.add(as);
            trainingSet.labels.add(label);
        }
        ArrayList<Double> weights = new ArrayList<Double>();
        weights = Logistic.gradAscent1(trainingSet, trainingSet.labels, 500);
        collector.collect(weights);
    }
}
