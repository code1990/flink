package part21idf;

import org.apache.flink.api.common.functions.MapFunction;
import part20tfidf.IkUtil;
import util.HbaseUtils;

import java.util.*;

/**
 * Created by li on 2019/1/20.
 */

/**
 *一段文本
 */
public class IdfMap implements MapFunction<String, IdfEntity> {

    @Override
    public IdfEntity map(String s) throws Exception {
        Map<String,Long> tfmap = new HashMap<String,Long>();
        List<String> listdata = IkUtil.getIkWord(s);
        Set<String> wordset = new HashSet<String>();
        for(String word:listdata){
            Long pre = tfmap.get(word)==null?0L:tfmap.get(word);
            tfmap.put(word,pre+1);
            wordset.add(word);
        }
        String docuemtnid = UUID.randomUUID().toString();
        IdfEntity idfEntity = new IdfEntity();
        idfEntity.setDocumentid(docuemtnid);
        idfEntity.setDatamap(tfmap);

        //计算总数
        long sum = 0L;
        Collection<Long> longset = tfmap.values();
        for(Long templong:longset){
                sum += templong;
        }

        Map<String,Double> tfmapfinal = new HashMap<String,Double>();
        Set<Map.Entry<String,Long>> entryset = tfmap.entrySet();
        for(Map.Entry<String,Long> entry:entryset){
                String word = entry.getKey();
                long count = entry.getValue();
                double tf = Double.valueOf(count)/Double.valueOf(sum);
                tfmapfinal.put(word,tf);
        }
        idfEntity.setTfmap(tfmapfinal);

        //create "tfidfdata,"baseinfo"
        for(String word:wordset){
            String tablename = "tfidfdata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtils.getData(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0L:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtils.putData(tablename,rowkey,famliyname,colum,total+"");
        }
        return idfEntity;
    }
}
