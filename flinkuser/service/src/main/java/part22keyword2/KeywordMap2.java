package part22keyword2;

import org.apache.flink.api.common.functions.MapFunction;
import part20tfidf.IkUtil;
import part22keyword.KeyWordEntity;
import util.HbaseUtils;

import java.util.*;

/**
 * Created by li on 2019/1/20.
 */

/**
 */
public class KeywordMap2 implements MapFunction<KeyWordEntity, KeyWordEntity> {

    @Override
    public KeyWordEntity map(KeyWordEntity keyWordEntity) throws Exception {
        List<String> words = keyWordEntity.getOriginalwords();
        Map<String,Long> tfmap = new HashMap<String,Long>();
        Set<String> wordset = new HashSet<String>();
        for(String outerword:words){
            List<String> listdata = IkUtil.getIkWord(outerword);
            for(String word:listdata){
                Long pre = tfmap.get(word)==null?0L:tfmap.get(word);
                tfmap.put(word,pre+1);
                wordset.add(word);
            }
        }

        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        String userid = keyWordEntity.getUserid();
        keyWordEntityfinal.setUserid(userid);
        keyWordEntityfinal.setDatamap(tfmap);

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
        keyWordEntityfinal.setTfmap(tfmapfinal);

        //create "keyworddata,"baseinfo"
        for(String word:wordset){
            String tablename = "keyworddata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtils.getData(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0L:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtils.putData(tablename,rowkey,famliyname,colum,total+"");
        }
        return keyWordEntity;
    }
}
