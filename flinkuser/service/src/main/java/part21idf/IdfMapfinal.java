package part21idf;

import org.apache.flink.api.common.functions.MapFunction;
import util.HbaseUtils;
import util.MapUtils;

import java.util.*;

/**
 * Created by li on 2019/1/20.
 */

public class IdfMapfinal implements MapFunction<IdfEntity, IdfEntity> {

    private long totaldoucments = 0L;
    private long words;
    public IdfMapfinal(long totaldoucments,long words){
        this.totaldoucments = totaldoucments;
        this.words =words;

    }    @Override
    public IdfEntity map(IdfEntity idfEntity) throws Exception {
        Map<String,Double> tfidfmap = new HashMap<String,Double>();
        String documentid = idfEntity.getDocumentid();
        Map<String,Double> tfmap = idfEntity.getTfmap();
        Set<Map.Entry<String,Double>> set = tfmap.entrySet();
        String tablename = "tfidfdata";
        String rowkey="word";
        String famliyname="baseinfo";
        String colum="idfcount";
        for(Map.Entry<String,Double> entry:set){
            String word = entry.getKey();
            Double value = entry.getValue();


            String data = HbaseUtils.getData(tablename,rowkey,famliyname,colum);
            long viewcount = Long.valueOf(data);
            Double idf = Math.log(totaldoucments/viewcount+1);
            Double tfidf = value*idf;
            tfidfmap.put(word,tfidf);
        }
        LinkedHashMap<String,Double> resultfinal = MapUtils.sortMapByValue(tfidfmap);
        Set<Map.Entry<String,Double>> entryset = resultfinal.entrySet();
        List<String> finalword = new ArrayList<String>();
        int count =1;
        for(Map.Entry<String,Double> mapentry:entryset){
            finalword.add(mapentry.getKey());
            count++;
            if(count>words){
                break;
            }
        }
        IdfEntity idfEntityfinal = new IdfEntity();
        idfEntityfinal.setDocumentid(documentid);
        idfEntityfinal.setFinalword(finalword);
        return idfEntityfinal;
    }
}
