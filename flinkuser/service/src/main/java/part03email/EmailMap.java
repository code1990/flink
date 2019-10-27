package part03email;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import util.EmailUtils;
import util.HbaseUtils;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:52
 * @Author: code1990
 * @Description:
 */
public class EmailMap implements MapFunction<String, EmailEntity> {
    @Override
    public EmailEntity map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String email = userinfos[4];

        String emailtype = EmailUtils.getEmailType(email);

        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "baseinfo";
        String colum = "emailinfo";//运营商
        HbaseUtils.putData(tablename,rowkey,famliyname,colum,emailtype);
        EmailEntity emailInfo = new EmailEntity();
        String groupfield = "emailInfo=="+emailtype;
        emailInfo.setEmailtype(emailtype);
        emailInfo.setCount(1L);
        emailInfo.setGroupfield(groupfield);
        return emailInfo;
    }
}