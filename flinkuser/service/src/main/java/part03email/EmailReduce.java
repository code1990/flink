package part03email;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:52
 * @Author: code1990
 * @Description:
 */
public class EmailReduce implements ReduceFunction<EmailEntity> {

    @Override
    public EmailEntity reduce(EmailEntity emaiInfo, EmailEntity t1) throws Exception {
        String emailtype = emaiInfo.getEmailtype();
        Long count1 = emaiInfo.getCount();

        Long count2 = t1.getCount();

        EmailEntity emaiInfofinal = new EmailEntity();
        emaiInfofinal.setEmailtype(emailtype);
        emaiInfofinal.setCount(count1 + count2);

        return emaiInfofinal;
    }
}
