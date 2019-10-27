package util;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:51
 * @Author: code1990
 * @Description:
 */
public class EmailUtils {

    public static String getEmailType(String email) {
        String emailtye = "其他邮箱用户";
        if (email.contains("@163.com") || email.contains("@126.com")) {
            emailtye = "网易邮箱用户";
        } else if (email.contains("@139.com")) {
            emailtye = "移动邮箱用户";
        } else if (email.contains("@sohu.com")) {
            emailtye = "搜狐邮箱用户";
        } else if (email.contains("@qq.com")) {
            emailtye = "qq邮箱用户";
        } else if (email.contains("@189.cn")) {
            emailtye = "189邮箱用户";
        } else if (email.contains("@tom.com")) {
            emailtye = "tom邮箱用户";
        } else if (email.contains("@aliyun.com")) {
            emailtye = "阿里邮箱用户";
        } else if (email.contains("@sina.com")) {
            emailtye = "新浪邮箱用户";
        }
        return emailtye;
    }
}
