package part03email;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 15:52
 * @Author: code1990
 * @Description:
 */
public class EmailEntity {
    private String emailtype;//邮箱类型
    private Long count;//数量
    private String groupfield;//分组字段

    public String getEmailtype() {
        return emailtype;
    }

    public void setEmailtype(String emailtype) {
        this.emailtype = emailtype;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGroupfield() {
        return groupfield;
    }

    public void setGroupfield(String groupfield) {
        this.groupfield = groupfield;
    }
}
