package part10usertype;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 7:32
 * @Author: code1990
 * @Description:
 */
public class UseTypeEntity {
    private String usetype;
    private long count;
    private String groupbyfield;

    public String getUsetype() {
        return usetype;
    }

    public void setUsetype(String usetype) {
        this.usetype = usetype;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getGroupbyfield() {
        return groupbyfield;
    }

    public void setGroupbyfield(String groupbyfield) {
        this.groupbyfield = groupbyfield;
    }
}
