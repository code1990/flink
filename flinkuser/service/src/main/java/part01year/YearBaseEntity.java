package part01year;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 11:07
 * @Author: code1990
 * @Description: 年代标签的开发
 */
public class YearBaseEntity {
    private String yeartype;//年代类型
    private Long count;//数量
    private String groupfield;//分组字段

    public String getGroupfield() {
        return groupfield;
    }

    public void setGroupfield(String groupfield) {
        this.groupfield = groupfield;
    }

    public String getYeartype() {
        return yeartype;
    }

    public void setYeartype(String yeartype) {
        this.yeartype = yeartype;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
