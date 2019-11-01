package part09brandlike;

/**
 * @program: flinkuser
 * @Date: 2019-10-31 23:11
 * @Author: code1990
 * @Description:
 */
public class BrandLikeEntity {
    private String brand;
    private long count;
    private String groupbyfield;

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
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
