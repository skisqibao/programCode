import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Groups {
    private String name;
    private String url;
    private int page;
    private Boolean isNonProfit;
    private Map address;

    private List<LinkBean> list = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public Boolean getNonProfit() {
        return isNonProfit;
    }

    public void setNonProfit(Boolean nonProfit) {
        isNonProfit = nonProfit;
    }

    public Map getAddress() {
        return address;
    }

    public void setAddress(Map address) {
        this.address = address;
    }

    public List<LinkBean> getList() {
        return list;
    }

    public void setList(List<LinkBean> list) {
        this.list = list;
    }
}

