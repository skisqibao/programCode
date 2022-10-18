public class Product {
    public Long id;

    public String city;

    public Integer onlineUserNum;


    public Product(Long id, String city, Integer onlineUserNum) {
        this.id = id;
        this.city = city;
        this.onlineUserNum = onlineUserNum;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Integer getOnlineUserNum() {
        return onlineUserNum;
    }

    public void setOnlineUserNum(Integer onlineUserNum) {
        this.onlineUserNum = onlineUserNum;
    }

    @Override
    public String toString() {
        return "Product{" +
                "id=" + id +
                ", city='" + city + '\'' +
                ", onlineUserNum=" + onlineUserNum +
                '}';
    }
}
