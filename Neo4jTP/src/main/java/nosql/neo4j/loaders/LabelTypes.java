package nosql.neo4j.loaders;

/**
 * Created by scamisay on 6/29/14.
 */
public enum LabelTypes {
    Region(0,"Region"),
    Nation(1,"Nation"),
    Supplier(2,"Supplier"),
    Part(3,"Part"),
    Customer(4,"Customer"),
    Order(5,"Order"),
    LineItem(6,"LineItem");

    private String desc;
    private Integer id;

    LabelTypes(){
        this.id = -1;
        this.desc = "";
    }

    LabelTypes(Integer id,String name){
        this.id = id;
        this.desc = name;
    }

    public String getDescription(){
        return this.desc;
    }

    public Integer getId() {
        return this.id;
    }

    public void setName(String name) {
        this.desc = name;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
