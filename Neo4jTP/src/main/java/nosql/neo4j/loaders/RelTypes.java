package nosql.neo4j.loaders;

import org.neo4j.graphdb.RelationshipType;

public enum RelTypes implements RelationshipType
{
    // Region - Nation
    BELONGS_TO_REGION,
    HAS_NATION,
    // Nation - Supplier
    BELONGS_TO_NATION,
    HAS_SUPPLIER,
    // Part - PartSupp
    BELONGS_TO_PART,
    PART_HAS_PARTSUPP,
    // Supplier - PartSupp
    BELONGS_TO_SUPPLIER,
    SUPPLIER_HAS_PARTSUPP,
    
    //Supplier - Part
    PROVIDE,
    PROVIDED_BY,
    
    // Nation - Customer
    HAS_CUSTOMER,
    // Customer - Order
    HAS_ORDER,
    BELONGS_TO_CUSTOMER,
    // Order - Lineitem
    HAS_LINEITEM,
    BELONGS_TO_ORDER,
    // PartSupp - Lineitem
    PARTSUPP_HAS_LINEITEM
}