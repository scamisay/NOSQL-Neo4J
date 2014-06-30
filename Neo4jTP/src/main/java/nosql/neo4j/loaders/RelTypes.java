package nosql.neo4j.loaders;

import org.neo4j.graphdb.RelationshipType;

public enum RelTypes implements RelationshipType
{
    HAS_NATION,
    HAS_SUPPLIER,
    PROVIDE,
    BELONGS_TO_PART,
    SUPPLIER_HAS_PARTSUPP,
    HAS_CUSTOMER,
    HAS_ORDER,
    HAS_LINEITEM,
    PARTSUPP_HAS_LINEITEM,
    IS_MADE_OF,
    SUPPLIED_BY
}