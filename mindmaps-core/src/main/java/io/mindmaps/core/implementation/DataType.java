package io.mindmaps.core.implementation;

/**
 * A type enum which restricts the types of links/concepts which can be created
 */
public final class DataType {
    private DataType(){
        throw new UnsupportedOperationException();
    }

    /**
     * The different types of edges between vertices
     */
    public enum EdgeLabel {
        ISA("isa"),
        AKO("ako"),
        HAS_ROLE("has-role"),
        PLAYS_ROLE("plays-role"),
        HAS_SCOPE("has-scope"),
        CASTING("casting"),
        ROLE_PLAYER("role-player"),
        HYPOTHESIS("hypothesis"),
        CONCLUSION("conclusion"),
        SHORTCUT("shortcut");

        private final String label;

        EdgeLabel(String l){
            label = l;
        }

        public String getLabel(){
            return label;
        }

        public static EdgeLabel getEdgeLabel(String label){
            if(ISA.getLabel().equals(label))
                return ISA;
            if(AKO.getLabel().equals(label))
                return AKO;
            if(HAS_ROLE.getLabel().equals(label))
                return HAS_ROLE;
            if(PLAYS_ROLE.getLabel().equals(label))
                return PLAYS_ROLE;
            if(HAS_SCOPE.getLabel().equals(label))
                return HAS_SCOPE;
            if(CASTING.getLabel().equals(label))
                return CASTING;
            if(ROLE_PLAYER.getLabel().equals(label))
                return ROLE_PLAYER;
            if(HYPOTHESIS.getLabel().equals(label))
                return HYPOTHESIS;
            if(CONCLUSION.getLabel().equals(label))
                return CONCLUSION;
            if(SHORTCUT.getLabel().equals(label))
                return SHORTCUT;
            return null;
        }
    }

    public enum ConceptMeta {
        TYPE("type"),
        ENTITY_TYPE("entity-type"),
        ROLE_TYPE("role-type"),
        RESOURCE_TYPE("resource-type"),
        RELATION_TYPE("relation-type"),
        RULE_TYPE("rule-type"),
        INFERENCE_RULE("inference-rule"),
        CONSTRAINT_RULE("constraint-rule");


        private final String id;
        ConceptMeta(String i){
            id = i;
        }

        public String getId(){
            return id;
        }

        public static boolean isMetaId(String id){
            for (ConceptMeta conceptMeta : ConceptMeta.values()) {
                if(conceptMeta.getId().equals(id))
                    return true;
            }
            return false;
        }
    }

    /**
     * Base Types reflecting the possible objects in the model
     */
    public enum BaseType{
        RELATION, CASTING, TYPE, ROLE_TYPE, RELATION_TYPE, RESOURCE_TYPE, ENTITY, RESOURCE, RULE, RULE_TYPE, ENTITY_TYPE
    }

    /**
     * A property enum defining the unique mutable properties of the concept. The must be unique properties.
     */
    public enum ConceptPropertyUnique {
        ITEM_IDENTIFIER, SUBJECT_IDENTIFIER, INDEX
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum ConceptProperty {
        TYPE, IS_ABSTRACT,
        REGEX, DATA_TYPE, IS_UNIQUE,
        IS_MATERIALISED, IS_EXPECTED, RULE_LHS, RULE_RHS,
        VALUE_STRING, VALUE_LONG, VALUE_DOUBLE, VALUE_BOOLEAN
    }

    /**
     * A property enum defining the possible labels that can go on the edge label.
     */
    public enum EdgeProperty {
        ROLE_TYPE,
        RELATION_ID,
        TO_ID,
        TO_ROLE,
        TO_TYPE,
        FROM_ID,
        FROM_ROLE,
        FROM_TYPE,
        ASSERTION_BASE_ID,
        VALUE,
        SHORTCUT_HASH
    }
}
