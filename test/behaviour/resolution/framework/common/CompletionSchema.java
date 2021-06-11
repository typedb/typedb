package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

public class CompletionSchema {

    public enum CompletionSchemaType {
        // Relations
        VAR_PROPERTY("var-property"),
        ISA_PROPERTY("isa-property"),
        HAS_ATTRIBUTE_PROPERTY("has-attribute-property"),
        RELATION_PROPERTY("relation-property"),
        RESOLUTION("resolution"),

        // Attributes
        LABEL("label"),
        RULE_LABEL("rule-label"),
        TYPE_LABEL("type-label"),
        ROLE_LABEL("role-label"),
        INFERRED("inferred");

        private final String name;

        CompletionSchemaType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public enum CompletionSchemaRole {
        INSTANCE("instance"),
        OWNER("owner"),
        OWNED("owned"),
        ROLEPLAYER("roleplayer"),
        REL("rel"),
        BODY("body"),
        HEAD("head");

        private final String label;

        CompletionSchemaRole(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }

        @Override
        public String toString() {
            return label;
        }
    }
}
