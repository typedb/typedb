/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.answer;


import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.value.Value;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.concept.value.Value.DATE_TIME_FORMATTER_MILLIS;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_LABEL;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_ROOT;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_TYPE;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_VALUE;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_VALUE_TYPE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ATTRIBUTE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ENTITY;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.RELATION;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ROLE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.THING;

public class ReadableConceptTree {

    private final Node.Map root;

    public ReadableConceptTree(Node.Map root) {
        this.root = root;
    }

    public Node.Map root() {
        return root;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReadableConceptTree that = (ReadableConceptTree) o;
        return root.equals(that.root);
    }

    @Override
    public int hashCode() {
        return Objects.hash(root);
    }

    @Override
    public String toString() {
        return root.toJSON();
    }

    public String toJSON() {
        return root.toJSON();
    }

    public interface Node {

        static String quote(String string) {
            return "\"" + string + "\"";
        }

        String toJSON();

        default boolean isMap() {
            return false;
        }

        default Map asMap() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Map.class));
        }

        default boolean isList() {
            return false;
        }

        default List asList() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(List.class));
        }

        default boolean isLeaf() {
            return false;
        }

        default Leaf<?> asLeaf() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Leaf.class));
        }

        class Map implements Node {

            private static final String KEY_VALUE_SEPARATOR = ":";
            private static final String ENTRY_SEPARATOR = ",";
            private static final String CURLY_LEFT = "{";
            private static final String CURLY_RIGHT = "}";

            private final java.util.Map<String, Node> map;

            public Map() {
                this.map = new HashMap<>();
            }

            public void add(String key, Node value) {
                assert !map.containsKey(key);
                map.put(key, value);
            }

            public java.util.Map<String, Node> map() {
                return map;
            }

            @Override
            public boolean isMap() {
                return true;
            }

            @Override
            public Map asMap() {
                return this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Map map = (Map) o;
                return this.map.equals(map.map);
            }

            @Override
            public int hashCode() {
                return map.hashCode();
            }

            @Override
            public String toString() {
                return toJSON();
            }

            @Override
            public String toJSON() {
                return CURLY_LEFT + "\n" +
                        map.entrySet().stream()
                                .map(e -> quote(e.getKey()) + KEY_VALUE_SEPARATOR + e.getValue().toJSON())
                                .collect(Collectors.joining(ENTRY_SEPARATOR + "\n", "", "\n")) +
                        CURLY_RIGHT;
            }
        }

        class List implements Node {

            private static final String ELEMENT_SEPARATOR = ",";
            private static final String SQUARE_LEFT = "[";
            private static final String SQUARE_RIGHT = "]";

            private final java.util.List<? extends Node> list;

            public List(java.util.List<? extends Node> list) {
                this.list = list;
            }

            public java.util.List<? extends Node> list() {
                return list;
            }

            @Override
            public boolean isList() {
                return true;
            }

            @Override
            public List asList() {
                return this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                List list = (List) o;
                return this.list.equals(list.list);
            }

            @Override
            public int hashCode() {
                return list.hashCode();
            }

            @Override
            public String toString() {
                return toJSON();
            }

            @Override
            public String toJSON() {
                return SQUARE_LEFT + "\n" +
                        list.stream().map(Object::toString)
                                .collect(Collectors.joining(ELEMENT_SEPARATOR + "\n", "", "\n")) +
                        SQUARE_RIGHT;
            }
        }

        class Leaf<T extends Concept.Readable> implements Node {

            private final T readableConcept;

            public Leaf(@Nullable T readableConcept) {
                this.readableConcept = readableConcept;
            }

            public T readableConcept() {
                return readableConcept;
            }

            @Override
            public boolean isLeaf() {
                return true;
            }

            @Override
            public Leaf<?> asLeaf() {
                return this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Leaf<?> leaf = (Leaf<?>) o;
                return Objects.equals(readableConcept, leaf.readableConcept);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(readableConcept);
            }

            @Override
            public String toString() {
                return toJSON();
            }

            @Override
            public String toJSON() {
                if (readableConcept == null) return "null";
                else if (readableConcept.isType()) return getType(readableConcept.asType());
                else if (readableConcept.isAttribute()) return getAttribute(readableConcept.asAttribute());
                else if (readableConcept.isValue()) return getValue(readableConcept.asValue());
                else throw TypeDBException.of(ILLEGAL_STATE);
            }

            private static String getType(Type type) {
                String buf = Map.CURLY_LEFT + "\n" +
                        quote(KEY_LABEL) + Map.KEY_VALUE_SEPARATOR + quote(type.getLabel().scopedName()) + Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_ROOT) + Map.KEY_VALUE_SEPARATOR + quote(getRoot(type));
                if (type.isAttributeType()) {
                    buf += Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_VALUE_TYPE) + Map.KEY_VALUE_SEPARATOR + quote(type.asAttributeType().getValueType().encoding().typeQLValueType().toString());
                }
                return buf + "\n" + Map.CURLY_RIGHT;
            }

            private static String getRoot(Type type) {
                if (type.isRoot()) return THING.properLabel().scopedName();
                else if (type.isEntityType()) return ENTITY.properLabel().scopedName();
                else if (type.isRelationType()) return RELATION.properLabel().scopedName();
                else if (type.isAttributeType()) return ATTRIBUTE.properLabel().scopedName();
                else if (type.isRoleType()) return ROLE.properLabel().scopedName();
                else throw TypeDBException.of(ILLEGAL_STATE);
            }

            private static String getAttribute(Attribute attribute) {
                String valueString;
                if (attribute.isBoolean()) valueString = attribute.asBoolean().getValue().toString();
                else if (attribute.isLong()) valueString = attribute.asLong().getValue().toString();
                else if (attribute.isDouble()) valueString = attribute.asDouble().getValue().toString();
                else if (attribute.isString()) valueString = quote(attribute.asString().getValue());
                else if (attribute.isDateTime()) {
                    valueString = quote(DATE_TIME_FORMATTER_MILLIS.format(attribute.asDateTime().getValue()));
                }
                else throw TypeDBException.of(ILLEGAL_STATE);
                return Map.CURLY_LEFT + "\n" +
                        quote(KEY_TYPE) + Map.KEY_VALUE_SEPARATOR + getType(attribute.getType()) + Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_VALUE) + Map.KEY_VALUE_SEPARATOR + valueString + "\n" +
                        Map.CURLY_RIGHT;
            }

            private static String getValue(Value<?> value) {
                String valueString;
                if (value.isBoolean()) valueString = value.asBoolean().value().toString();
                else if (value.isLong()) valueString = value.asLong().value().toString();
                else if (value.isDouble()) valueString = value.asDouble().value().toString();
                else if (value.isString()) valueString = quote(value.asString().value());
                else if (value.isDateTime()) {
                    valueString = quote(DATE_TIME_FORMATTER_MILLIS.format(value.asDateTime().value()));
                } else throw TypeDBException.of(ILLEGAL_STATE);
                return Map.CURLY_LEFT + "\n" +
                        quote(KEY_VALUE) + Map.KEY_VALUE_SEPARATOR + valueString + Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_VALUE_TYPE) + Map.KEY_VALUE_SEPARATOR + quote(value.valueType().typeQLValueType().toString()) + "\n" +
                        Map.CURLY_RIGHT;
            }
        }
    }
}
