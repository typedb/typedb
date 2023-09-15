/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.concept.answer;


import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.concept.value.Value;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_LABEL;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_ROOT;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_TYPE;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_VALUE;
import static com.vaticle.typedb.core.concept.Concept.Readable.KEY_VALUE_TYPE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ATTRIBUTE;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ENTITY;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.RELATION;
import static com.vaticle.typedb.core.encoding.Encoding.Vertex.Type.Root.ROLE;

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

        class Map implements Node {

            private static final String KEY_VALUE_SEPARATOR = ":";
            private static final String ENTRY_SEPARATOR = ",";
            private static final String CURLY_LEFT = "{";
            private static final String CURLY_RIGHT = "}";

            private final java.util.Map<String, Node> entries;

            public Map() {
                this.entries = new HashMap<>();
            }

            public void add(String key, Node value) {
                assert !entries.containsKey(key);
                entries.put(key, value);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Map map = (Map) o;
                return entries.equals(map.entries);
            }

            @Override
            public int hashCode() {
                return entries.hashCode();
            }

            @Override
            public String toString() {
                return toJSON();
            }

            @Override
            public String toJSON() {
                return CURLY_LEFT + "\n" +
                        entries.entrySet().stream()
                                .map(e -> quote(e.getKey()) + KEY_VALUE_SEPARATOR + e.getValue().toString())
                                .collect(Collectors.joining(ENTRY_SEPARATOR + "\n", "", "\n")) +
                        CURLY_RIGHT;
            }
        }

        class List implements Node {

            private static final String ELEMENT_SEPARATOR = ",";
            private static final String SQUARE_LEFT = "[";
            private static final String SQUARE_RIGHT = "]";

            private final java.util.List<? extends Node> nodes;

            public List(java.util.List<? extends Node> nodes) {
                this.nodes = nodes;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                List list = (List) o;
                return nodes.equals(list.nodes);
            }

            @Override
            public int hashCode() {
                return nodes.hashCode();
            }

            @Override
            public String toString() {
                return toJSON();
            }

            @Override
            public String toJSON() {
                return SQUARE_LEFT + "\n" +
                        nodes.stream().map(Object::toString)
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
                if (readableConcept == null) return "";
                else if (readableConcept.isType()) return getType(readableConcept.asType());
                else if (readableConcept.isAttribute()) return getAttribute(readableConcept.asAttribute());
                else if (readableConcept.isValue()) return getValue(readableConcept.asValue());
                else throw TypeDBException.of(ILLEGAL_STATE);
            }

            private static String getType(Type type) {
                return Map.CURLY_LEFT + "\n" +
                        quote(KEY_LABEL) + Map.KEY_VALUE_SEPARATOR + quote(type.getLabel().scopedName()) + Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_ROOT) + Map.KEY_VALUE_SEPARATOR + quote(getRoot(type)) + "\n" +
                        Map.CURLY_RIGHT;
            }

            private static String getRoot(Type type) {
                if (type.isEntityType()) return ENTITY.properLabel().scopedName();
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
                else if (attribute.isDateTime()) valueString = quote(attribute.asDateTime().getValue().toString());
                else throw TypeDBException.of(ILLEGAL_STATE);
                return Map.CURLY_LEFT + "\n" +
                        quote(KEY_TYPE) + Map.KEY_VALUE_SEPARATOR + getType(attribute.getType()) + Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_VALUE) + Map.KEY_VALUE_SEPARATOR + valueString +  Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_VALUE_TYPE) + Map.KEY_VALUE_SEPARATOR + quote(attribute.getType().getValueType().encoding().typeQLValueType().toString()) + "\n" +
                        Map.CURLY_RIGHT;
            }

            private static String getValue(Value<?> value) {
                String valueString;
                if (value.isBoolean()) valueString = value.asBoolean().value().toString();
                else if (value.isLong()) valueString = value.asLong().value().toString();
                else if (value.isDouble()) valueString = value.asDouble().value().toString();
                else if (value.isString()) valueString = quote(value.asString().value());
                else if (value.isDateTime()) valueString = quote(value.asDateTime().value().toString());
                else throw TypeDBException.of(ILLEGAL_STATE);
                return Map.CURLY_LEFT + "\n" +
                        quote(KEY_VALUE) + Map.KEY_VALUE_SEPARATOR + valueString + Map.ENTRY_SEPARATOR + "\n" +
                        quote(KEY_VALUE_TYPE) + Map.KEY_VALUE_SEPARATOR + quote(value.valueType().typeQLValueType().toString()) + "\n" +
                        Map.CURLY_RIGHT;
            }
        }
    }
}
