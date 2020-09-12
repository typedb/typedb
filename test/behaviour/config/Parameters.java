/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.test.behaviour.config;

import grakn.core.common.parameters.Arguments;
import grakn.core.concept.type.AttributeType;
import io.cucumber.java.DataTableType;
import io.cucumber.java.ParameterType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.hash;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class Parameters {

    @ParameterType("true|false")
    public Boolean bool(String bool) {
        return Boolean.parseBoolean(bool);
    }

    @ParameterType("[0-9]+")
    public Integer number(String number) {
        return Integer.parseInt(number);
    }

    @ParameterType("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d")
    public LocalDateTime datetime(String dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.parse(dateTime, formatter);
    }

    @ParameterType("entity|attribute|relation")
    public RootLabel root_label(String type) {
        return RootLabel.of(type);
    }

    @ParameterType("[a-zA-Z0-9-_]+")
    public String type_label(String typeLabel) {
        return typeLabel;
    }

    @ParameterType("[a-zA-Z0-9-_]+:[a-zA-Z0-9-_]+")
    public ScopedLabel scoped_label(String roleLabel) {
        String[] labels = roleLabel.split(":");
        return new ScopedLabel(labels[0], labels[1]);
    }

    @DataTableType
    public List<ScopedLabel> scoped_labels(List<String> values) {
        Iterator<String> valuesIter = values.iterator();
        String next;
        List<ScopedLabel> scopedLabels = new ArrayList<>();
        while (valuesIter.hasNext() && (next = valuesIter.next()).matches("[a-zA-Z0-9-_]+:[a-zA-Z0-9-_]+")) {
            String[] labels = next.split(":");
            scopedLabels.add(new ScopedLabel(labels[0], labels[1]));
        }

        if (valuesIter.hasNext()) fail("Values do not match Scoped Labels regular expression");
        return scopedLabels;
    }

    @ParameterType("long|double|string|boolean|datetime")
    public AttributeType.ValueType value_type(String type) {
        switch (type) {
            case "long":
                return AttributeType.ValueType.LONG;
            case "double":
                return AttributeType.ValueType.DOUBLE;
            case "string":
                return AttributeType.ValueType.STRING;
            case "boolean":
                return AttributeType.ValueType.BOOLEAN;
            case "datetime":
                return AttributeType.ValueType.DATETIME;
            default:
                return null;
        }
    }

    @ParameterType("\\$([a-zA-Z0-9]+)")
    public String var(String variable) {
        return variable;
    }

    @ParameterType("read|write")
    public Arguments.Transaction.Type transaction_type(String type) {
        if (type.equals("read")) {
            return Arguments.Transaction.Type.READ;
        } else if (type.equals("write")) {
            return Arguments.Transaction.Type.WRITE;
        }
        return null;
    }

    @DataTableType
    public List<Arguments.Transaction.Type> transaction_types(List<String> values) {
        List<Arguments.Transaction.Type> typeList = new ArrayList<>();
        for (String value : values) {
            Arguments.Transaction.Type type = transaction_type(value);
            assertNotNull(type);
            typeList.add(type);
        }

        return typeList;
    }

    public enum RootLabel {
        ENTITY("entity"),
        ATTRIBUTE("attribute"),
        RELATION("relation");

        private final String label;

        RootLabel(String label) {
            this.label = label;
        }

        public static RootLabel of(String label) {
            for (RootLabel t : RootLabel.values()) {
                if (t.label.equals(label)) {
                    return t;
                }
            }
            return null;
        }

        public String label() {
            return label;
        }
    }

    public static class ScopedLabel {
        private final String scope;
        private final String role;

        public ScopedLabel(String scope, String role) {
            this.scope = scope;
            this.role = role;
        }

        public String scope() {
            return scope;
        }

        public String role() {
            return role;
        }

        @Override
        public String toString() {
            return scope + ":" + role;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            ScopedLabel that = (ScopedLabel) object;
            return (this.scope.equals(that.scope) &&
                    this.role.equals(that.role));
        }

        @Override
        public final int hashCode() {
            return hash(scope, role);
        }
    }
}
