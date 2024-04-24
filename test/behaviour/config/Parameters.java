/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.config;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;
import io.cucumber.java.DataTableType;
import io.cucumber.java.ParameterType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNRECOGNISED_ANNOTATION;
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

    @ParameterType("entity|attribute|relation|thing")
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
        return new ScopedLabel(Label.of(labels[1], labels[0]));
    }

    @DataTableType
    public List<ScopedLabel> scoped_labels(List<String> values) {
        Iterator<String> valuesIter = values.iterator();
        String next;
        List<ScopedLabel> scopedLabels = new ArrayList<>();
        while (valuesIter.hasNext() && (next = valuesIter.next()).matches("[a-zA-Z0-9-_]+:[a-zA-Z0-9-_]+")) {
            String[] labels = next.split(":");
            scopedLabels.add(new ScopedLabel(Label.of(labels[1], labels[0])));
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

    @ParameterType("(\\s*([\\w\\-_]+,\\s*)*[\\w\\-_]*\\s*)")
    public List<Annotation> annotations(String stringList) {
        List<String> strings = Arrays.asList(stringList.split(",\\s?"));
        List<Annotation> annotations = new ArrayList<>();
        strings.forEach(string -> {
            Annotation annotation = Annotation.of(string);
            if (annotation == null) throw TypeDBException.of(UNRECOGNISED_ANNOTATION, string);
            else annotations.add(annotation);
        });
        return annotations;
    }

    public enum RootLabel {
        ENTITY("entity"),
        ATTRIBUTE("attribute"),
        RELATION("relation"),
        THING("thing");

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

    // TODO: remove this class, as we now have com.vaticle.typedb.core.common.Label class natively
    public static class ScopedLabel {

        private final Label label;

        public ScopedLabel(Label label) {
            this.label = label;
        }

        public String scope() {
            assert label.scope().isPresent();
            return label.scope().get();
        }

        public String label() {
            return label.name();
        }

        @Override
        public String toString() {
            return label.toString();
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            ScopedLabel that = (ScopedLabel) object;
            return this.label.equals(that.label);
        }

        @Override
        public final int hashCode() {
            return hash(label);
        }
    }
}
