package grakn.core.traversal.property;

import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public abstract class VertexProperty {

    public static class Abstract extends VertexProperty {

        private final int hash;

        public Abstract() {
            this.hash = Objects.hash(getClass());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class IID extends VertexProperty {

        private final Identifier param;
        private final int hash;

        public IID(Identifier param) {
            this.param = param;
            this.hash = Objects.hash(this.param);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IID that = (IID) o;
            return this.param.equals(that.param);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Label extends VertexProperty {

        private final String label, scope;
        private final int hash;

        public Label(String label, @Nullable String scope) {
            this.label = label;
            this.scope = scope;
            this.hash = Objects.hash(this.label, this.scope);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Label that = (Label) o;
            return (this.label.equals(that.label) && Objects.equals(this.scope, that.scope));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Regex extends VertexProperty {

        private final String regex;
        private final int hash;

        public Regex(String regex) {
            this.regex = regex;
            this.hash = Objects.hash(this.regex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Regex that = (Regex) o;
            return this.regex.equals(that.regex);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Type extends VertexProperty {

        private final String[] labels;
        private final int hash;

        public Type(String[] labels) {
            this.labels = labels;
            this.hash = Arrays.hashCode(this.labels);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Type that = (Type) o;
            return Arrays.equals(this.labels, that.labels);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class ValueType extends VertexProperty {

        private final Encoding.ValueType valueType;
        private final int hash;

        public ValueType(Encoding.ValueType valueType) {
            this.valueType = valueType;
            this.hash = Objects.hash(this.valueType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ValueType that = (ValueType) o;
            return this.valueType.equals(that.valueType);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Value extends VertexProperty {

        private final GraqlToken.Comparator comparator;
        private final Identifier param;
        private final int hash;

        public Value(GraqlToken.Comparator comparator, Identifier param) {
            this.comparator = comparator;
            this.param = param;
            this.hash = Objects.hash(this.comparator, this.param);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Value that = (Value) o;
            return (this.comparator.equals(that.comparator) && this.param.equals(that.param));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
