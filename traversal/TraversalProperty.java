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

package grakn.core.traversal;

public abstract class TraversalProperty {

//    public static class Label extends TraversalProperty {
//
//        private final Reference vertex;
//        private final String label, scope;
//
//        public Label(final Reference vertex, final String label, final String scope) {
//            this.vertex = vertex;
//            this.label = label;
//            this.scope = scope;
//            directedTraversals.add(new Label.Lookup());
//            directedTraversals.add(new Label.Filter());
//        }
//
//        public static Label of(final Reference vertex, final String label, final String scope) {
//            return new Label(vertex, label, scope);
//        }
//
//        private class Lookup extends Directed {}
//
//        private class Filter extends Directed {}
//    }
//
//    public static class Abstract extends TraversalProperty {
//
//        private final Reference vertex;
//
//        public Abstract(final Reference vertex) {
//            this.vertex = vertex;
//            directedTraversals.add(new Abstract.Lookup());
//            directedTraversals.add(new Abstract.Filter());
//        }
//
//        public static Abstract of(final Reference vertex) {
//            return new Abstract(vertex);
//        }
//
//        private class Lookup extends Directed {}
//
//        private class Filter extends Directed {}
//    }
//
//    public static class ValueType extends TraversalProperty {
//
//        private final Reference attributeType;
//        private final Encoding.ValueType valueType;
//
//        public ValueType(final Reference attributeType, final Encoding.ValueType valueType) {
//            this.attributeType = attributeType;
//            this.valueType = valueType;
//            directedTraversals.add(new ValueType.Lookup());
//            directedTraversals.add(new ValueType.Filter());
//        }
//
//        public static ValueType of(final Reference attributeType, final GraqlArg.ValueType valueType) {
//            return new ValueType(attributeType, Encoding.ValueType.of(valueType));
//        }
//
//        private class Lookup extends Directed {}
//
//        private class Filter extends Directed {}
//    }
//
//    public static class Regex extends TraversalProperty {
//
//        private final Reference attributeType;
//        private final String regex;
//
//        public Regex(final Reference attributeType, final String regex) {
//            this.attributeType = attributeType;
//            this.regex = regex;
//            directedTraversals.add(new Regex.Lookup());
//            directedTraversals.add(new Regex.Filter());
//        }
//
//        public static Regex of(final Reference attributeType, final String regex) {
//            return new Regex(attributeType, regex);
//        }
//
//        private class Lookup extends Directed {}
//
//        private class Filter extends Directed {}
//    }
//
//    public static class IID extends TraversalProperty {
//
//        private final Reference vertex;
//        private final byte[] iid;
//
//        public IID(final Reference vertex, final byte[] iid) {
//            this.vertex = vertex;
//            this.iid = iid;
//            directedTraversals.add(new IID.Lookup());
//            directedTraversals.add(new IID.Filter());
//        }
//
//        public static IID of(final Reference vertex, final byte[] iid) {
//            return new IID(vertex, iid);
//        }
//
//        private class Lookup extends Directed {}
//
//        private class Filter extends Directed {}
//    }
//
//    public static class Value extends TraversalProperty {
//
//    }
}
