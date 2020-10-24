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

import grakn.core.common.exception.GraknException;
import grakn.core.graph.util.Encoding;
import graql.lang.common.GraqlArg;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Traversal {

    final List<Directed> directedTraversals;

    protected Traversal() {
        directedTraversals = new ArrayList<>();
    }

    public List<Traversal.Directed> directedTraversals() {
        return directedTraversals;
    }

    public boolean isEdge() {
        return false;
    }

    public boolean isProperty() {
        return false;
    }

    public Traversal.Path asEdge() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Path.class)));
    }

    public Traversal.Property asProperty() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Property.class)));
    }

    public abstract static class Directed {}

    public abstract static class Path extends Traversal {

        public boolean isEdge() {
            return true;
        }

        public Path asEdge() {
            return this;
        }

        public static class Is extends Path {

            private final Reference concept1, concept2;

            public Is(final Reference concept1, final Reference concept2) {
                this.concept1 = concept1;
                this.concept2 = concept2;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Is of(final Reference concept1, final Reference concept2) {
                return new Is(concept1, concept2);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Sub extends Path {

            private final Reference thingType, superType;

            public Sub(final Reference thingType, final Reference superType) {
                this.thingType = thingType;
                this.superType = superType;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Sub of(final Reference thingType, final Reference superType) {
                return new Sub(thingType, superType);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Owns extends Path {

            private final Reference thingType, attributeType;

            public Owns(final Reference thingType, final Reference attributeType) {
                this.thingType = thingType;
                this.attributeType = attributeType;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Owns of(final Reference thingType, final Reference attributeType) {
                return new Owns(thingType, attributeType);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Plays extends Path {

            private final Reference thing, role;

            public Plays(final Reference thing, final Reference role) {
                this.thing = thing;
                this.role = role;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Plays of(final Reference thingType, final Reference roleType) {
                return new Plays(thingType, roleType);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Relates extends Path {

            private final Reference relationType, roleType;

            public Relates(final Reference relationType, final Reference roleType) {
                this.relationType = relationType;
                this.roleType = roleType;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Relates of(final Reference relationType, final Reference roleType) {
                return new Relates(relationType, roleType);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Isa extends Path {

            private final Reference thing, type;
            private final boolean isExplicit;

            public Isa(final Reference thing, final Reference type, final boolean isExplicit) {
                this.thing = thing;
                this.type = type;
                this.isExplicit = isExplicit;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Isa of(final Reference thing, final Reference type, final boolean isExplicit) {
                return new Isa(thing, type, isExplicit);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Has extends Path {

            private final Reference owner, attribute;

            Has(final Reference owner, final Reference attribute) {
                this.owner = owner;
                this.attribute = attribute;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Has of(final Reference owner, final Reference attribute) {
                return new Has(owner, attribute);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Playing extends Path {

            private final Reference thing, role;

            public Playing(final Reference thing, final Reference role) {
                this.thing = thing;
                this.role = role;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Playing of(final Reference thing, final Reference role) {
                return new Playing(thing, role);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class Relating extends Path {

            private final Reference relation, role;

            public Relating(final Reference relation, final Reference role) {
                this.relation = relation;
                this.role = role;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static Relating of(final Reference relation, final Reference role) {
                return new Relating(relation, role);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }

        public static class RolePlayer extends Path {

            private final Reference relation, roleType, player;

            public RolePlayer(final Reference relation, @Nullable final Reference roleType, final Reference player) {
                this.roleType = roleType;
                this.relation = relation;
                this.player = player;
                directedTraversals.add(new Out());
                directedTraversals.add(new In());
            }

            public static RolePlayer of(final Reference relation, final Reference player) {
                return of(relation, null, player);
            }

            public static RolePlayer of(final Reference relation, @Nullable final Reference roleType,
                                        final Reference player) {
                return new RolePlayer(relation, roleType, player);
            }

            private class Out extends Directed {}

            private class In extends Directed {}
        }
    }

    public static abstract class Property extends Traversal {

        public boolean isProperty() {
            return true;
        }

        public Traversal.Property asProperty() {
            return this;
        }

        public static class Label extends Property {

            private final Reference vertex;
            private final String label, scope;

            public Label(final Reference vertex, final String label, final String scope) {
                this.vertex = vertex;
                this.label = label;
                this.scope = scope;
                directedTraversals.add(new Lookup());
                directedTraversals.add(new Filter());
            }

            public static Label of(final Reference vertex, final String label, final String scope) {
                return new Label(vertex, label, scope);
            }

            private class Lookup extends Directed {}

            private class Filter extends Directed {}
        }

        public static class Abstract extends Property {

            private final Reference vertex;

            public Abstract(final Reference vertex) {
                this.vertex = vertex;
                directedTraversals.add(new Lookup());
                directedTraversals.add(new Filter());
            }

            public static Abstract of(final Reference vertex) {
                return new Abstract(vertex);
            }

            private class Lookup extends Directed {}

            private class Filter extends Directed {}
        }

        public static class ValueType extends Property {

            private final Reference attributeType;
            private final Encoding.ValueType valueType;

            public ValueType(final Reference attributeType, final Encoding.ValueType valueType) {
                this.attributeType = attributeType;
                this.valueType = valueType;
                directedTraversals.add(new Lookup());
                directedTraversals.add(new Filter());
            }

            public static ValueType of(final Reference attributeType, final GraqlArg.ValueType valueType) {
                return new ValueType(attributeType, Encoding.ValueType.of(valueType));
            }

            private class Lookup extends Directed {}

            private class Filter extends Directed {}
        }

        public static class Regex extends Property {

            private final Reference attributeType;
            private final String regex;

            public Regex(final Reference attributeType, final String regex) {
                this.attributeType = attributeType;
                this.regex = regex;
                directedTraversals.add(new Lookup());
                directedTraversals.add(new Filter());
            }

            public static Regex of(final Reference attributeType, final String regex) {
                return new Regex(attributeType, regex);
            }

            private class Lookup extends Directed {}

            private class Filter extends Directed {}
        }

        public static class IID extends Property {

            private final Reference vertex;
            private final byte[] iid;

            public IID(final Reference vertex, final byte[] iid) {
                this.vertex = vertex;
                this.iid = iid;
                directedTraversals.add(new Lookup());
                directedTraversals.add(new Filter());
            }

            public static IID of(final Reference vertex, final byte[] iid) {
                return new IID(vertex, iid);
            }

            private class Lookup extends Directed {}

            private class Filter extends Directed {}
        }

        public static class Value extends Property {

        }
    }
}
