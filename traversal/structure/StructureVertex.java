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

package grakn.core.traversal.structure;

import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

public class StructureVertex {

    public final Set<StructureEdge> outgoing;
    public final Set<StructureEdge> incoming;
    private final Structure structure;
    private final Identifier identifier;
    private final Set<Property> properties;

    StructureVertex(Identifier identifier, Structure structure) {
        this.identifier = identifier;
        this.properties = new HashSet<>();
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
        this.structure = structure;
    }

    void out(StructureEdge edge) {
        outgoing.add(edge);
    }

    void in(StructureEdge edge) {
        incoming.add(edge);
    }

    Set<StructureEdge> outs() {
        return outgoing;
    }

    Set<StructureEdge> ins() {
        return incoming;
    }

    Identifier identifier() {
        return identifier;
    }

    public void property(Property property) {
        properties.add(property);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StructureVertex that = (StructureVertex) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    public abstract static class Property {

        public static class Abstract extends Property {

            public Abstract() {}
        }

        public static class IID extends Property {

            private final Identifier param;

            public IID(Identifier param) {
                this.param = param;
            }
        }

        public static class Label extends Property {

            private final String label, scope;

            public Label(String label, @Nullable String scope) {
                this.label = label;
                this.scope = scope;
            }
        }

        public static class Regex extends Property {

            private final String regex;

            public Regex(String regex) {
                this.regex = regex;
            }
        }

        public static class Type extends Property {

            private final String[] labels;

            public Type(String[] labels) {
                this.labels = labels;
            }
        }

        public static class ValueType extends Property {

            private final Encoding.ValueType valueType;

            public ValueType(Encoding.ValueType valueType) {
                this.valueType = valueType;
            }
        }

        public static class Value extends Property {

            private final GraqlToken.Comparator comparator;
            private final Identifier param;

            public Value(GraqlToken.Comparator comparator, Identifier param) {
                this.comparator = comparator;
                this.param = param;
            }
        }
    }
}
