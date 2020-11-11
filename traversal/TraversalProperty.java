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

import grakn.core.graph.util.Encoding;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;

abstract class TraversalProperty {

    static class Abstract extends TraversalProperty {

        Abstract() {}
    }

    static class IID extends TraversalProperty {

        private final Identifier param;

        IID(Identifier param) {
            this.param = param;
        }
    }

    static class Label extends TraversalProperty {

        private final String label, scope;

        Label(String label, @Nullable String scope) {
            this.label = label;
            this.scope = scope;
        }
    }

    static class Regex extends TraversalProperty {

        private final String regex;

        Regex(String regex) {
            this.regex = regex;
        }
    }

    static class Type extends TraversalProperty {

        private final String[] labels;

        Type(String[] labels) {
            this.labels = labels;
        }
    }

    static class ValueType extends TraversalProperty {

        private final Encoding.ValueType valueType;

        ValueType(Encoding.ValueType valueType) {
            this.valueType = valueType;
        }
    }

    static class Value extends TraversalProperty {

        private final GraqlToken.Comparator comparator;
        private final Identifier param;

        public Value(GraqlToken.Comparator comparator, Identifier param) {
            this.comparator = comparator;
            this.param = param;
        }
    }
}
