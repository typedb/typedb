/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.concept.Label;
import grakn.core.graql.util.StringUtil;

/**
 * Represents the {@code label} property on a Type.
 * This property can be queried and inserted. If used in an insert query and there is an existing type with the give
 * label, then that type will be retrieved.
 */
public class LabelProperty extends VarProperty {

    private final Label label;

    public LabelProperty(Label label) {
        if (label == null) {
            throw new NullPointerException("Null label");
        }
        this.label = label;
    }

    public Label label() {
        return label;
    }

    @Override
    public String name() {
        return Name.LABEL.toString();
    }

    @Override
    public String property() {
        return StringUtil.typeLabelToString(label());
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof LabelProperty) {
            LabelProperty that = (LabelProperty) o;
            return (this.label.equals(that.label()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.label.hashCode();
        return h;
    }
}
