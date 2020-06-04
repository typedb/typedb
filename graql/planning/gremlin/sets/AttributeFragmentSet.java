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

package grakn.core.graql.planning.gremlin.sets;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import java.util.Set;

/**
 * Traverse attribute instance ownership edges in either direction
 */
public class AttributeFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable owner;
    private final Variable attribute;
    private final ImmutableSet<Label> attributeTypeLabels;

    AttributeFragmentSet(VarProperty varProperty, Variable owner, Variable attribute, ImmutableSet<Label> attributeTypeLabels){
        super(varProperty);
        this.owner = owner;
        this.attribute = attribute;
        this.attributeTypeLabels = attributeTypeLabels;
    }

    @Override
    public final Set<Fragment> fragments() {
        Variable edge = new Variable();
        return ImmutableSet.of(
                Fragments.inAttribute(varProperty(), attribute, owner, edge, attributeTypeLabels),
                Fragments.outAttribute(varProperty(), owner, attribute, edge, attributeTypeLabels)
        );
    }

}
