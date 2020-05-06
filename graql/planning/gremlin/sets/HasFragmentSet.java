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

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.neq;
import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.rolePlayer;

public class HasFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable owner;
    private final Variable attribute;
    private final ImmutableSet<Label> attributeTypeLabels;

    HasFragmentSet(VarProperty varProperty, Variable owner, Variable attribute, ImmutableSet<Label> attributeTypeLabels){
        super(varProperty);
        this.owner = owner;
        this.attribute = attribute;
        this.attributeTypeLabels = attributeTypeLabels;
    }

    @Override
    public final Set<Fragment> fragments() {
//        rolePlayer(property, property.relation().var(), edge1, var, null,
//                ImmutableSet.of(hasOwnerRole, keyOwnerRole), ImmutableSet.of(has, key)),
//                //value rolePlayer edge
//                rolePlayer(property, property.relation().var(), edge2, property.attribute().var(), null,
//                        ImmutableSet.of(hasValueRole, keyValueRole), ImmutableSet.of(has, key)),
//                neq(property, edge1, edge2)

        //        Label has = Schema.ImplicitType.HAS.getLabel(type);
        //        Label key = Schema.ImplicitType.KEY.getLabel(type);
        //
        //        Label hasOwnerRole = Schema.ImplicitType.HAS_OWNER.getLabel(type);
        //        Label keyOwnerRole = Schema.ImplicitType.KEY_OWNER.getLabel(type);
        //        Label hasValueRole = Schema.ImplicitType.HAS_VALUE.getLabel(type);
        //        Label keyValueRole = Schema.ImplicitType.KEY_VALUE.getLabel(type);
        //
        //        Variable edge1 = new Variable();
        //        Variable edge2 = new Variable();

        return ImmutableSet.of(
                Fragments.inHas(varProperty(), attribute, owner, attributeTypeLabels),
                Fragments.outHas(varProperty(), owner, attribute, attributeTypeLabels)
        );
    }

}
