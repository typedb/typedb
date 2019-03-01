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
package grakn.core.graql.reasoner.atom.binary;

import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.unifier.Unifier;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.PlaysProperty;
import graql.lang.property.RelatesProperty;
import graql.lang.property.SubProperty;

import java.util.Set;

/**
 *
 * <p>
 * Atom implementation defining type atoms of the general form:
 *
 * {isa|sub|plays|relates|has}($varName, $predicateVariable)
 *
 * Type atoms correspond to the following respective graql properties:
 * {@link IsaProperty},
 * {@link SubProperty},
 * {@link PlaysProperty}
 * {@link RelatesProperty}
 * {@link HasAttributeTypeProperty}
 * </p>
 *
 *
 */
public abstract class TypeAtom extends Binary{

    @Override
    public boolean isType(){ return true;}

    @Override
    public boolean isRuleApplicableViaAtom(Atom ruleAtom) {
        if (!(ruleAtom instanceof IsaAtom)) return this.isRuleApplicableViaAtom(ruleAtom.toIsaAtom());
        return ruleAtom.isUnifiableWith(this);
    }

    @Override
    public boolean isSelectable() {
        return getTypePredicate() == null
                //disjoint atom
                || !this.getNeighbours(Atom.class).findFirst().isPresent()
                || getPotentialRules().findFirst().isPresent();
    }

    @Override
    public boolean requiresMaterialisation() {
        return isUserDefined() && getSchemaConcept() != null && getSchemaConcept().isRelationType();
    }

    /**
     * @param u unifier to be applied
     * @return set of type atoms resulting from applying the unifier
     */
    public abstract Set<TypeAtom> unify(Unifier u);
}

