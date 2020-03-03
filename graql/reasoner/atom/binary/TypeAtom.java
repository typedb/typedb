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
package grakn.core.graql.reasoner.atom.binary;

import grakn.core.graql.reasoner.ReasoningContext;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 * <p>
 * Atom implementation defining type atoms of the general form:
 *
 * {isa|sub|plays|relates|has}($varName, $predicateVariable)
 *
 * Type atoms correspond to the following respective graql properties:
 * IsaProperty,
 * SubProperty,
 * PlaysProperty
 * RelatesProperty
 * HasAttributeTypeProperty
 * </p>
 *
 *
 */
public abstract class TypeAtom extends Binary {

    TypeAtom(Variable varName, Statement pattern, ReasonerQuery reasonerQuery, @Nullable Label label,
             Variable predicateVariable, ReasoningContext ctx) {
        super(varName, pattern, reasonerQuery, label, predicateVariable, ctx);
    }

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
        if (!isUserDefined()) return false;
        SchemaConcept type = getSchemaConcept();
        return isUserDefined() && type != null && type.isRelationType();
    }

    /**
     * @param u unifier to be applied
     * @return set of type atoms resulting from applying the unifier
     */
    public abstract Set<TypeAtom> unify(Unifier u);
}

