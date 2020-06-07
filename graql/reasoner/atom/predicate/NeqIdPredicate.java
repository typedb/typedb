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

package grakn.core.graql.reasoner.atom.predicate;

import com.google.common.collect.Iterables;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import graql.lang.property.NeqProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.stream.Collectors;

/**
 *
 * <p>
 * Predicate implementation specialising it to be an inequality predicate. Corresponds to graql NeqProperty.
 * </p>
 *
 *
 */

public class NeqIdPredicate extends VariablePredicate {

    private NeqIdPredicate(Variable varName, Variable predicateVar, Statement pattern, ReasonerQuery parentQuery) {
        super(varName, predicateVar, pattern, parentQuery);
    }

    public static NeqIdPredicate create(Statement pattern, ReasonerQuery parent) {
        return new NeqIdPredicate(pattern.var(), extractPredicateVariable(pattern), pattern, parent);
    }
    public static NeqIdPredicate create(Variable varName, NeqProperty prop, ReasonerQuery parent) {
        Statement pattern = new Statement(varName).not(prop);
        return create(pattern, parent);
    }

    private static Variable extractPredicateVariable(Statement pattern) {
        return Iterables.getOnlyElement(pattern.getProperties(NeqProperty.class).collect(Collectors.toSet())).statement().var();
    }

    @Override
    public Atomic copy(ReasonerQuery parent) { return create(this.getPattern(), parent);}

    @Override
    public String toString(){
        IdPredicate idPredicate = this.getIdPredicate(this.getVarName());
        IdPredicate refIdPredicate = this.getIdPredicate(this.getPredicate());
        return "[" + getVarName() + "!=" + getPredicate() + "]" +
                (idPredicate != null? idPredicate : "" ) +
                (refIdPredicate != null? refIdPredicate : "");
    }

    /**
     * @param sub substitution to be checked against the predicate
     * @return true if provided subsitution satisfies the predicate
     */
    public boolean isSatisfied(ConceptMap sub) {
        return !sub.containsVar(getVarName())
                || !sub.containsVar(getPredicate())
                || !sub.get(getVarName()).equals(sub.get(getPredicate()));
    }
}
