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

package grakn.core.graql.internal.reasoner.atom;

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.internal.executor.property.PropertyExecutor;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqValuePredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.statement.Statement;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.predicate.NeqPredicate;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.findPredicateValue;

/**
 * Factory class for creating {@link Atomic} objects.
 */
public class AtomicFactory {

    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public static Stream<Atomic> createAtoms(Conjunction<Statement> pattern, ReasonerQuery parent) {
        Set<Atomic> atoms = pattern.statements().stream()
                .flatMap(statement -> statement.properties().stream()
                        .map(property -> PropertyExecutor.create(statement.var(), property)
                                .atomic(parent, statement, pattern.statements()))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        //extract neqs from attributes
        pattern.statements().stream()
                .flatMap(statement -> statement.getProperties(HasAttributeProperty.class))
                .flatMap(hp -> hp.statements().flatMap(statement ->
                        statement.getProperties(ValueProperty.class)
                                .map(property -> PropertyExecutor.create(statement.var(), property)
                                        .atomic(parent, statement, pattern.statements()))
                                .filter(Objects::nonNull))
                ).forEach(atoms::add);
        return atoms.stream();
    }

    public static Atomic createValuePredicate(ValueProperty property, Statement statement, Set<Statement> otherStatements,
                                              boolean attributeCheck, ReasonerQuery parent) {
        HasAttributeProperty has = statement.getProperties(HasAttributeProperty.class).findFirst().orElse(null);
        Variable var = has != null? has.attribute().var() : statement.var();
        grakn.core.graql.query.predicate.ValuePredicate directPredicate = property.predicate();
        boolean hasPredicateVar = property.predicate().getInnerVar().isPresent();
        Variable predicateVar = hasPredicateVar? property.predicate().getInnerVar().get().var() : null;

        boolean buildNeq = directPredicate instanceof NeqPredicate;
        boolean partOfAttribute = otherStatements.stream()
                .flatMap(s -> s.getProperties(HasAttributeProperty.class))
                .anyMatch(p -> p.attribute().var().equals(var));
        boolean hasParentVp =
                otherStatements.stream()
                        .flatMap(s -> s.getProperties(ValueProperty.class))
                        .map(vp -> vp.predicate().getInnerVar())
                        .flatMap(CommonUtil::optionalToStream)
                        .map(Statement::var)
                        .anyMatch(pVar -> pVar.equals(var));
        if (hasParentVp) return null;
        if (attributeCheck && (partOfAttribute && !buildNeq)) return null;

        grakn.core.graql.query.predicate.ValuePredicate indirectPredicate = findPredicateValue(predicateVar, otherStatements);
        grakn.core.graql.query.predicate.ValuePredicate predicate = indirectPredicate != null? indirectPredicate : directPredicate;
        Object value = predicate.value().orElse(null);
        return buildNeq?
                NeqValuePredicate.create(var.asUserDefined(), predicateVar, value, parent) :
                ValuePredicate.create(var.asUserDefined(), predicate, parent);
    }
}

