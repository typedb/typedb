/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner.atom;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.executor.property.PropertyExecutor;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import graql.lang.pattern.Conjunction;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory class for creating Atomic objects.
 */
public class AtomicFactory {

    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public static Stream<Atomic> createAtoms(Conjunction<Statement> pattern, ReasonerQuery parent) {
        //parse all atoms
        Set<Atomic> atoms = pattern.statements().stream()
                .flatMap(statement -> statement.properties().stream()
                        .map(property -> PropertyExecutor
                                .create(statement.var(), property)
                                .atomic(parent, statement, pattern.statements()))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        //Extract variable predicates from attributes and add them to theatom set
        //We need to treat them separately to ensure correctness - different conditions can arise with different
        //orderings of resolvable atoms - hence we need to compare at the end.
        //NB: this creates different vps because the statement context is bound to HasAttributeProperty.
        Set<Atomic> neqs = pattern.statements().stream()
                .flatMap(statement -> statement.getProperties(HasAttributeProperty.class))
                .flatMap(hp -> hp.statements().flatMap(
                        statement -> statement.getProperties(ValueProperty.class)
                                .map(property -> PropertyExecutor
                                        .create(statement.var(), property)
                                        .atomic(parent, statement, pattern.statements()))
                                .filter(Objects::nonNull))
                ).collect(Collectors.toSet());
        atoms.addAll(neqs);

        //remove duplicates
        return atoms.stream()
                .filter(at -> atoms.stream()
                        .filter(Atom.class::isInstance)
                        .map(Atom.class::cast)
                        .flatMap(Atom::getInnerPredicates)
                        .noneMatch(at::equals)
                );
    }

    /**
     * @param parent query context
     * @return (partial) set of predicates corresponding to this answer
     */
    @CheckReturnValue
    public static Set<Atomic> answerToPredicates(ConceptMap answer, ReasonerQuery parent) {
        Set<Variable> varNames = parent.getVarNames();
        return answer.map().entrySet().stream()
                .filter(e -> varNames.contains(e.getKey()))
                .map(e -> IdPredicate.create(e.getKey(), e.getValue().id(), parent))
                .collect(Collectors.toSet());
    }

    /**
     *
     * @param property value property we are interested in
     * @param statement the value property belongs to
     * @param otherStatements other statements providing necessary context
     * @param parent query the VP should be part of
     * @return value predicate corresponding to the provided property
     */
    public static ValuePredicate createValuePredicate(ValueProperty property, Statement statement, Set<Statement> otherStatements,
                                                                             ReasonerQuery parent) {
        HasAttributeProperty has = statement.getProperties(HasAttributeProperty.class).findFirst().orElse(null);
        Variable var = has != null? has.attribute().var() : statement.var();
        ValueProperty.Operation directOperation = property.operation();
        Variable predicateVar = directOperation.innerStatement() != null? directOperation.innerStatement().var() : null;

        boolean partOfAttribute = otherStatements.stream()
                .flatMap(s -> s.getProperties(HasAttributeProperty.class))
                .anyMatch(p -> p.attribute().var().equals(var));
        Set<ValueProperty> parentVPs = otherStatements.stream()
                .flatMap(s -> s.getProperties(ValueProperty.class))
                .filter(vp -> {
                    Statement inner = vp.operation().innerStatement();
                    if (inner == null) return false;
                    return inner.var().equals(var);
                })
                .collect(Collectors.toSet());
        //true if the VP has another VP that references it - a parent VP
        boolean hasParentVp = !parentVPs.isEmpty();
        //if (hasParentVp) return null;
        if (hasParentVp && !partOfAttribute) return null;

        //if predicate variable is bound in another atom, we always need to create a NeqPredicate
        boolean predicateVarBound = otherStatements.stream()
                .flatMap(s -> s.properties().stream())
                .filter(p -> !(p instanceof ValueProperty))
                .flatMap(VarProperty::statements)
                .map(Statement::var)
                .anyMatch(v -> v.equals(predicateVar));
        ValueProperty.Operation indirectOperation = !predicateVarBound?
                ReasonerUtils.findValuePropertyOp(predicateVar, otherStatements) : null;
        ValueProperty.Operation operation;

        if (indirectOperation == null) operation = directOperation;
        else{
            operation = ValueProperty.Operation.Comparison.of(directOperation.comparator(), indirectOperation.value());
        }
        return ValuePredicate.create(var.asReturnedVar(), operation, parent);
    }
}

