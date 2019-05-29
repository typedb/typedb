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

package grakn.core.graql.reasoner.atom;

import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.executor.property.PropertyExecutor;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.NeqValuePredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.ValueProperty;
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
                        .map(property -> PropertyExecutor.create(statement.var(), property)
                                .atomic(parent, statement, pattern.statements()))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        //extract neqs from attributes and add them to atom set - we need to treat them separately to ensure correctness
        //this creates different vps because the statement context is bound to HasAttributeProperty
        Set<Atomic> neqs = pattern.statements().stream()
                .flatMap(statement -> statement.getProperties(HasAttributeProperty.class))
                .flatMap(hp -> hp.statements().flatMap(
                        statement -> statement.getProperties(ValueProperty.class)
                                .map(property -> PropertyExecutor.create(statement.var(), property)
                                        .atomic(parent, statement, pattern.statements()))
                                .filter(Objects::nonNull))
                ).collect(Collectors.toSet());

        if (!atoms.containsAll(neqs)) {
            System.out.println();
        }
        boolean changed = atoms.addAll(neqs);


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
     * @param allowNeq allow to produce a NeqPredicate if required
     * @param discardIfInAttribute if true we discard the VP if it's a part of an attribute and doesn't have an inequality
     * @param parent query the VP should be part of
     * @return value predicate corresponding to the provided property
     */
    public static Atomic createValuePredicate(ValueProperty property, Statement statement, Set<Statement> otherStatements,
                                                                              boolean allowNeq, boolean discardIfInAttribute, ReasonerQuery parent) {
        HasAttributeProperty has = statement.getProperties(HasAttributeProperty.class).findFirst().orElse(null);
        Variable var = has != null? has.attribute().var() : statement.var();
        ValueProperty.Operation directOperation = property.operation();
        Variable predicateVar = directOperation.innerStatement() != null? directOperation.innerStatement().var() : null;

        boolean hasNeq = directOperation.comparator().equals(Graql.Token.Comparator.NEQV);
        boolean partOfAttribute = otherStatements.stream()
                .flatMap(s -> s.getProperties(HasAttributeProperty.class))
                .anyMatch(p -> p.attribute().var().equals(var));
        //true if the VP has another VP that references it - a parent VP
        Set<ValueProperty> parentVPs = otherStatements.stream()
                .flatMap(s -> s.getProperties(ValueProperty.class))
                .filter(vp -> {
                    Statement inner = vp.operation().innerStatement();
                    if (inner == null) return false;
                    return inner.var().equals(var);
                })
                .collect(Collectors.toSet());
        boolean hasParentVp = !parentVPs.isEmpty();
        if (hasParentVp) return null;
        if (discardIfInAttribute && (partOfAttribute && !hasNeq)) return null;

        ValueProperty.Operation indirectOperation = ReasonerUtils.findValuePropertyOp(predicateVar, otherStatements);
        ValueProperty.Operation operation = indirectOperation != null? indirectOperation : directOperation;
        Object value = operation.innerStatement() == null? operation.value() : null;

        NeqValuePredicate oldNeq = NeqValuePredicate.create(var.asReturnedVar(), predicateVar, value, parent);
        NeqValuePredicate newNeq = NeqValuePredicate.create(var.asReturnedVar(), operation, parent);
        if (!newNeq.equals(oldNeq)){
            System.out.println();
        }
        return hasNeq?
                (allowNeq?
                        NeqValuePredicate.create(var.asReturnedVar(), predicateVar, value, parent) :
                        //NeqValuePredicate.create(var.asReturnedVar(), operation, parent) :
                        ValuePredicate.neq(var.asReturnedVar(), predicateVar, value, parent)) :
                ValuePredicate.create(var.asReturnedVar(), operation, parent);
    }

    public static Atomic createValuePredicate(ValueProperty property, Statement statement, Set<Statement> otherStatements,
                                                                             ReasonerQuery parent) {
        HasAttributeProperty has = statement.getProperties(HasAttributeProperty.class).findFirst().orElse(null);
        Variable var = has != null? has.attribute().var() : statement.var();
        ValueProperty.Operation directOperation = property.operation();
        Variable predicateVar = directOperation.innerStatement() != null? directOperation.innerStatement().var() : null;

        //true if the VP has another VP that references it - a parent VP
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
        boolean hasParentVp = !parentVPs.isEmpty();
        if (hasParentVp && !partOfAttribute) return null;

        //
        ValueProperty.Operation indirectOperation = null;
        //ValueProperty.Operation indirectOperation = ReasonerUtils.findValuePropertyOp(predicateVar, otherStatements);
        ValueProperty.Operation operation = indirectOperation != null? indirectOperation : directOperation;
        Object value = operation.innerStatement() == null? operation.value() : null;

        //TODO update parsing for negated vps - save operation correctly - not hardcoded neq
        //maybe make NeqValuePredicate extend ValuePredicate and add variable
        return value != null?
                ValuePredicate.create(var.asReturnedVar(), operation, parent) :
                NeqValuePredicate.create(var.asReturnedVar(), operation, parent);
    }
}

