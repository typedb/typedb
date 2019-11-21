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
 *
 */

package grakn.core.graql.reasoner.atom;

import com.google.common.collect.ImmutableMap;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graql.executor.property.PropertyExecutorFactoryImpl;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.HasAtom;
import grakn.core.graql.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.atom.property.DataTypeAtom;
import grakn.core.graql.reasoner.atom.property.IsAbstractAtom;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.graql.reasoner.utils.ReasonerUtils;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.property.DataTypeProperty;
import graql.lang.property.HasAttributeProperty;
import graql.lang.property.HasAttributeTypeProperty;
import graql.lang.property.IsaProperty;
import graql.lang.property.RelationProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.CheckReturnValue;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.graql.reasoner.utils.ReasonerUtils.getIdPredicate;
import static grakn.core.graql.reasoner.utils.ReasonerUtils.getValuePredicates;

/**
 * Factory class for creating Atomic objects.
 */
public class AtomicFactory {

    public Atomic hasAttributeType(Variable var, HasAttributeTypeProperty property, ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //NB: HasResourceType is a special case and it doesn't allow variables as resource types
        String label = property.attributeType().getType().orElse(null);

        Variable predicateVar = new Variable();
        SchemaConcept attributeType = parent.tx().getSchemaConcept(Label.of(label));
        ConceptId predicateId = attributeType != null ? attributeType.id() : null;
        return HasAtom.create(var, predicateVar, predicateId, parent);
    }

    Atomic hasAttribute(Variable var, HasAttributeProperty property, ReasonerQuery parent, Set<Statement> otherStatements) {
        //NB: HasAttributeProperty always has (type) label specified
        Variable varName = var.asReturnedVar();

        //NB: we always make the attribute variable explicit
        Variable attributeVariable = property.attribute().var().asReturnedVar();
        Variable relationVariable = property.relation().var();
        Variable predicateVariable = new Variable();
        Set<ValuePredicate> predicates = getValuePredicates(attributeVariable, property.attribute(), otherStatements, parent,
                new PropertyExecutorFactoryImpl());

        IsaProperty isaProp = property.attribute().getProperties(IsaProperty.class).findFirst().orElse(null);
        Statement typeVar = isaProp != null ? isaProp.type() : null;
        IdPredicate predicate = typeVar != null ? getIdPredicate(predicateVariable, typeVar, otherStatements, parent) : null;
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //add resource atom
        Statement resVar = relationVariable.isReturned() ?
                new Statement(varName).has(property.type(), new Statement(attributeVariable), new Statement(relationVariable)) :
                new Statement(varName).has(property.type(), new Statement(attributeVariable));
        return AttributeAtom.create(resVar, attributeVariable, relationVariable, predicateVariable, predicateId, predicates, parent);
    }


    public DataTypeAtom dataType(Variable var, DataTypeProperty property, ReasonerQuery parent) {
        ImmutableMap.Builder<Graql.Token.DataType, AttributeType.DataType<?>> dataTypesBuilder = new ImmutableMap.Builder<>();
        dataTypesBuilder.put(Graql.Token.DataType.BOOLEAN, AttributeType.DataType.BOOLEAN);
        dataTypesBuilder.put(Graql.Token.DataType.DATE, AttributeType.DataType.DATE);
        dataTypesBuilder.put(Graql.Token.DataType.DOUBLE, AttributeType.DataType.DOUBLE);
        dataTypesBuilder.put(Graql.Token.DataType.LONG, AttributeType.DataType.LONG);
        dataTypesBuilder.put(Graql.Token.DataType.STRING, AttributeType.DataType.STRING);
        ImmutableMap<Graql.Token.DataType, AttributeType.DataType<?>> dataTypes = dataTypesBuilder.build();
        return DataTypeAtom.create(var, property, parent, dataTypes.get(property.dataType()));
    }

    Atomic isAbstract(Variable var, ReasonerQuery parent) {
        return IsAbstractAtom.create(var, parent);
    }

    Atomic isa(Variable var, IsaProperty property, ReasonerQuery parent, Statement statement, Set<Statement> otherStatements) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (statement.hasProperty(RelationProperty.class)) return null;

        Variable typeVar = property.type().var();

        IdPredicate predicate = getIdPredicate(typeVar, property.type(), otherStatements, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //isa part
        Statement isaVar;

        if (property.isExplicit()) {
            isaVar = new Statement(var).isaX(new Statement(typeVar));
        } else {
            isaVar = new Statement(var).isa(new Statement(typeVar));
        }

        return IsaAtom.create(var, typeVar, isaVar, predicateId, parent);
    }




    /**
     * @param pattern conjunction of patterns to be converted to atoms
     * @param parent query the created atoms should belong to
     * @return set of atoms
     */
    public static Stream<Atomic> createAtoms(Conjunction<Statement> pattern, ReasonerQuery parent) {
        //parse all atoms
        Set<Atomic> atoms = pattern.statements().stream()
                .flatMap(statement -> statement.properties().stream()
                        .map(property -> parent.tx().propertyExecutorFactory()
                                .create(statement.var(), property)
                                .atomic(parent, statement, pattern.statements()))
                        .filter(Objects::nonNull))
                .collect(Collectors.toSet());

        //Extract variable predicates from attributes and add them to the atom set
        //We need to treat them separately to ensure correctness - different conditions can arise with different
        //orderings of resolvable atoms - hence we need to compare at the end.
        //NB: this creates different vps because the statement context is bound to HasAttributeProperty.
        Set<Atomic> neqs = pattern.statements().stream()
                .flatMap(statement -> statement.getProperties(HasAttributeProperty.class))
                .flatMap(hp -> hp.statements().flatMap(
                        statement -> statement.getProperties(ValueProperty.class)
                                .map(property -> parent.tx().propertyExecutorFactory()
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
                )
                .map(Atomic::simplify);
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

        Object value = indirectOperation == null ? directOperation.value() : indirectOperation.value();
        ValueProperty.Operation operation = ValueProperty.Operation.Comparison.of(directOperation.comparator(), value);
        return ValuePredicate.create(var.asReturnedVar(), operation, parent);
    }
}

