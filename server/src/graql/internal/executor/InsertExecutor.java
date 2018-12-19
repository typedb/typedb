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

package grakn.core.graql.internal.executor;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.InsertQuery;
import grakn.core.graql.query.MatchClause;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.AbstractIsaProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.PropertyExecutor;
import grakn.core.graql.query.pattern.property.RelationshipProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableList;
import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("Duplicates")
public class InsertExecutor {

    private final TransactionOLTP transaction;
    private final boolean infer;

    public InsertExecutor(TransactionOLTP transaction, boolean infer) {
        this.transaction = transaction;
        this.infer = infer;
    }

    public Stream<ConceptMap> insert(InsertQuery query) {
        Collection<Statement> statements = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        if (query.match() != null) {
            MatchClause match = query.match();
            Set<Variable> matchVars = match.getSelectedNames();
            Set<Variable> insertVars = statements.stream().map(statement -> statement.var()).collect(toImmutableSet());

            Set<Variable> projectedVars = new HashSet<>(matchVars);
            projectedVars.retainAll(insertVars);

            Stream<ConceptMap> answers = transaction.stream(match.get(projectedVars), infer);
            return answers.map(answer -> insert(statements, answer)).collect(toList()).stream();
        } else {
            return Stream.of(insert(statements, new ConceptMap()));
        }
    }

    private ConceptMap insert(Collection<Statement> patterns, ConceptMap results) {
        ImmutableSet.Builder<WriteExecutor.VarAndProperty> properties = ImmutableSet.builder();
        for (Statement statement : patterns) {
            for (VarProperty property : statement.getProperties().collect(Collectors.toList())){
                for (PropertyExecutor executor : propertyExecutors(property, statement.var())) {
                    properties.add(new WriteExecutor.VarAndProperty(statement.var(), property, executor));
                }
            }
        }
        return WriteExecutor.create(properties.build(), transaction).insertAll(results);
    }

    private Set<PropertyExecutor> propertyExecutors(VarProperty varProperty, Variable var) {
        if (varProperty instanceof AbstractIsaProperty) {
            return isaExecutors((AbstractIsaProperty) varProperty, var);
        } else if (varProperty instanceof HasAttributeProperty) {
            return hasAttributeExecutors((HasAttributeProperty) varProperty, var);
        } else if (varProperty instanceof IdProperty) {
            return idExecutors((IdProperty) varProperty, var);
        } else if (varProperty instanceof LabelProperty) {
            return labelExecutors((LabelProperty) varProperty, var);
        } else if (varProperty instanceof RelationshipProperty) {
            return relationshipExecutors((RelationshipProperty) varProperty, var);
        } else if (varProperty instanceof ValueProperty) {
            return valueExecutors((ValueProperty) varProperty, var);
        } else {
            throw GraqlQueryException.insertUnsupportedProperty(varProperty.getName());
        }
    }

    private Set<PropertyExecutor> isaExecutors(AbstractIsaProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(varProperty.type().var()).asType();
            executor.builder(var).isa(type);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(varProperty.type().var())
                .produces(var)
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> hasAttributeExecutors(HasAttributeProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            Attribute attributeConcept = executor.get(varProperty.attribute().var()).asAttribute();
            Thing thing = executor.get(var).asThing();
            ConceptId relationshipId = thing.relhas(attributeConcept).id();
            executor.builder(varProperty.relationship().var()).id(relationshipId);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .produces(varProperty.relationship().var())
                .requires(var, varProperty.attribute().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> idExecutors(IdProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).id(varProperty.id());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> labelExecutors(LabelProperty varProperty, Variable var) {
        // This is supported in insert queries in the same way it does for define queries
        // in order to allow looking up schema concepts by label
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).label(varProperty.label());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> relationshipExecutors(RelationshipProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            Relationship relationship = executor.get(var).asRelationship();
            varProperty.relationPlayers().forEach(relationPlayer -> varProperty.addRoleplayer(executor, relationship, relationPlayer));
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(varProperty.requiredVars(var)).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> valueExecutors(ValueProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            Object value = varProperty.predicate().equalsValue().orElseThrow(GraqlQueryException::insertPredicate);
            executor.builder(var).value(value);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }
}
