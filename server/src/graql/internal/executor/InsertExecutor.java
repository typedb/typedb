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

    private ConceptMap insert(Collection<Statement> statements, ConceptMap results) {
        ImmutableSet.Builder<WriteExecutor.VarAndProperty> properties = ImmutableSet.builder();
        for (Statement statement : statements) {
            for (VarProperty property : statement.getProperties().collect(Collectors.toList())){
                for (PropertyExecutor executor : propertyExecutors(statement.var(), property)) {
                    properties.add(new WriteExecutor.VarAndProperty(statement.var(), property, executor));
                }
            }
        }
        return WriteExecutor.create(properties.build(), transaction).insertAll(results);
    }

    private Set<PropertyExecutor> propertyExecutors(Variable var, VarProperty property) {
        if (property instanceof AbstractIsaProperty) {
            return isaExecutors(var, (AbstractIsaProperty) property);

        } else if (property instanceof HasAttributeProperty) {
            return hasAttributeExecutors(var, (HasAttributeProperty) property);

        } else if (property instanceof IdProperty) {
            return idExecutors(var, (IdProperty) property);

        } else if (property instanceof LabelProperty) {
            return labelExecutors(var, (LabelProperty) property);

        } else if (property instanceof RelationshipProperty) {
            return relationshipExecutors(var, (RelationshipProperty) property);

        } else if (property instanceof ValueProperty) {
            return valueExecutors(var, (ValueProperty) property);

        } else {
            throw GraqlQueryException.insertUnsupportedProperty(property.getName());
        }
    }

    private Set<PropertyExecutor> isaExecutors(Variable var, AbstractIsaProperty property) {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(property.type().var()).asType();
            executor.builder(var).isa(type);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(property.type().var())
                .produces(var)
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> hasAttributeExecutors(Variable var, HasAttributeProperty property) {
        PropertyExecutor.Method method = executor -> {
            Attribute attributeConcept = executor.get(property.attribute().var()).asAttribute();
            Thing thing = executor.get(var).asThing();
            ConceptId relationshipId = thing.relhas(attributeConcept).id();
            executor.builder(property.relationship().var()).id(relationshipId);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .produces(property.relationship().var())
                .requires(var, property.attribute().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> idExecutors(Variable var, IdProperty property) {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).id(property.id());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> labelExecutors(Variable var, LabelProperty property) {
        // This is supported in insert queries in the same way it does for define queries
        // in order to allow looking up schema concepts by label
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).label(property.label());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> relationshipExecutors(Variable var, RelationshipProperty property) {
        PropertyExecutor.Method method = executor -> {
            Relationship relationship = executor.get(var).asRelationship();
            property.relationPlayers().forEach(relationPlayer -> property.addRoleplayer(executor, relationship, relationPlayer));
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(property.requiredVars(var)).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> valueExecutors(Variable var, ValueProperty property) {
        PropertyExecutor.Method method = executor -> {
            Object value = property.predicate().equalsValue().orElseThrow(GraqlQueryException::insertPredicate);
            executor.builder(var).value(value);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }
}
