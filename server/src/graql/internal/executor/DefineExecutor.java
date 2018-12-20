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
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.DefineQuery;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.AbstractSubProperty;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.PropertyExecutor;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.util.CommonUtil.toImmutableList;

@SuppressWarnings("Duplicates")
public class DefineExecutor {

    private final TransactionOLTP transaction;

    public DefineExecutor(TransactionOLTP transaction) {
        this.transaction = transaction;
    }

    public ConceptMap define(DefineQuery query) {
        ImmutableSet.Builder<Writer.VarAndProperty> properties = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(s -> s.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.getProperties().collect(Collectors.toList())){
                for (PropertyExecutor executor : propertyExecutors(statement.var(), property)) {
                    properties.add(new Writer.VarAndProperty(statement.var(), property, executor));
                }
            }
        }
        return Writer.create(properties.build(), transaction).insertAll(new ConceptMap());
    }

    private Set<PropertyExecutor> propertyExecutors(Variable var, VarProperty property) {
        if (property instanceof AbstractSubProperty) {
            return subExecutors(var, (AbstractSubProperty) property);

        } else if (property instanceof DataTypeProperty) {
            return dataTypeExecutors(var, (DataTypeProperty) property);

        } else if (property instanceof HasAttributeTypeProperty) {
            return hasAttributeTypeExecutors(var, (HasAttributeTypeProperty) property);

        } else if (property instanceof IdProperty) {
            return idExecutors(var, (IdProperty) property);

        } else if (property instanceof IsAbstractProperty) {
            return isAbstractExecutors(var);

        } else if (property instanceof LabelProperty) {
            return labelExecutors(var, (LabelProperty) property);

        } else if (property instanceof PlaysProperty) {
            return playsExecutors(var, (PlaysProperty) property);

        } else if (property instanceof RegexProperty) {
            return regexExecutors(var, (RegexProperty) property);

        } else if (property instanceof RelatesProperty) {
            return relatesExecutors(var, (RelatesProperty) property);

        } else if (property instanceof ThenProperty) {
            return thenExecutors(var, (ThenProperty) property);

        } else if (property instanceof WhenProperty) {
            return whenExecutors(var, (WhenProperty) property);

        } else {
            throw GraqlQueryException.defineUnsupportedProperty(property.getName());
        }
    }

    private Set<PropertyExecutor> subExecutors(Variable var, AbstractSubProperty property) {
        PropertyExecutor.Method method = executor -> {
            SchemaConcept superConcept = executor.getConcept(property.superType().var()).asSchemaConcept();

            Optional<ConceptBuilder> builder = executor.tryBuilder(var);

            if (builder.isPresent()) {
                builder.get().sub(superConcept);
            } else {
                ConceptBuilder.setSuper(executor.getConcept(var).asSchemaConcept(), superConcept);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(property.superType().var())
                .produces(var)
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> dataTypeExecutors(Variable var, DataTypeProperty property) {
        PropertyExecutor.Method method = executor -> {
            executor.getBuilder(var).dataType(property.dataType());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> hasAttributeTypeExecutors(Variable var, HasAttributeTypeProperty property) {
        PropertyExecutor.Method method = executor -> {
            Type entityTypeConcept = executor.getConcept(var).asType();
            AttributeType attributeTypeConcept = executor.getConcept(property.type().var()).asAttributeType();

            if (property.isRequired()) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.has(attributeTypeConcept);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(var, property.type().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> idExecutors(Variable var, IdProperty property) {
        // This property works in both insert and define queries, because it is only for look-ups
        PropertyExecutor.Method method = executor -> {
            executor.getBuilder(var).id(property.id());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> isAbstractExecutors(Variable var) {
        PropertyExecutor.Method method = executor -> {
            Concept concept = executor.getConcept(var);
            if (concept.isType()) {
                concept.asType().isAbstract(true);
            } else {
                throw GraqlQueryException.insertAbstractOnNonType(concept.asSchemaConcept());
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> labelExecutors(Variable var, LabelProperty property) {
        PropertyExecutor.Method method = executor -> {
            executor.getBuilder(var).label(property.label());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> playsExecutors(Variable var, PlaysProperty property) {
        PropertyExecutor.Method method = executor -> {
            Role role = executor.getConcept(property.role().var()).asRole();
            executor.getConcept(var).asType().plays(role);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(var, property.role().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> regexExecutors(Variable var, RegexProperty property) {
        PropertyExecutor.Method method = executor -> {
            executor.getConcept(var).asAttributeType().regex(property.regex());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> relatesExecutors(Variable var, RelatesProperty property) {
        Set<PropertyExecutor> propertyExecutors = new HashSet<>();
        Variable roleVar = property.role().var();

        PropertyExecutor.Method relatesMethod = executor -> {
            Role role = executor.getConcept(roleVar).asRole();
            executor.getConcept(var).asRelationshipType().relates(role);
        };

        PropertyExecutor relatesExecutor = PropertyExecutor.builder(relatesMethod).requires(var, roleVar).build();
        propertyExecutors.add(relatesExecutor);

        // This allows users to skip stating `$roleVar sub role` when they say `$var relates $roleVar`
        PropertyExecutor.Method isRoleMethod = executor -> executor.getBuilder(roleVar).isRole();

        PropertyExecutor isRoleExecutor = PropertyExecutor.builder(isRoleMethod).produces(roleVar).build();
        propertyExecutors.add(isRoleExecutor);

        Statement superRoleStatement = property.superRole();
        if (superRoleStatement != null) {
            Variable superRoleVar = superRoleStatement.var();
            PropertyExecutor.Method subMethod = executor -> {
                Role superRole = executor.getConcept(superRoleVar).asRole();
                executor.getBuilder(roleVar).sub(superRole);
            };

            PropertyExecutor subExecutor = PropertyExecutor.builder(subMethod)
                    .requires(superRoleVar)
                    .produces(roleVar)
                    .build();

            propertyExecutors.add(subExecutor);
        }

        return Collections.unmodifiableSet(propertyExecutors);
    }

    private Set<PropertyExecutor> thenExecutors(Variable var, ThenProperty property) {
        PropertyExecutor.Method method = executor -> {
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar then { ... }`
            executor.getBuilder(var).isRule().then(property.pattern());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> whenExecutors(Variable var, WhenProperty property) {
        PropertyExecutor.Method method = executor -> {
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar when { ... }`
            executor.getBuilder(var).isRule().when(property.pattern());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }
}
