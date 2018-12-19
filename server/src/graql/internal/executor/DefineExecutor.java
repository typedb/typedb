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
        ImmutableSet.Builder<WriteExecutor.VarAndProperty> properties = ImmutableSet.builder();
        List<Statement> statements = query.statements().stream()
                .flatMap(s -> s.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : statements) {
            for (VarProperty property : statement.getProperties().collect(Collectors.toList())){
                for (PropertyExecutor executor : propertyExecutors(property, statement.var())) {
                    properties.add(new WriteExecutor.VarAndProperty(statement.var(), property, executor));
                }
            }
        }
        return WriteExecutor.create(properties.build(), transaction).insertAll(new ConceptMap());
    }

    private Set<PropertyExecutor> propertyExecutors(VarProperty varProperty, Variable var) {
        if (varProperty instanceof AbstractSubProperty) {
            return subPropertyExecutors((AbstractSubProperty) varProperty, var);
        } else if (varProperty instanceof DataTypeProperty) {
            return dataTypePropertyExecutors((DataTypeProperty) varProperty, var);
        } else if (varProperty instanceof HasAttributeTypeProperty) {
            return hasAttributeTypePropertyExecutors((HasAttributeTypeProperty) varProperty, var);
        } else if (varProperty instanceof IdProperty) {
            return idPropertyExecutors((IdProperty) varProperty, var);
        } else if (varProperty instanceof IsAbstractProperty) {
            return isAbstractPropertyExecutors(var);
        } else if (varProperty instanceof LabelProperty) {
            return labelPropertyExecutors((LabelProperty) varProperty, var);
        } else if (varProperty instanceof PlaysProperty) {
            return playsPropertyExecutors((PlaysProperty) varProperty, var);
        } else if (varProperty instanceof RegexProperty) {
            return regexPropertyExecutors((RegexProperty) varProperty, var);
        } else if (varProperty instanceof RelatesProperty) {
            return relatesPropertyExecutors((RelatesProperty) varProperty, var);
        } else if (varProperty instanceof ThenProperty) {
            return thenPropertyExecutors((ThenProperty) varProperty, var);
        } else if (varProperty instanceof WhenProperty) {
            return whenPropertyExecutors((WhenProperty) varProperty, var);
        } else {
            throw GraqlQueryException.defineUnsupportedProperty(varProperty.getName());
        }
    }

    private Set<PropertyExecutor> subPropertyExecutors(AbstractSubProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            SchemaConcept superConcept = executor.get(varProperty.superType().var()).asSchemaConcept();

            Optional<ConceptBuilder> builder = executor.tryBuilder(var);

            if (builder.isPresent()) {
                builder.get().sub(superConcept);
            } else {
                ConceptBuilder.setSuper(executor.get(var).asSchemaConcept(), superConcept);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(varProperty.superType().var())
                .produces(var)
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> dataTypePropertyExecutors(DataTypeProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).dataType(varProperty.dataType());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> hasAttributeTypePropertyExecutors(HasAttributeTypeProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            Type entityTypeConcept = executor.get(var).asType();
            AttributeType attributeTypeConcept = executor.get(varProperty.type().var()).asAttributeType();

            if (varProperty.isRequired()) {
                entityTypeConcept.key(attributeTypeConcept);
            } else {
                entityTypeConcept.has(attributeTypeConcept);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(var, varProperty.type().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> idPropertyExecutors(IdProperty varProperty, Variable var) {
        // This property works in both insert and define queries, because it is only for look-ups
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).id(varProperty.id());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> isAbstractPropertyExecutors(Variable var) {
        PropertyExecutor.Method method = executor -> {
            Concept concept = executor.get(var);
            if (concept.isType()) {
                concept.asType().isAbstract(true);
            } else {
                throw GraqlQueryException.insertAbstractOnNonType(concept.asSchemaConcept());
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> labelPropertyExecutors(LabelProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).label(varProperty.label());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> playsPropertyExecutors(PlaysProperty varProperty, Variable var) {
        PropertyExecutor.Method method = executor -> {
            Role role = executor.get(varProperty.role().var()).asRole();
            executor.get(var).asType().plays(role);
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(var, varProperty.role().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> regexPropertyExecutors(RegexProperty varProperty, Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.get(var).asAttributeType().regex(varProperty.regex());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> relatesPropertyExecutors(RelatesProperty varProperty, Variable var) throws GraqlQueryException {
        Set<PropertyExecutor> propertyExecutors = new HashSet<>();
        Variable roleVar = varProperty.role().var();

        PropertyExecutor.Method relatesMethod = executor -> {
            Role role = executor.get(roleVar).asRole();
            executor.get(var).asRelationshipType().relates(role);
        };

        PropertyExecutor relatesExecutor = PropertyExecutor.builder(relatesMethod).requires(var, roleVar).build();
        propertyExecutors.add(relatesExecutor);

        // This allows users to skip stating `$roleVar sub role` when they say `$var relates $roleVar`
        PropertyExecutor.Method isRoleMethod = executor -> executor.builder(roleVar).isRole();

        PropertyExecutor isRoleExecutor = PropertyExecutor.builder(isRoleMethod).produces(roleVar).build();
        propertyExecutors.add(isRoleExecutor);

        Statement superRoleStatement = varProperty.superRole();
        if (superRoleStatement != null) {
            Variable superRoleVar = superRoleStatement.var();
            PropertyExecutor.Method subMethod = executor -> {
                Role superRole = executor.get(superRoleVar).asRole();
                executor.builder(roleVar).sub(superRole);
            };

            PropertyExecutor subExecutor = PropertyExecutor.builder(subMethod)
                    .requires(superRoleVar)
                    .produces(roleVar)
                    .build();

            propertyExecutors.add(subExecutor);
        }

        return Collections.unmodifiableSet(propertyExecutors);
    }

    private Set<PropertyExecutor> thenPropertyExecutors(ThenProperty varProperty, Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar then { ... }`
            executor.builder(var).isRule().then(varProperty.pattern());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> whenPropertyExecutors(WhenProperty varProperty, Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            // This allows users to skip stating `$ruleVar sub rule` when they say `$ruleVar when { ... }`
            executor.builder(var).isRule().when(varProperty.pattern());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }
}
