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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.UndefineQuery;
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
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.session.TransactionOLTP;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.common.util.CommonUtil.toImmutableList;

@SuppressWarnings("Duplicates")
public class UndefineExecutor {

    private final TransactionOLTP transaction;

    public UndefineExecutor(TransactionOLTP transaction) {
        this.transaction = transaction;
    }

    public ConceptMap undefine(UndefineQuery query) {
        ImmutableSet.Builder<Writer.VarAndProperty> properties = ImmutableSet.builder();
        ImmutableList<Statement> allPatterns = query.statements().stream()
                .flatMap(v -> v.innerStatements().stream())
                .collect(toImmutableList());

        for (Statement statement : allPatterns) {
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
            return dataTypeExecutors();

        } else if (property instanceof HasAttributeTypeProperty) {
            return hasAttributeTypeExecutors(var, (HasAttributeTypeProperty) property);

        } else if (property instanceof IdProperty) {
            return idExecutors(var, (IdProperty) property);

        } else if (property instanceof IsAbstractProperty) {
            return isAbstractExecutors(var);
        } else if (property instanceof LabelProperty) {
            return labelExecutors(var, (LabelProperty) property);

        } else if (property instanceof PlaysProperty) {
            return playsExecutor(var, (PlaysProperty) property);

        } else if (property instanceof RegexProperty) {
            return regexExecutors(var, (RegexProperty) property);

        } else if (property instanceof RelatesProperty) {
            return relatesExecutor(var, (RelatesProperty) property);

        } else {
            throw GraqlQueryException.defineUnsupportedProperty(property.getName());
        }
    }

    private Set<PropertyExecutor> subExecutors(Variable var, AbstractSubProperty property) {
        PropertyExecutor.Method method = executor -> {
            SchemaConcept concept = executor.getConcept(var).asSchemaConcept();

            SchemaConcept expectedSuperConcept = executor.getConcept(property.superType().var()).asSchemaConcept();
            SchemaConcept actualSuperConcept = concept.sup();

            if (!concept.isDeleted() && expectedSuperConcept.equals(actualSuperConcept)) {
                concept.delete();
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(var, property.superType().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> dataTypeExecutors() {
        // TODO: resolve the below issue correctly
        // undefine for datatype must be supported, because it is supported in define.
        // However, making it do the right thing is difficult. Ideally we want the same as define:
        //
        //    undefine name datatype string, sub attribute; <- Remove `name`
        //    undefine first-name sub name;                 <- Remove `first-name`
        //    undefine name datatype string;                <- FAIL
        //    undefine name sub attribute;                  <- FAIL
        //
        // Doing this is tough because it means the `datatype` property needs to be aware of the context somehow.
        // As a compromise, we make all the cases succeed (where some do nothing)
        return ImmutableSet.of(PropertyExecutor.builder(executor -> {}).build());
    }

    private Set<PropertyExecutor> hasAttributeTypeExecutors(Variable var, HasAttributeTypeProperty property) {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.getConcept(var).asType();
            AttributeType<?> attributeType = executor.getConcept(property.type().var()).asAttributeType();

            if (!type.isDeleted() && !attributeType.isDeleted()) {
                if (property.isRequired()) {
                    type.unkey(attributeType);
                } else {
                    type.unhas(attributeType);
                }
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var, property.type().var()).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> idExecutors(Variable var, IdProperty property) {
        // This property works in undefine queries, because it is only for look-ups
        PropertyExecutor.Method method = executor -> {
            executor.getBuilder(var).id(property.id());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> isAbstractExecutors(Variable var) {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.getConcept(var).asType();
            if (!type.isDeleted()) {
                type.isAbstract(false);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> labelExecutors(Variable var, LabelProperty property) {
        // This is supported in undefine queries in order to allow looking up schema concepts by label
        PropertyExecutor.Method method = executor -> {
            executor.getBuilder(var).label(property.label());
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).produces(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> playsExecutor(Variable var, PlaysProperty property) {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.getConcept(var).asType();
            Role role = executor.getConcept(property.role().var()).asRole();

            if (!type.isDeleted() && !role.isDeleted()) {
                type.unplay(role);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method)
                .requires(var, property.role().var())
                .build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> regexExecutors(Variable var, RegexProperty property) {
        PropertyExecutor.Method method = executor -> {
            AttributeType<Object> attributeType = executor.getConcept(var).asAttributeType();
            if (!attributeType.isDeleted() && property.regex().equals(attributeType.regex())) {
                attributeType.regex(null);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }

    private Set<PropertyExecutor> relatesExecutor(Variable var, RelatesProperty property) {
        PropertyExecutor.Method method = executor -> {
            RelationshipType relationshipType = executor.getConcept(var).asRelationshipType();
            Role role = executor.getConcept(property.role().var()).asRole();

            if (!relationshipType.isDeleted() && !role.isDeleted()) {
                relationshipType.unrelate(role);
            }
        };

        PropertyExecutor executor = PropertyExecutor.builder(method).requires(var, property.role().var()).build();

        return Collections.unmodifiableSet(Collections.singleton(executor));
    }
}
