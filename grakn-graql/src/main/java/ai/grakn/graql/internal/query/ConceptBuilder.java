/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 */

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;

import javax.annotation.Nullable;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for building a {@link Concept}, by providing properties.
 *
 * @author Felix Chapman
 */
public class ConceptBuilder {

    private final Var var;

    private @Nullable Type type = null;
    private @Nullable OntologyConcept superConcept = null;
    private @Nullable Label label = null;
    private @Nullable ConceptId id = null;
    private @Nullable Object value = null;
    private @Nullable ResourceType.DataType<?> dataType = null;
    private @Nullable Pattern when = null;
    private @Nullable Pattern then = null;

    ConceptBuilder(Var var) {
        this.var = var;
    }

    public ConceptBuilder isa(Type type) {
        // TODO: Handle set isa twice
        this.type = type;
        return this;
    }

    public ConceptBuilder sub(OntologyConcept superConcept) {
        // TODO: Handle set sub twice
        this.superConcept = superConcept;
        return this;
    }

    public ConceptBuilder label(Label label) {
        // TODO: Handle set label twice
        this.label = label;
        return this;
    }

    public ConceptBuilder id(ConceptId id) {
        // TODO: Handle set id twice
        this.id = id;
        return this;
    }

    public ConceptBuilder value(Object value) {
        if (this.value != null && this.value != value) {
            throw GraqlQueryException.insertMultipleValues(this.value, value);
        }

        this.value = value;
        return this;
    }

    public ConceptBuilder dataType(ResourceType.DataType<?> dataType) {
        // TODO: Handle set datatype twice
        this.dataType = dataType;
        return this;
    }

    public ConceptBuilder when(Pattern when) {
        // TODO: Handle set `when` twice
        this.when = when;
        return this;
    }

    public ConceptBuilder then(Pattern then) {
        // TODO: Handle set `then` twice
        this.then = then;
        return this;
    }

    /**
     * Build the {@link Concept} and return it, using the properties given.
     *
     * @throws GraqlQueryException if the properties provided are inconsistent
     */
    Concept build(InsertQueryExecutor executor) {
        if (type != null && superConcept != null) {
            throw GraqlQueryException.insertIsaAndSub("hello"); // TODO
        }

        if (label != null && type != null) {
            throw GraqlQueryException.insertInstanceWithLabel(label);
        }

        // If type provided, then 'put' the concept, else 'get' it by ID or label
        if (superConcept != null) {
            return putOntologyConcept(executor);
        } else if (type != null) {
            return putInstance(executor);
        } else if (id != null) {
            Concept concept = executor.graph().getConcept(id);
            if (concept == null) throw GraqlQueryException.insertWithoutType(id);
            return concept;
        } else if (label != null) {
            Concept concept = executor.graph().getOntologyConcept(label);
            if (concept == null) throw GraqlQueryException.labelNotFound(label);
            return concept;
        } else {
            throw GraqlQueryException.insertUndefinedVariable(executor.printableRepresentation(var));
        }
    }

    private Thing putInstance(InsertQueryExecutor executor) {
        checkNotNull(type);

        if (type.isEntityType()) {
            checkNotRule();
            return addOrGetInstance(executor, id, type.asEntityType()::addEntity);
        } else if (type.isRelationType()) {
            checkNotRule();
            return addOrGetInstance(executor, id, type.asRelationType()::addRelation);
        } else if (type.isResourceType()) {
            checkNotRule();
            return addOrGetInstance(executor, id,
                    () -> {
                        if (value == null) {
                            throw GraqlQueryException.insertResourceWithoutValue();
                        }
                        return type.asResourceType().putResource(value);
                    }
            );
        } else if (type.isRuleType()) {
            return addOrGetInstance(executor, id, () -> {
                if (when == null) throw GraqlQueryException.insertRuleWithoutLhs(executor.printableRepresentation(var));
                if (then == null) throw GraqlQueryException.insertRuleWithoutRhs(executor.printableRepresentation(var));
                checkNotNull(when); // TODO: proper errors
                checkNotNull(then);
                return type.asRuleType().putRule(when, then);
            });
        } else if (type.getLabel().equals(Schema.MetaSchema.THING.getLabel())) {
            throw GraqlQueryException.createInstanceOfMetaConcept(var, type);
        } else {
            throw CommonUtil.unreachableStatement("Can't recognize type " + type);
        }
    }

    private void checkNotRule() {
        if (when != null) {
            throw GraqlQueryException.insertUnsupportedProperty("when", Schema.MetaSchema.RULE);
        }

        if (then != null) {
            throw GraqlQueryException.insertUnsupportedProperty("then", Schema.MetaSchema.RULE);
        }
    }

    /**
     * @return a concept with the given ID and the specified type
     */
    private OntologyConcept putOntologyConcept(InsertQueryExecutor executor) {
        if (label == null) throw GraqlQueryException.insertTypeWithoutLabel();
        checkNotNull(superConcept);

        checkNotRule();

        if (superConcept.isEntityType()) {
            return executor.graph().putEntityType(label).sup(superConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            return executor.graph().putRelationType(label).sup(superConcept.asRelationType());
        } else if (superConcept.isRole()) {
            return executor.graph().putRole(label).sup(superConcept.asRole());
        } else if (superConcept.isResourceType()) {
            ResourceType resourceType = superConcept.asResourceType();
            if (dataType == null) {
                dataType = resourceType.getDataType();
            } // TODO: What if set twice?

            if (dataType == null) {
                throw GraqlQueryException.insertResourceTypeWithoutDataType(executor.printableRepresentation(var));
            }

            return executor.graph().putResourceType(label, dataType).sup(resourceType);
        } else if (superConcept.isRuleType()) {
            return executor.graph().putRuleType(label).sup(superConcept.asRuleType());
        } else {
            throw GraqlQueryException.insertMetaType(label, superConcept);
        }
    }

    /**
     * Put an instance of a type which may or may not have an ID specified
     * @param id the ID of the instance to create, or null to not specify an ID
     * @param addInstance an 'add' method on a GraknGraph such a graph::addEntity
     * @param <S> the class of the instance, e.g. Entity
     * @return an instance of the specified type, with the given ID if one was specified
     */
    private <S extends Thing> S addOrGetInstance(
            InsertQueryExecutor executor, @Nullable ConceptId id, Supplier<S> addInstance) {
        return id != null ? executor.graph().getConcept(id) : addInstance.get();
    }
}
