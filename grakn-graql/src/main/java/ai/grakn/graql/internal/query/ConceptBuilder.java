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
import ai.grakn.graql.internal.pattern.property.DataTypeProperty;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import ai.grakn.graql.internal.pattern.property.SubProperty;
import ai.grakn.graql.internal.pattern.property.ThenProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.pattern.property.WhenProperty;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for building a {@link Concept}, by providing properties.
 *
 * @author Felix Chapman
 */
public class ConceptBuilder {

    private final Var var;

    @FunctionalInterface
    private interface BuilderParam<T> {
        String name();
    }

    private static final BuilderParam<Type> TYPE = () -> IsaProperty.NAME;
    private static final BuilderParam<OntologyConcept> SUPER_CONCEPT = () -> SubProperty.NAME;
    private static final BuilderParam<Label> LABEL = () -> LabelProperty.NAME;
    private static final BuilderParam<ConceptId> ID = () -> IdProperty.NAME;
    private static final BuilderParam<Object> VALUE = () -> ValueProperty.NAME;
    private static final BuilderParam<ResourceType.DataType<?>> DATA_TYPE = () -> DataTypeProperty.NAME;
    private static final BuilderParam<Pattern> WHEN = () -> WhenProperty.NAME;
    private static final BuilderParam<Pattern> THEN = () -> ThenProperty.NAME;

    private final Map<BuilderParam<?>, Object> params = new HashMap<>();

    ConceptBuilder(Var var) {
        this.var = var;
    }

    private <T> T get(BuilderParam<T> param) {
        // This is safe, assuming we only add to the map with the `set` method
        //noinspection unchecked
        return checkNotNull((T) params.get(param));
    }

    private <T> Optional<T> tryGet(BuilderParam<T> param) {
        // This is safe, assuming we only add to the map with the `set` method
        //noinspection unchecked
        return Optional.ofNullable((T) params.get(param));
    }

    private boolean has(BuilderParam<?> param) {
        return params.containsKey(param);
    }

    private <T> ConceptBuilder set(BuilderParam<T> param, T value) {
        if (params.containsKey(param) && !params.get(param).equals(value)) {
            throw GraqlQueryException.insertMultipleProperties(param.name(), value, params.get(param));
        }
        params.put(param, checkNotNull(value));
        return this;
    }

    public ConceptBuilder isa(Type type) {
        return set(TYPE, type);
    }

    public ConceptBuilder sub(OntologyConcept superConcept) {
        return set(SUPER_CONCEPT, superConcept);
    }

    public ConceptBuilder label(Label label) {
        return set(LABEL, label);
    }

    public ConceptBuilder id(ConceptId id) {
        return set(ID, id);
    }

    public ConceptBuilder value(Object value) {
        return set(VALUE, value);
    }

    public ConceptBuilder dataType(ResourceType.DataType<?> dataType) {
        return set(DATA_TYPE, dataType);
    }

    public ConceptBuilder when(Pattern when) {
        return set(WHEN, when);
    }

    public ConceptBuilder then(Pattern then) {
        return set(THEN, then);
    }

    /**
     * Build the {@link Concept} and return it, using the properties given.
     *
     * @throws GraqlQueryException if the properties provided are inconsistent
     */
    Concept build(InsertQueryExecutor executor) {
        if (has(TYPE) && has(SUPER_CONCEPT)) {
            throw GraqlQueryException.insertIsaAndSub("hello"); // TODO
        }

        if (has(LABEL) && has(TYPE)) {
            throw GraqlQueryException.insertInstanceWithLabel(get(LABEL));
        }

        // If type provided, then 'put' the concept, else 'get' it by ID or label
        if (has(SUPER_CONCEPT)) {
            return putOntologyConcept(executor);
        } else if (has(TYPE)) {
            return putInstance(executor);
        } else if (has(ID)) {
            Concept concept = executor.graph().getConcept(get(ID));
            if (concept == null) throw GraqlQueryException.insertWithoutType(get(ID));
            return concept;
        } else if (has(LABEL)) {
            Concept concept = executor.graph().getOntologyConcept(get(LABEL));
            if (concept == null) throw GraqlQueryException.labelNotFound(get(LABEL));
            return concept;
        } else {
            throw GraqlQueryException.insertUndefinedVariable(executor.printableRepresentation(var));
        }
    }

    private Thing putInstance(InsertQueryExecutor executor) {
        Type type = get(TYPE);
        @Nullable ConceptId id = tryGet(ID).orElse(null);

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
                        if (!has(VALUE)) {
                            throw GraqlQueryException.insertResourceWithoutValue();
                        }
                        return type.asResourceType().putResource(get(VALUE));
                    }
            );
        } else if (type.isRuleType()) {
            return addOrGetInstance(executor, id, () -> {
                if (!has(WHEN)) throw GraqlQueryException.insertRuleWithoutLhs(executor.printableRepresentation(var));
                if (!has(THEN)) throw GraqlQueryException.insertRuleWithoutRhs(executor.printableRepresentation(var));
                return type.asRuleType().putRule(get(WHEN), get(THEN));
            });
        } else if (type.getLabel().equals(Schema.MetaSchema.THING.getLabel())) {
            throw GraqlQueryException.createInstanceOfMetaConcept(var, type);
        } else {
            throw CommonUtil.unreachableStatement("Can't recognize type " + type);
        }
    }

    private void checkNotRule() {
        if (has(WHEN)) {
            throw GraqlQueryException.insertUnsupportedProperty("when", Schema.MetaSchema.RULE);
        }

        if (has(THEN)) {
            throw GraqlQueryException.insertUnsupportedProperty("then", Schema.MetaSchema.RULE);
        }
    }

    /**
     * @return a concept with the given ID and the specified type
     */
    private OntologyConcept putOntologyConcept(InsertQueryExecutor executor) {
        if (!has(LABEL)) throw GraqlQueryException.insertTypeWithoutLabel();

        OntologyConcept superConcept = get(SUPER_CONCEPT);
        Label label = get(LABEL);

        checkNotRule();

        if (superConcept.isEntityType()) {
            return executor.graph().putEntityType(label).sup(superConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            return executor.graph().putRelationType(label).sup(superConcept.asRelationType());
        } else if (superConcept.isRole()) {
            return executor.graph().putRole(label).sup(superConcept.asRole());
        } else if (superConcept.isResourceType()) {
            ResourceType resourceType = superConcept.asResourceType();
            @Nullable ResourceType.DataType<?> dataType = tryGet(DATA_TYPE).orElse(null);
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
