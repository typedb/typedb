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
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Rule;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for building a {@link Concept}, by providing properties.
 *
 * <p>
 *     A {@link ai.grakn.graql.admin.VarProperty} is responsible for inserting itself into the graph. However,
 *     some properties can only operate in <i>combination</i>. For example, to create a {@link Resource} you need both
 *     an {@link IsaProperty} and a {@link ValueProperty}.
 * </p>
 * <p>
 *     Therefore, these properties do not create the {@link Concept} themselves.
 *     instead they provide the necessary information to the {@link ConceptBuilder}, which will create the
 *     {@link Concept} at a later time:
 * </p>
 * <pre>
 *     // Executor:
 *     ConceptBuilder builder = ConceptBuilder.of(executor, var);
 *     // IsaProperty:
 *     builder.isa(name);
 *     // ValueProperty:
 *     builder.value("Bob");
 *     // Executor:
 *     Concept concept = builder.build();
 * </pre>
 *
 * @author Felix Chapman
 */
public class ConceptBuilder {

    private final InsertQueryExecutor executor;

    private final Var var;

    /**
     * A map of parameters that have been specified for this concept.
     */
    private final Map<BuilderParam<?>, Object> params = new HashMap<>();

    /**
     * A set of expected parameters, used when executing {@link #build()}.
     * <p>
     * Every time {@link #expect(BuilderParam)} or {@link #expectOrDefault(BuilderParam, Object)} is called, the argument is
     * added to this set. After the concept is built, any parameter not in this set is considered "unexpected". If it
     * is present in the field {@link #params}, then an error is thrown.
     * </p>
     */
    private final Set<BuilderParam<?>> expectedParams = new HashSet<>();

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

    static ConceptBuilder of(InsertQueryExecutor executor, Var var) {
        return new ConceptBuilder(executor, var);
    }

    /**
     * Build the {@link Concept} and return it, using the properties given.
     *
     * @throws GraqlQueryException if the properties provided are inconsistent
     */
    Concept build() {

        // If a label or ID is provided, attempt to `get` the concept
        Concept concept = tryGetConcept();

        if (concept != null) {
            validate(concept);
            return concept;
        } else {
            return tryPutConcept();
        }
    }

    @Nullable
    private Concept tryGetConcept() {
        if (has(ID)) {
            return executor.graph().getConcept(expect(ID));
        } else if (has(LABEL)) {
            return executor.graph().getOntologyConcept(expect(LABEL));
        } else {
            return null;
        }
    }

    private Concept tryPutConcept() {
        expectedParams.clear();

        Concept concept;

        if (has(SUPER_CONCEPT)) {
            concept = putOntologyConcept();
        } else if (has(TYPE)) {
            concept = putInstance();
        } else {
            throw GraqlQueryException.insertUndefinedVariable(executor.printableRepresentation(var));
        }

        // Check for any unexpected parameters
        params.forEach((param, value) -> {
            if (!expectedParams.contains(param)) {
                throw GraqlQueryException.insertUnexpectedProperty(param.name(), value, concept);
            }
        });

        return concept;
    }

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

    private ConceptBuilder(InsertQueryExecutor executor, Var var) {
        this.executor = executor;
        this.var = var;
    }

    private <T> T expectOrDefault(BuilderParam<T> param, @Nullable T defaultValue) {
        expectedParams.add(param);

        // This is safe, assuming we only add to the map with the `set` method
        //noinspection unchecked
        T value = (T) params.get(param);

        if (value == null) value = defaultValue;

        if (value == null) {
            throw GraqlQueryException.insertNoExpectedProperty(param.name(), executor.printableRepresentation(var));
        }

        return value;
    }

    /**
     * Called during {@link #build()} whenever a particular parameter is expected in order to build the {@link Concept}.
     * <p>
     *     This method will return the parameter, if present and also record that it was expected, so that we can later
     *     check for any unexpected properties.
     * </p>
     *
     * @throws GraqlQueryException if the parameter is not present
     */
    private <T> T expect(BuilderParam<T> param) {
        return expectOrDefault(param, null);
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

    /**
     * Check if this pre-existing concept conforms to all specified parameters
     *
     * @throws GraqlQueryException if any parameter does not match
     */
    private void validate(Concept concept) {
        validateParam(concept, TYPE, Thing.class, Thing::type);
        validateParam(concept, SUPER_CONCEPT, OntologyConcept.class, OntologyConcept::sup);
        validateParam(concept, LABEL, OntologyConcept.class, OntologyConcept::getLabel);
        validateParam(concept, ID, Concept.class, Concept::getId);
        validateParam(concept, VALUE, Resource.class, Resource::getValue);
        validateParam(concept, DATA_TYPE, ResourceType.class, ResourceType::getDataType);
        validateParam(concept, WHEN, Rule.class, Rule::getWhen);
        validateParam(concept, THEN, Rule.class, Rule::getThen);
    }

    /**
     * Check if the concept is of the given type and has a property that matches the given parameter.
     *
     * @throws GraqlQueryException if the concept does not satisfy the parameter
     */
    private <S extends Concept, T> void validateParam(
            Concept concept, BuilderParam<T> param, Class<S> conceptType, Function<S, T> getter) {

        if (has(param)) {
            T value = expect(param);

            boolean isInstance = conceptType.isInstance(concept);

            if (!isInstance || !Objects.equals(getter.apply(conceptType.cast(concept)), value)) {
                throw GraqlQueryException.insertPropertyOnExistingConcept(param.name(), value, concept);
            }
        }
    }

    private Thing putInstance() {
        Type type = expect(TYPE);

        if (type.isEntityType()) {
            return type.asEntityType().addEntity();
        } else if (type.isRelationType()) {
            return type.asRelationType().addRelation();
        } else if (type.isResourceType()) {
            return type.asResourceType().putResource(expect(VALUE));
        } else if (type.isRuleType()) {
            return type.asRuleType().putRule(expect(WHEN), expect(THEN));
        } else if (type.getLabel().equals(Schema.MetaSchema.THING.getLabel())) {
            throw GraqlQueryException.createInstanceOfMetaConcept(var, type);
        } else {
            throw CommonUtil.unreachableStatement("Can't recognize type " + type);
        }
    }

    private OntologyConcept putOntologyConcept() {
        OntologyConcept superConcept = expect(SUPER_CONCEPT);
        Label label = expect(LABEL);

        if (superConcept.isEntityType()) {
            return executor.graph().putEntityType(label).sup(superConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            return executor.graph().putRelationType(label).sup(superConcept.asRelationType());
        } else if (superConcept.isRole()) {
            return executor.graph().putRole(label).sup(superConcept.asRole());
        } else if (superConcept.isResourceType()) {
            ResourceType resourceType = superConcept.asResourceType();
            ResourceType.DataType<?> dataType = expectOrDefault(DATA_TYPE, resourceType.getDataType());
            return executor.graph().putResourceType(label, dataType).sup(resourceType);
        } else if (superConcept.isRuleType()) {
            return executor.graph().putRuleType(label).sup(superConcept.asRuleType());
        } else {
            throw GraqlQueryException.insertMetaType(label, superConcept);
        }
    }

}
