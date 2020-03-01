/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graql.executor;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlQueryException;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.executor.ConceptBuilder;
import grakn.core.kb.graql.executor.WriteExecutor;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class for building a Concept, by providing properties.
 * A VarProperty is responsible for inserting itself into the graph. However,
 * some properties can only operate in combination. For example, to create a Attribute you need both
 * an IsaProperty and a ValueProperty.
 * Therefore, these properties do not create the Concept themselves.
 * instead they provide the necessary information to the ConceptBuilder, which will create the
 * Concept at a later time:
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
 */
public class ConceptBuilderImpl implements ConceptBuilder {

    private final ConceptManager conceptManager;
    private final WriteExecutor writeExecutor;

    private final Variable var;

    /**
     * A map of parameters that have been specified for this concept.
     */
    private final Map<BuilderParam<?>, Object> preProvidedParams = new HashMap<>();

    /**
     * A set of parameters that were used for building the concept. Modified while executing #build().
     * This set starts empty. Every time #use(BuilderParam) or #useOrDefault(BuilderParam, Object)
     * is called, the parameter is added to this set. After the concept is built, any parameter not in this set is
     * considered "unexpected". If it is present in the field #preProvidedParams, then an error is thrown.
     * Simplified example of how this operates:
     * <pre>
     * // preProvidedParams = {LABEL: actor, SUPER_CONCEPT: role, VALUE: "Bob"}
     * // usedParams = {}
     *
     * if (has(LABEL)) {
     *      Label label = expect(LABEL);                          // usedParams = {LABEL}
     *      // Retrieve SUPER_CONCEPT and adds it to usedParams
     *      SchemaConcept superConcept = expect(SUPER_CONCEPT); // usedParams = {LABEL, SUPER_CONCEPT}
     *      return graph.putEntityType(label).sup(superConcept.asRole());
     * }
     *
     * // Check for any unexpected parameters
     * preProvidedParams.forEach((providedParam, value) -> {
     *     if (!usedParams.contains(providedParam)) {
     *         // providedParam = VALUE
     *         // Throws because VALUE was provided, but not used!
     *     }
     * });
     * </pre>
     */
    private final Set<BuilderParam<?>> usedParams = new HashSet<>();

    @Override
    public ConceptBuilder isa(Type type) {
        return set(TYPE, type);
    }

    @Override
    public ConceptBuilder sub(SchemaConcept superConcept) {
        return set(SUPER_CONCEPT, superConcept);
    }

    @Override
    public ConceptBuilder isRole() {
        return set(IS_ROLE, Unit.INSTANCE);
    }

    @Override
    public ConceptBuilder isRule() {
        return set(IS_RULE, Unit.INSTANCE);
    }

    @Override
    public ConceptBuilder label(Label label) {
        return set(LABEL, label);
    }

    @Override
    public ConceptBuilder id(ConceptId id) {
        return set(ID, id);
    }

    @Override
    public ConceptBuilder value(Object value) {
        return set(VALUE, value);
    }

    @Override
    public ConceptBuilder dataType(AttributeType.DataType<?> dataType) {
        return set(DATA_TYPE, dataType);
    }

    @Override
    public ConceptBuilder when(Pattern when) {
        return set(WHEN, when);
    }

    @Override
    public ConceptBuilder then(Pattern then) {
        return set(THEN, then);
    }

    static ConceptBuilder of(ConceptManager conceptManager, WriteExecutor writeExecutor, Variable var) {
        return new ConceptBuilderImpl(conceptManager, writeExecutor, var);
    }
    /**
     * Build the Concept and return it, using the properties given.
     *
     * @throws GraqlSemanticException if the properties provided are inconsistent
     */
    @Override
    public Concept build() {

        // If a label or ID is provided, attempt to `get` the concept
        Concept concept = tryGetConcept();

        if (concept != null) {
            return concept;
        } else {
            return tryPutConcept();
        }
    }

    @Nullable
    private Concept tryGetConcept() {
        Concept concept = null;

        if (has(ID)) {
            concept = conceptManager.getConcept(use(ID));

            if (has(LABEL)) {
                concept.asSchemaConcept().label(use(LABEL));
            }
        } else if (has(LABEL)) {
            concept = conceptManager.getSchemaConcept(use(LABEL));
        }

        if (concept != null) {
            // The super can be changed on an existing concept
            if (has(SUPER_CONCEPT)) {
                SchemaConcept superConcept = use(SUPER_CONCEPT);
                setSuper(concept.asSchemaConcept(), superConcept);
            }

            validate(concept);
        }

        return concept;
    }

    private Concept tryPutConcept() {
        usedParams.clear();

        Concept concept;

        if (has(IS_ROLE)) {
            use(IS_ROLE);

            Label label = use(LABEL);
            SchemaConcept schemaConcept = conceptManager.getSchemaConcept(label);
            Role role;
            if (schemaConcept != null && !schemaConcept.isRole()) {
                // is already assigned to a different type
                // TODO better exception
                throw new RuntimeException();
            } else if (schemaConcept == null) {
                role = conceptManager.createRole(label, conceptManager.getMetaRole());
            } else {
                role = schemaConcept.asRole();
            }

            if (has(SUPER_CONCEPT)) {
                setSuper(role, use(SUPER_CONCEPT));
            }

            concept = role;
        } else if (has(IS_RULE)) {
            use(IS_RULE);

            // PUT behavior on rule
            Label label = use(LABEL);
            Pattern when = use(WHEN);
            Pattern then = use(THEN);
            Rule rule = putSchemaConcept(label, () -> conceptManager.createRule(label, when, then, conceptManager.getMetaRule()), Rule.class);

            // TODO we may have to update the rule cache here

            if (has(SUPER_CONCEPT)) {
                setSuper(rule, use(SUPER_CONCEPT));
            }

            concept = rule;
        } else if (has(SUPER_CONCEPT)) {
            concept = putSchemaConcept();
        } else if (has(TYPE)) {
            concept = putInstance();
        } else {
            throw GraqlSemanticException.insertUndefinedVariable(writeExecutor.printableRepresentation(var));
        }

        // Check for any unexpected parameters
        preProvidedParams.forEach((param, value) -> {
            if (!usedParams.contains(param)) {
                throw GraqlSemanticException.insertUnexpectedProperty(param.name(), value, concept);
            }
        });

        return concept;
    }

    private static final BuilderParam<Type> TYPE = BuilderParam.of(Graql.Token.Property.ISA);
    private static final BuilderParam<SchemaConcept> SUPER_CONCEPT = BuilderParam.of(Graql.Token.Property.SUB);
    private static final BuilderParam<Label> LABEL = BuilderParam.of(Graql.Token.Property.TYPE);
    private static final BuilderParam<ConceptId> ID = BuilderParam.of(Graql.Token.Property.ID);
    private static final BuilderParam<Object> VALUE = BuilderParam.of(Graql.Token.Property.VALUE);
    private static final BuilderParam<AttributeType.DataType<?>> DATA_TYPE = BuilderParam.of(Graql.Token.Property.DATA_TYPE);
    private static final BuilderParam<Pattern> WHEN = BuilderParam.of(Graql.Token.Property.WHEN);
    private static final BuilderParam<Pattern> THEN = BuilderParam.of(Graql.Token.Property.THEN);
    private static final BuilderParam<Unit> IS_ROLE = BuilderParam.of("role"); // TODO: replace this with a value registered in an enum
    private static final BuilderParam<Unit> IS_RULE = BuilderParam.of("rule"); // TODO: replace this with a value registered in an enum

    private ConceptBuilderImpl(ConceptManager conceptManager, WriteExecutor writeExecutor, Variable var) {
        this.conceptManager = conceptManager;
        this.writeExecutor = writeExecutor;
        this.var = var;
    }

    private <T> T useOrDefault(BuilderParam<T> param, @Nullable T defaultValue) {
        usedParams.add(param);

        // This is safe, assuming we only add to the map with the `set` method
        //noinspection unchecked
        T value = (T) preProvidedParams.get(param);

        if (value == null) value = defaultValue;

        if (value == null) {
            Statement owner = writeExecutor.printableRepresentation(var);
            throw GraqlSemanticException.insertNoExpectedProperty(param.name(), owner);
        }

        return value;
    }

    /**
     * Called during #build() whenever a particular parameter is expected in order to build the Concept.
     * This method will return the parameter, if present and also record that it was expected, so that we can later
     * check for any unexpected properties.
     *
     * @throws GraqlSemanticException if the parameter is not present
     */
    private <T> T use(BuilderParam<T> param) {
        return useOrDefault(param, null);
    }

    private boolean has(BuilderParam<?> param) {
        return preProvidedParams.containsKey(param);
    }

    private <T> ConceptBuilder set(BuilderParam<T> param, T value) {
        if (preProvidedParams.containsKey(param) && !preProvidedParams.get(param).equals(value)) {
            Statement varPattern = writeExecutor.printableRepresentation(var);
            Object otherValue = preProvidedParams.get(param);
            throw GraqlSemanticException.insertMultipleProperties(varPattern, param.name(), value, otherValue);
        }
        preProvidedParams.put(param, checkNotNull(value));
        return this;
    }

    /**
     * Check if this pre-existing concept conforms to all specified parameters
     *
     * @throws GraqlSemanticException if any parameter does not match
     */
    private void validate(Concept concept) {
        validateParam(concept, TYPE, Thing.class, Thing::type);
        validateParam(concept, SUPER_CONCEPT, SchemaConcept.class, SchemaConcept::sup);
        validateParam(concept, LABEL, SchemaConcept.class, SchemaConcept::label);
        validateParam(concept, ID, Concept.class, Concept::id);
        validateParam(concept, VALUE, Attribute.class, Attribute::value);
        validateParam(concept, DATA_TYPE, AttributeType.class, AttributeType::dataType);
        validateParam(concept, WHEN, Rule.class, Rule::when);
        validateParam(concept, THEN, Rule.class, Rule::then);
    }

    /**
     * Check if the concept is of the given type and has a property that matches the given parameter.
     *
     * @throws GraqlSemanticException if the concept does not satisfy the parameter
     */
    private <S extends Concept, T> void validateParam(
            Concept concept, BuilderParam<T> param, Class<S> conceptType, Function<S, T> getter) {

        if (has(param)) {
            T value = use(param);

            boolean isInstance = conceptType.isInstance(concept);

            if (!isInstance || !Objects.equals(getter.apply(conceptType.cast(concept)), value)) {
                throw GraqlSemanticException.insertPropertyOnExistingConcept(param.name(), value, concept);
            }
        }
    }

    private Thing putInstance() {
        Type type = use(TYPE);

        if (type.isEntityType()) {
            return type.asEntityType().create();
        } else if (type.isRelationType()) {
            return type.asRelationType().create();
        } else if (type.isAttributeType()) {
            return type.asAttributeType().create(use(VALUE));
        } else if (type.label().equals(Schema.MetaSchema.THING.getLabel())) {
            throw GraqlSemanticException.createInstanceOfMetaConcept(var, type);
        } else {
            throw GraqlQueryException.unreachableStatement("Can't recognize type " + type);
        }
    }

    private SchemaConcept putSchemaConcept() {
        SchemaConcept superConcept = use(SUPER_CONCEPT);
        Label label = use(LABEL);

        SchemaConcept concept;

        if (superConcept.isEntityType()) {
            concept = putSchemaConcept(label, () -> conceptManager.createEntityType(label, superConcept.asEntityType()), EntityType.class);
        } else if (superConcept.isRelationType()) {
            concept = putSchemaConcept(label, () -> conceptManager.createRelationType(label, superConcept.asRelationType()), RelationType.class);
        } else if (superConcept.isRole()) {
            concept = putSchemaConcept(label, () -> conceptManager.createRole(label, superConcept.asRole()), Role.class);
        } else if (superConcept.isAttributeType()) {
            AttributeType attributeType = superConcept.asAttributeType();
            AttributeType.DataType<?> dataType = useOrDefault(DATA_TYPE, attributeType.dataType());
            concept = putSchemaConcept(label, () -> conceptManager.createAttributeType(label, superConcept.asAttributeType(), dataType), AttributeType.class);
        } else if (superConcept.isRule()) {
            concept = putSchemaConcept(label, () -> conceptManager.createRule(label, use(WHEN), use(THEN), superConcept.asRule()), Rule.class);
        } else {
            throw GraknConceptException.invalidSuperType(label, superConcept);
        }

        setSuper(concept, superConcept);

        return concept;
    }

    private <T extends SchemaConcept> T putSchemaConcept(Label label, Supplier<T> create, Class<T> expectedClass) {
        SchemaConcept schemaConcept = conceptManager.getSchemaConcept(label);
        if (schemaConcept != null && !expectedClass.isInstance(schemaConcept)) {
            // TODO better exception
            throw new RuntimeException();
        } else if (schemaConcept == null) {
            return create.get();
        } else {
            return expectedClass.cast(schemaConcept);
        }
    }

    @Override
    public String toString() {
        return "ConceptBuilder{" +
                "var=" + var +
                ", preProvidedParams=" + preProvidedParams +
                ", usedParams=" + usedParams +
                '}';
    }


    /**
     * Make the second argument the super of the first argument
     *
     * @throws GraqlSemanticException if the types are different, or setting the super to be a meta-type
     */
    static void setSuper(SchemaConcept subConcept, SchemaConcept superConcept) {
        if (superConcept.isEntityType()) {
            subConcept.asEntityType().sup(superConcept.asEntityType());
        } else if (superConcept.isRelationType()) {
            subConcept.asRelationType().sup(superConcept.asRelationType());
        } else if (superConcept.isRole()) {
            subConcept.asRole().sup(superConcept.asRole());
        } else if (superConcept.isAttributeType()) {
            subConcept.asAttributeType().sup(superConcept.asAttributeType());
        } else if (superConcept.isRule()) {
            subConcept.asRule().sup(superConcept.asRule());
        } else {
            throw GraknConceptException.invalidSuperType(subConcept.label(), superConcept);
        }
    }

    /**
     * Describes a parameter that can be set on a ConceptBuilder.
     * We could instead just represent these parameters as fields of ConceptBuilder. Instead, we use a
     * {@code Map<BuilderParam<?>, Object>}. This allows us to do clever stuff like iterate over the parameters,
     * or check for unexpected parameters without lots of boilerplate.
     */
    // The generic is technically unused, but is useful to constrain the values of the parameter
    @SuppressWarnings("unused")
    final static class BuilderParam<T> {
        private final String name;

        BuilderParam(String name) {
            this.name = name;
        }

        String name() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }

        static <T> BuilderParam of(Object obj) {
            return of(obj.toString());
        }

        static <T> BuilderParam of(String name) {
            return new BuilderParam(name);
        }
    }

    /**
     * Class with no fields and exactly one instance.
     * Similar in use to Void, but the single instance is Unit#INSTANCE instead of {@code null}. Useful
     * when {@code null} is not allowed.
     * see <a href=https://en.wikipedia.org/wiki/Unit_type>Wikipedia</a>
     */
    final static class Unit {
        private Unit() {
        }

        static Unit INSTANCE = new Unit();

        @Override
        public String toString() {
            return "";
        }
    }
}
