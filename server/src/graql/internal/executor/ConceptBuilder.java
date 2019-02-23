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

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.server.kb.Schema;
import grakn.core.server.exception.InvalidKBException;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;
import graql.lang.property.IsaProperty;
import graql.lang.property.ValueProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

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
 * A {@link VarProperty} is responsible for inserting itself into the graph. However,
 * some properties can only operate in combination. For example, to create a {@link Attribute} you need both
 * an {@link IsaProperty} and a {@link ValueProperty}.
 * Therefore, these properties do not create the {@link Concept} themselves.
 * instead they provide the necessary information to the {@link ConceptBuilder}, which will create the
 * {@link Concept} at a later time:
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
public class ConceptBuilder {

    private final WriteExecutor executor;

    private final Variable var;

    /**
     * A map of parameters that have been specified for this concept.
     */
    private final Map<BuilderParam<?>, Object> preProvidedParams = new HashMap<>();

    /**
     * A set of parameters that were used for building the concept. Modified while executing {@link #build()}.
     * This set starts empty. Every time {@link #use(BuilderParam)} or {@link #useOrDefault(BuilderParam, Object)}
     * is called, the parameter is added to this set. After the concept is built, any parameter not in this set is
     * considered "unexpected". If it is present in the field {@link #preProvidedParams}, then an error is thrown.
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

    public ConceptBuilder isa(Type type) {
        return set(TYPE, type);
    }

    public ConceptBuilder sub(SchemaConcept superConcept) {
        return set(SUPER_CONCEPT, superConcept);
    }

    public ConceptBuilder isRole() {
        return set(IS_ROLE, Unit.INSTANCE);
    }

    public ConceptBuilder isRule() {
        return set(IS_RULE, Unit.INSTANCE);
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

    public ConceptBuilder dataType(AttributeType.DataType<?> dataType) {
        return set(DATA_TYPE, dataType);
    }

    public ConceptBuilder when(Pattern when) {
        return set(WHEN, when);
    }

    public ConceptBuilder then(Pattern then) {
        return set(THEN, then);
    }

    static ConceptBuilder of(WriteExecutor executor, Variable var) {
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
            return concept;
        } else {
            return tryPutConcept();
        }
    }

    @Nullable
    private Concept tryGetConcept() {
        Concept concept = null;

        if (has(ID)) {
            concept = executor.tx().getConcept(use(ID));

            if (has(LABEL)) {
                concept.asSchemaConcept().label(use(LABEL));
            }
        } else if (has(LABEL)) {
            concept = executor.tx().getSchemaConcept(use(LABEL));
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
            Role role = executor.tx().putRole(label);

            if (has(SUPER_CONCEPT)) {
                setSuper(role, use(SUPER_CONCEPT));
            }

            concept = role;
        } else if (has(IS_RULE)) {
            use(IS_RULE);

            Label label = use(LABEL);
            Pattern when = use(WHEN);
            Pattern then = use(THEN);
            Rule rule = executor.tx().putRule(label, when, then);

            if (has(SUPER_CONCEPT)) {
                setSuper(rule, use(SUPER_CONCEPT));
            }

            concept = rule;
        } else if (has(SUPER_CONCEPT)) {
            concept = putSchemaConcept();
        } else if (has(TYPE)) {
            concept = putInstance();
        } else {
            throw GraqlQueryException.insertUndefinedVariable(executor.printableRepresentation(var));
        }

        // Check for any unexpected parameters
        preProvidedParams.forEach((param, value) -> {
            if (!usedParams.contains(param)) {
                throw GraqlQueryException.insertUnexpectedProperty(param.name(), value, concept);
            }
        });

        return concept;
    }

    /**
     * Describes a parameter that can be set on a {@link ConceptBuilder}.
     * We could instead just represent these parameters as fields of {@link ConceptBuilder}. Instead, we use a
     * {@code Map<BuilderParam<?>, Object>}. This allows us to do clever stuff like iterate over the parameters,
     * or check for unexpected parameters without lots of boilerplate.
     */
    // The generic is technically unused, but is useful to constrain the values of the parameter
    @SuppressWarnings("unused")
    private static final class BuilderParam<T> {
        private final String name;

        private BuilderParam(String name) {
            this.name = name;
        }

        String name() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }

        static <T> BuilderParam<T> of(Object obj) {
            return of(obj.toString());
        }

        static <T> BuilderParam<T> of(String name) {
            return new BuilderParam<>(name);
        }
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

    /**
     * Class with no fields and exactly one instance.
     * Similar in use to {@link Void}, but the single instance is {@link Unit#INSTANCE} instead of {@code null}. Useful
     * when {@code null} is not allowed.
     * @see <a href=https://en.wikipedia.org/wiki/Unit_type>Wikipedia</a>
     */
    private static final class Unit {
        private Unit() {
        }

        private static Unit INSTANCE = new Unit();

        @Override
        public String toString() {
            return "";
        }
    }

    private ConceptBuilder(WriteExecutor executor, Variable var) {
        this.executor = executor;
        this.var = var;
    }

    private <T> T useOrDefault(BuilderParam<T> param, @Nullable T defaultValue) {
        usedParams.add(param);

        // This is safe, assuming we only add to the map with the `set` method
        //noinspection unchecked
        T value = (T) preProvidedParams.get(param);

        if (value == null) value = defaultValue;

        if (value == null) {
            Statement owner = executor.printableRepresentation(var);
            throw GraqlQueryException.insertNoExpectedProperty(param.name(), owner);
        }

        return value;
    }

    /**
     * Called during {@link #build()} whenever a particular parameter is expected in order to build the {@link Concept}.
     * This method will return the parameter, if present and also record that it was expected, so that we can later
     * check for any unexpected properties.
     *
     * @throws GraqlQueryException if the parameter is not present
     */
    private <T> T use(BuilderParam<T> param) {
        return useOrDefault(param, null);
    }

    private boolean has(BuilderParam<?> param) {
        return preProvidedParams.containsKey(param);
    }

    private <T> ConceptBuilder set(BuilderParam<T> param, T value) {
        if (preProvidedParams.containsKey(param) && !preProvidedParams.get(param).equals(value)) {
            Statement varPattern = executor.printableRepresentation(var);
            Object otherValue = preProvidedParams.get(param);
            throw GraqlQueryException.insertMultipleProperties(varPattern, param.name(), value, otherValue);
        }
        preProvidedParams.put(param, checkNotNull(value));
        return this;
    }

    /**
     * Check if this pre-existing concept conforms to all specified parameters
     *
     * @throws GraqlQueryException if any parameter does not match
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
     * @throws GraqlQueryException if the concept does not satisfy the parameter
     */
    private <S extends Concept, T> void validateParam(
            Concept concept, BuilderParam<T> param, Class<S> conceptType, Function<S, T> getter) {

        if (has(param)) {
            T value = use(param);

            boolean isInstance = conceptType.isInstance(concept);

            if (!isInstance || !Objects.equals(getter.apply(conceptType.cast(concept)), value)) {
                throw GraqlQueryException.insertPropertyOnExistingConcept(param.name(), value, concept);
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
            throw GraqlQueryException.createInstanceOfMetaConcept(var, type);
        } else {
            throw CommonUtil.unreachableStatement("Can't recognize type " + type);
        }
    }

    private SchemaConcept putSchemaConcept() {
        SchemaConcept superConcept = use(SUPER_CONCEPT);
        Label label = use(LABEL);

        SchemaConcept concept;

        if (superConcept.isEntityType()) {
            concept = executor.tx().putEntityType(label);
        } else if (superConcept.isRelationType()) {
            concept = executor.tx().putRelationType(label);
        } else if (superConcept.isRole()) {
            concept = executor.tx().putRole(label);
        } else if (superConcept.isAttributeType()) {
            AttributeType attributeType = superConcept.asAttributeType();
            AttributeType.DataType<?> dataType = useOrDefault(DATA_TYPE, attributeType.dataType());
            concept = executor.tx().putAttributeType(label, dataType);
        } else if (superConcept.isRule()) {
            concept = executor.tx().putRule(label, use(WHEN), use(THEN));
        } else {
            throw InvalidKBException.insertMetaType(label, superConcept);
        }

        setSuper(concept, superConcept);

        return concept;
    }

    /**
     * Make the second argument the super of the first argument
     *
     * @throws GraqlQueryException if the types are different, or setting the super to be a meta-type
     */
    public static void setSuper(SchemaConcept subConcept, SchemaConcept superConcept) {
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
            throw InvalidKBException.insertMetaType(subConcept.label(), superConcept);
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
}
