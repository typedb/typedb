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

package grakn.core.graql.query.pattern;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.common.util.CommonUtil;
import grakn.core.graql.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.Query.Char;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsAbstractProperty;
import grakn.core.graql.query.pattern.property.IsaExplicitProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.SubExplicitProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;
import grakn.core.graql.query.predicate.ValuePredicate;
import grakn.core.graql.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * A variable together with its properties in one Graql statement.
 * A Statement may be given a variable, or use an "anonymous" variable.
 * Graql provides static methods for constructing Statement objects.
 * The methods in Statement are used to set its properties. A Statement
 * behaves differently depending on the type of query its used in.
 * In a Match clause, a Statement describes the properties any matching
 * concept must have. In an InsertQuery, it describes the properties that
 * should be set on the inserted concept. In a DeleteQuery, it describes the
 * properties that should be deleted.
 */
public class Statement implements Pattern {

    private final Variable var;
    private final LinkedHashSet<VarProperty> properties;
    protected final Logger LOG = LoggerFactory.getLogger(Statement.class);
    private int hashCode = 0;

    public Statement(Variable var) {
        this(var, Collections.emptySet());
    }

    public Statement(Variable var, Set<VarProperty> properties) {
        this(var, new LinkedHashSet<>(properties));
    }

    public Statement(Variable var, LinkedHashSet<VarProperty> properties) {
        if (var == null) {
            throw new NullPointerException("Null var");
        }
        this.var = var;
        if (properties == null) {
            throw new NullPointerException("Null properties");
        }
        this.properties = properties;
    }

    /**
     * @return the variable name of this variable
     */
    @CheckReturnValue
    public Variable var() {
        return var;
    }

    public LinkedHashSet<VarProperty> properties() {
        return properties;
    }

    /**
     * @param id a ConceptId that this variable's ID must match
     * @return this
     */
    @CheckReturnValue
    public final Statement id(String id) {
        return addProperty(new IdProperty(id));
    }

    /**
     * @param label a string that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    public final Statement label(String label) {
        return label(Label.of(label));
    }

    /**
     * @param label a type label that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    public final Statement label(Label label) {
        return addProperty(new LabelProperty(label));
    }

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    @CheckReturnValue
    public final Statement val(Object value) {
        return val(Graql.eq(value));
    }

    /**
     * @param predicate a atom this variable's value must match
     * @return this
     */
    @CheckReturnValue
    public final Statement val(ValuePredicate predicate) {
        return addProperty(new ValueProperty(predicate));
    }

    /**
     * the variable must have a resource of the given type with an exact matching value
     *
     * @param type  a resource type in the schema
     * @param value a value of a resource
     * @return this
     */
    @CheckReturnValue
    public final Statement has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    @CheckReturnValue
    public final Statement has(String type, Object value, Statement relation) {
        return has(type, Graql.eq(value), relation);
    }

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param predicate a atom on the value of a resource
     * @return this
     */
    @CheckReturnValue
    public final Statement has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().val(predicate));
    }

    @CheckReturnValue
    public final Statement has(String type, ValuePredicate predicate, Statement relation) {
        return has(type, Graql.var().val(predicate), relation);
    }

    /**
     * the variable must have an Attribute of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param attribute a variable pattern representing an Attribute
     * @return this
     */
    @CheckReturnValue
    public final Statement has(String type, Statement attribute) {
        return addProperty(new HasAttributeProperty(type, attribute));
    }

    /**
     * the variable must have an Attribute of the given type that matches attribute.
     * The Relationship associating the two must match relation.
     *
     * @param type         a resource type in the ontology
     * @param attribute    a variable pattern representing an Attribute
     * @param relationship a variable pattern representing a Relationship
     * @return this
     */
    @CheckReturnValue
    public final Statement has(String type, Statement attribute, Statement relationship) {
        return addProperty(new HasAttributeProperty(type, attribute, relationship));
    }

    /**
     * @param type a concept type id that the variable must be of this type directly
     * @return this
     */
    @CheckReturnValue
    public final Statement isaExplicit(String type) {
        return isaExplicit(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly
     * @return this
     */
    @CheckReturnValue
    public final Statement isaExplicit(Statement type) {
        return addProperty(new IsaExplicitProperty(type));
    }

    /**
     * @param type a concept type id that the variable must be of this type directly or indirectly
     * @return this
     */
    @CheckReturnValue
    public final Statement isa(String type) {
        return isa(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly or indirectly
     * @return this
     */
    @CheckReturnValue
    public final Statement isa(Statement type) {
        return addProperty(new IsaProperty(type));
    }

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    public final Statement sub(String type) {
        return sub(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    public final Statement sub(Statement type) {
        return addProperty(new SubProperty(type));
    }

    /**
     * @param type a concept type id that this variable must be a kind of, without looking at parent types
     * @return this
     */
    @CheckReturnValue
    public final Statement subExplicit(String type) {
        return subExplicit(Graql.label(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of, without looking at parent type
     * @return this
     */
    @CheckReturnValue
    public final Statement subExplicit(Statement type) {
        return addProperty(new SubExplicitProperty(type));
    }

    /**
     * @param type a Role id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public final Statement relates(String type) {
        return relates(type, null);
    }

    /**
     * @param type a Role that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public final Statement relates(Statement type) {
        return relates(type, null);
    }

    /**
     * @param roleType a Role id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public Statement relates(String roleType, @Nullable String superRoleType) {
        return relates(Graql.label(roleType), superRoleType == null ? null : Graql.label(superRoleType));
    }

    /**
     * @param roleType a Role that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public Statement relates(Statement roleType, @Nullable Statement superRoleType) {
        return addProperty(new RelatesProperty(roleType, superRoleType));
    }

    /**
     * @param type a Role id that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    public final Statement plays(String type) {
        return plays(Graql.label(type));
    }

    /**
     * @param type a Role that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    public final Statement plays(Statement type) {
        return addProperty(new PlaysProperty(type, false));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    public final Statement has(String type) {
        return has(Graql.label(type));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    public final Statement has(Statement type) {
        return addProperty(new HasAttributeTypeProperty(type, false));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    public final Statement key(String type) {
        return key(Graql.var().label(type));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    public final Statement key(Statement type) {
        return addProperty(new HasAttributeTypeProperty(type, true));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final Statement rel(String player) {
        return rel(Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final Statement rel(Statement roleplayer) {
        return addRolePlayer(new RelationProperty.RolePlayer(null, roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a Role in the schema
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final Statement rel(String role, String roleplayer) {
        return rel(Graql.label(role), Graql.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a variable pattern representing a Role
     * @param roleplayer a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final Statement rel(Statement role, String roleplayer) {
        return rel(role, Graql.var(roleplayer));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a Role in the schema
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final Statement rel(String role, Statement roleplayer) {
        return rel(Graql.label(role), roleplayer);
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a variable pattern representing a Role
     * @param roleplayer a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public final Statement rel(Statement role, Statement roleplayer) {
        return addRolePlayer(new RelationProperty.RolePlayer(role, roleplayer));
    }

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     *
     * @return this
     */
    @CheckReturnValue
    public final Statement isAbstract() {
        return addProperty(IsAbstractProperty.get());
    }

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    public final Statement datatype(Query.DataType datatype) {
        return addProperty(new DataTypeProperty(datatype));
    }

    /**
     * Specify the regular expression instances of this resource type must match
     *
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    public final Statement like(String regex) {
        return addProperty(new RegexProperty(regex));
    }

    /**
     * @param when the left-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    public final Statement when(Pattern when) {
        return addProperty(new WhenProperty(when));
    }

    /**
     * @param then the right-hand side of this rule
     * @return this
     */
    @CheckReturnValue
    public final Statement then(Pattern then) {
        return addProperty(new ThenProperty(then));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param var the variable that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public final Statement neq(String var) {
        return neq(Graql.var(var));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param statement the variable pattern that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public final Statement neq(Statement statement) {
        return addProperty(new NeqProperty(statement));
    }

    @Override
    public final Set<Variable> variables() {
        return innerStatements().stream()
                .filter(v -> v.var().isUserDefinedName())
                .map(statement -> statement.var())
                .collect(toSet());
    }

    /**
     * @return the name this variable represents, if it represents something with a specific name
     */
    @CheckReturnValue
    public final Optional<Label> getTypeLabel() {
        return getProperty(LabelProperty.class).map(labelProperty -> labelProperty.label());
    }

    /**
     * @return all type names that this variable refers to
     */
    @CheckReturnValue
    public final Set<Label> getTypeLabels() {
        return properties().stream()
                .flatMap(varProperty -> varProperty.types())
                .map(statement -> statement.getTypeLabel())
                .flatMap(optional -> CommonUtil.optionalToStream(optional))
                .collect(toSet());
    }

    /**
     * Get a stream of all properties of a particular type on this variable
     *
     * @param type the class of VarProperty to return
     * @param <T>  the type of VarProperty to return
     */
    @CheckReturnValue
    public final <T extends VarProperty> Stream<T> getProperties(Class<T> type) {
        return properties().stream().filter(type::isInstance).map(type::cast);
    }

    /**
     * Get a unique property of a particular type on this variable, if it exists
     *
     * @param type the class of VarProperty to return
     * @param <T>  the type of VarProperty to return
     */
    @CheckReturnValue
    public final <T extends VarProperty> Optional<T> getProperty(Class<T> type) {
        return properties().stream().filter(type::isInstance).map(type::cast).findFirst();
    }

    /**
     * Get whether this Statement} has a {@link VarProperty of the given type
     *
     * @param type the type of the VarProperty
     * @param <T>  the type of the VarProperty
     * @return whether this Statement} has a {@link VarProperty of the given type
     */
    @CheckReturnValue
    public final <T extends VarProperty> boolean hasProperty(Class<T> type) {
        return getProperties(type).findAny().isPresent();
    }

    /**
     * @return all variables that this variable references
     */
    @CheckReturnValue
    public final Collection<Statement> innerStatements() {
        Stack<Statement> statementStack = new Stack<>();
        List<Statement> statements = new ArrayList<>();

        statementStack.add(this);

        while (!statementStack.isEmpty()) {
            Statement statement = statementStack.pop();
            statements.add(statement);
            statement.properties().stream()
                    .flatMap(property -> property.innerStatements())
                    .forEach(s -> statementStack.add(s));
        }

        return statements;
    }

    /**
     * Get all inner variables, including implicit variables such as in a has property
     */
    @CheckReturnValue
    public final Collection<Statement> implicitInnerStatements() {
        Stack<Statement> statementStack = new Stack<>();
        List<Statement> statements = new ArrayList<>();

        statementStack.add(this);

        while (!statementStack.isEmpty()) {
            Statement var = statementStack.pop();
            statements.add(var);
            var.properties().stream()
                    .flatMap(property -> property.implicitInnerStatements())
                    .forEach(s -> statementStack.add(s));
        }

        return statements;
    }

    @Override
    public final Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        // a disjunction containing only one option
        Conjunction<Statement> conjunction = Graql.and(Collections.singleton(this));
        return Graql.or(Collections.singleton(conjunction));
    }

    @CheckReturnValue
    private Statement addRolePlayer(RelationProperty.RolePlayer rolePlayer) {
        Optional<RelationProperty> relationProperty = getProperty(RelationProperty.class);

        LinkedHashSet<RelationProperty.RolePlayer> oldRolePlayers = relationProperty
                .map(RelationProperty::relationPlayers)
                .orElse(new LinkedHashSet<>());

        LinkedHashSet<RelationProperty.RolePlayer> newRolePlayers = Stream
                .concat(oldRolePlayers.stream(), Stream.of(rolePlayer))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        RelationProperty newProperty = new RelationProperty(newRolePlayers);

        return relationProperty.map(this::removeProperty).orElse(this).addProperty(newProperty);
    }

    @CheckReturnValue
    public Statement addProperty(VarProperty property) {
        if (property.isUnique()) {
            getProperty(property.getClass()).filter(other -> !other.equals(property)).ifPresent(other -> {
                throw GraqlQueryException.conflictingProperties(this, property, other);
            });
        }
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(properties());
        newProperties.add(property);
        return new Statement(var(), newProperties);
    }

    @CheckReturnValue
    private Statement removeProperty(VarProperty property) {
        return new Statement(var(), Sets.difference(properties(), ImmutableSet.of(property)));
    }

    /**
     * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
     */
    @CheckReturnValue
    public final String getPrintableName() {
        if (properties().size() == 0) {
            // If there are no properties, we display the variable name
            return var().toString();
        } else if (properties().size() == 1) {
            // If there is only a label, we display that
            Optional<Label> label = getTypeLabel();
            if (label.isPresent()) {
                if (label.get().getValue().contains(" ")) {
                    return StringUtil.typeLabelToString(label.get());
                } else {
                    return label.get().getValue();
                }
            }
        }

        // Otherwise, we print the entire pattern
        return toString();
    }

    @Override
    public final String toString() {
        Collection<Statement> innerStatements = innerStatements();
        innerStatements.remove(this);

        // TODO: We seem to be removing implicit attribute relation. Encapsulate this in HasAttributeProperty.toString()
        getProperties(HasAttributeProperty.class)
                .map(property -> property.attribute())
                .flatMap(attribute -> attribute.innerStatements().stream())
                .forEach(statement -> innerStatements.remove(statement));

        if (innerStatements.stream()
                .anyMatch(statement -> statement.properties().stream()
                        .anyMatch(p -> !(p instanceof LabelProperty)))) {
            LOG.warn("printing a query with inner variables, which is not supported in native Graql");
        }

        StringBuilder statement = new StringBuilder();

        if (var.isUserDefinedName()) {
            statement.append(var);

            if (!properties.isEmpty()) {
                statement.append(Char.SPACE);
            }
        }

        statement.append(properties.stream()
                                 .map(VarProperty::toString)
                                 .collect(joining(Char.COMMA_SPACE.toString())));
        statement.append(Char.SEMICOLON);
        return statement.toString();
    }

    @Override
    public final boolean equals(Object o) {
        // This equals implementation is special: it considers all non-user-defined vars as equivalent
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Statement other = (Statement) o;

        if (var().isUserDefinedName() != other.var().isUserDefinedName()) return false;

        // "simplifying" this makes it harder to read
        //noinspection SimplifiableIfStatement
        if (!properties().equals(other.properties())) return false;

        return !var().isUserDefinedName() || var().equals(other.var());

    }

    @Override
    public final int hashCode() {
        if (hashCode == 0) {
            // This hashCode implementation is special: it considers all non-user-defined vars as equivalent
            hashCode = properties().hashCode();
            if (var().isUserDefinedName()) hashCode = 31 * hashCode + var().hashCode();
            hashCode = 31 * hashCode + (var().isUserDefinedName() ? 1 : 0);
        }
        return hashCode;
    }
}
