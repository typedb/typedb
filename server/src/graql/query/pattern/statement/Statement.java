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

package grakn.core.graql.query.pattern.statement;

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.Query;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Disjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.property.AbstractProperty;
import grakn.core.graql.query.pattern.property.DataTypeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeProperty;
import grakn.core.graql.query.pattern.property.HasAttributeTypeProperty;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.IsaProperty;
import grakn.core.graql.query.pattern.property.NeqProperty;
import grakn.core.graql.query.pattern.property.PlaysProperty;
import grakn.core.graql.query.pattern.property.RegexProperty;
import grakn.core.graql.query.pattern.property.RelatesProperty;
import grakn.core.graql.query.pattern.property.RelationProperty;
import grakn.core.graql.query.pattern.property.SubExplicitProperty;
import grakn.core.graql.query.pattern.property.SubProperty;
import grakn.core.graql.query.pattern.property.ThenProperty;
import grakn.core.graql.query.pattern.property.TypeProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.graql.query.pattern.property.WhenProperty;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementAttribute;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementRelation;
import grakn.core.graql.query.pattern.statement.StatementInstance.StatementThing;
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
import java.util.stream.Stream;

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

    protected final Logger LOG = LoggerFactory.getLogger(Statement.class);
    private final Variable var;
    private final LinkedHashSet<VarProperty> properties;
    private int hashCode = 0;

    public Statement(Variable var) {
        this(var, new LinkedHashSet<>());
    }

    public Statement(Variable var, List<VarProperty> properties) {
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

    // TODO: This may not be needed once we have strict typing for statements
    public static Statement create(Variable var, LinkedHashSet<VarProperty> properties) {
        Set<Class> statementClasses = properties.stream().map(VarProperty::statementClass).collect(toSet());

        if (statementClasses.size() > 2) {
            throw new IllegalStateException("Not allowed to mix Properties for various Type/Instance statements");

        } else if (statementClasses.size() == 2) {
            if (statementClasses.contains(StatementType.class)) {
                throw new IllegalStateException("Not allowed to mix Properties for Type and Instance statements");
            } else if (statementClasses.contains(StatementInstance.class)) {
                statementClasses.remove(StatementInstance.class);
                return create(statementClasses.iterator().next(), var, properties);
            } else {
                throw new IllegalStateException("Not allowed to mix Properties for different Instance stateents");
            }
        } else if (statementClasses.size() == 1) {
            return create(statementClasses.iterator().next(), var, properties);
        } else {
            return new Statement(var);
        }
    }

    private static Statement create(Class statementClass, Variable var, LinkedHashSet<VarProperty> properties) {
        if (statementClass == StatementType.class) {
            return new StatementType(var, properties);

        } else if (statementClass == StatementInstance.class
                || statementClass == StatementThing.class) {
            return new StatementThing(var, properties);

        } else if (statementClass == StatementRelation.class) {
            return new StatementRelation(var, properties);

        } else if (statementClass == StatementAttribute.class) {
            return new StatementAttribute(var, properties);

        } else {
            throw new IllegalArgumentException("Unrecognised Statement class: " + statementClass.getName());
        }
    }

    // TYPE STATEMENT PROPERTIES ===============================================

    /**
     * @param name a string that this variable's label must match
     * @return this
     */
    @CheckReturnValue
    public StatementType type(String name) {
        return StatementType.create(this, new TypeProperty(name));
    }

    /**
     * set this concept type variable as abstract, meaning it cannot have direct instances
     *
     * @return this
     */
    @CheckReturnValue
    public StatementType isAbstract() {
        return StatementType.create(this, AbstractProperty.get());
    }

    /**
     * @param type a concept type id that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    public StatementType sub(String type) {
        return sub(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of
     * @return this
     */
    @CheckReturnValue
    public StatementType sub(Statement type) {
        return StatementType.create(this, new SubProperty(type));
    }

    /**
     * @param type a concept type id that this variable must be a kind of, without looking at parent types
     * @return this
     */
    @CheckReturnValue
    public StatementType subX(String type) {
        return subX(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be a kind of, without looking at parent type
     * @return this
     */
    @CheckReturnValue
    public StatementType subX(Statement type) {
        return StatementType.create(this, new SubExplicitProperty(type));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    public StatementType key(String type) {
        return key(Graql.var().type(type));
    }

    /**
     * @param type a resource type that this type variable can be one-to-one related to
     * @return this
     */
    @CheckReturnValue
    public StatementType key(Statement type) {
        return StatementType.create(this, new HasAttributeTypeProperty(type, true));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    public StatementType has(String type) {
        return has(Graql.type(type));
    }

    /**
     * @param type a resource type that this type variable can be related to
     * @return this
     */
    @CheckReturnValue
    public StatementType has(Statement type) {
        return StatementType.create(this, new HasAttributeTypeProperty(type, false));
    }

    /**
     * @param type a Role id that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    public StatementType plays(String type) {
        return plays(Graql.type(type));
    }

    /**
     * @param type a Role that this concept type variable must play
     * @return this
     */
    @CheckReturnValue
    public StatementType plays(Statement type) {
        return StatementType.create(this, new PlaysProperty(type, false));
    }

    /**
     * @param type a Role id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public StatementType relates(String type) {
        return relates(type, null);
    }

    /**
     * @param type a Role that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public StatementType relates(Statement type) {
        return relates(type, null);
    }

    /**
     * @param roleType a Role id that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public StatementType relates(String roleType, @Nullable String superRoleType) {
        return relates(Graql.type(roleType), superRoleType == null ? null : Graql.type(superRoleType));
    }

    /**
     * @param roleType a Role that this relation type variable must have
     * @return this
     */
    @CheckReturnValue
    public StatementType relates(Statement roleType, @Nullable Statement superRoleType) {
        return StatementType.create(this, new RelatesProperty(roleType, superRoleType));
    }

    /**
     * @param datatype the datatype to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    public StatementType datatype(Query.DataType datatype) {
        return StatementType.create(this, new DataTypeProperty(datatype));
    }

    /**
     * Specify the regular expression instances of this resource type must match
     *
     * @param regex the regex to set for this resource type variable
     * @return this
     */
    @CheckReturnValue
    public StatementType regex(String regex) {
        return StatementType.create(this, new RegexProperty(regex));
    }

    /**
     * @param when the left-hand side of this rule
     * @return this
     */
    @CheckReturnValue // TODO: make when() method take a more strict sub type of pattern
    public StatementType when(Pattern when) {
        return StatementType.create(this, new WhenProperty(when));
    }

    /**
     * @param then the right-hand side of this rule
     * @return this
     */
    @CheckReturnValue // TODO: make then() method take a more strict sub type of pattern
    public StatementType then(Pattern then) {
        return StatementType.create(this, new ThenProperty(then));
    }

    // INSTANCE STATEMENT PROPERTIES ===========================================

    /**
     * @param type a concept type id that the variable must be of this type directly or indirectly
     * @return this
     */
    @CheckReturnValue
    public StatementInstance isa(String type) {
        return isa(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly or indirectly
     * @return this
     */
    @CheckReturnValue
    public StatementInstance isa(Statement type) {
        return isa(new IsaProperty(type));
    }

    /**
     * @param type a concept type id that the variable must be of this type directly
     * @return this
     */
    @CheckReturnValue
    public StatementInstance isaX(String type) {
        return isaX(Graql.type(type));
    }

    /**
     * @param type a concept type that this variable must be an instance of directly
     * @return this
     */
    @CheckReturnValue
    public StatementInstance isaX(Statement type) {
        return isa(new IsaProperty(type, true));
    }

    @CheckReturnValue
    public StatementInstance isa(IsaProperty property) {
        return StatementInstance.create(this, property);
    }

    /**
     * the variable must have a resource of the given type with an exact matching value
     *
     * @param type  a resource type in the schema
     * @param value a value of a resource
     * @return this
     */
    @CheckReturnValue
    public StatementInstance has(String type, Object value) {
        return has(type, Graql.eq(value));
    }

    /**
     * the variable must have a resource of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param predicate a atom on the value of a resource
     * @return this
     */
    @CheckReturnValue
    public StatementInstance has(String type, ValuePredicate predicate) {
        return has(type, Graql.var().val(predicate));
    }

    /**
     * the variable must have an Attribute of the given type that matches the given atom
     *
     * @param type      a resource type in the schema
     * @param attribute a variable pattern representing an Attribute
     * @return this
     */
    @CheckReturnValue
    public StatementInstance has(String type, Statement attribute) {
        return has(new HasAttributeProperty(type, attribute));
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
    public StatementInstance has(String type, Statement attribute, Statement relationship) {
        return has(new HasAttributeProperty(type, attribute, relationship));
    }

    @CheckReturnValue
    public StatementInstance has(HasAttributeProperty property) {
        return StatementInstance.create(this, property);
    }

    // THING STATEMENT PROPERTIES ----------------------------------------------

    /**
     * @param id a ConceptId that this variable's ID must match
     * @return this
     */
    @CheckReturnValue
    public StatementThing id(String id) {
        return StatementThing.create(this, new IdProperty(id));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param var the variable that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public StatementThing neq(String var) {
        return neq(new Variable(var));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param var the variable pattern that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public StatementThing neq(Variable var) {
        return neq(new NeqProperty(new Statement(var)));
    }

    /**
     * Specify that the variable is different to another variable
     *
     * @param property the NEQ property containing variable pattern that this variable should not be equal to
     * @return this
     */
    @CheckReturnValue
    public StatementThing neq(NeqProperty property) {
        return StatementThing.create(this, property);
    }

    // RELATION STATEMENT PROPERTIES --------------------------------------------

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public StatementRelation rel(String player) {
        return rel(Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer
     *
     * @param player a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public StatementRelation rel(Statement player) {
        return StatementRelation.create(this,
                                        new RelationProperty.RolePlayer(null, player));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a Role in the schema
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public StatementRelation rel(String role, String player) {
        return rel(Graql.type(role), Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a variable pattern representing a Role
     * @param player a variable representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public StatementRelation rel(Statement role, String player) {
        return rel(role, Graql.var(player));
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a Role in the schema
     * @param player a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public StatementRelation rel(String role, Statement player) {
        return rel(Graql.type(role), player);
    }

    /**
     * the variable must be a relation with the given roleplayer playing the given Role
     *
     * @param role       a variable pattern representing a Role
     * @param player a variable pattern representing a roleplayer
     * @return this
     */
    @CheckReturnValue
    public StatementRelation rel(Statement role, Statement player) {
        return StatementRelation.create(this,
                                        new RelationProperty.RolePlayer(role, player));
    }

    @CheckReturnValue
    public StatementRelation rel(RelationProperty property) {
        return StatementRelation.create(this, property);
    }

    // ATTRIBUTE STATEMENT PROPERTIES ------------------------------------------

    /**
     * @param value a value that this variable's value must exactly match
     * @return this
     */
    @CheckReturnValue
    public StatementAttribute val(Object value) {
        return val(Graql.eq(value));
    }

    /**
     * @param predicate a atom this variable's value must match
     * @return this
     */
    @CheckReturnValue
    public StatementAttribute val(ValuePredicate predicate) {
        return val(new ValueProperty(predicate));
    }

    @CheckReturnValue
    public StatementAttribute val(ValueProperty property) {
        return StatementAttribute.create(this, property);
    }

    // GENERAL STATEMENT PROPERTIES ============================================

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

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        // A disjunction containing only one option
        Conjunction<Statement> conjunction = Graql.and(Collections.singleton(this));
        return Graql.or(Collections.singleton(conjunction));
    }

    @Override
    public Disjunction<Conjunction<Pattern>> getNegationDNF() {
        Conjunction<Pattern> conjunction = Graql.and(Collections.singleton(this));
        return Graql.or(Collections.singleton(conjunction));
    }
    /**
     * @return the name this variable represents, if it represents something with a specific name
     */
    @CheckReturnValue
    public Optional<Label> getTypeLabel() {
        return getProperty(TypeProperty.class).map(labelProperty -> Label.of(labelProperty.name()));
    }

    /**
     * @return all type names that this variable refers to
     */
    @CheckReturnValue
    public Set<Label> getTypeLabels() {
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
    public <T extends VarProperty> Stream<T> getProperties(Class<T> type) {
        return properties().stream().filter(type::isInstance).map(type::cast);
    }

    /**
     * Get a unique property of a particular type on this variable, if it exists
     *
     * @param type the class of VarProperty to return
     * @param <T>  the type of VarProperty to return
     */
    @CheckReturnValue
    public <T extends VarProperty> Optional<T> getProperty(Class<T> type) {
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
    public <T extends VarProperty> boolean hasProperty(Class<T> type) {
        return getProperties(type).findAny().isPresent();
    }

    /**
     * @return all variables that this variable references
     */
    @CheckReturnValue
    public Collection<Statement> innerStatements() {
        Stack<Statement> statementStack = new Stack<>();
        List<Statement> statements = new ArrayList<>();

        statementStack.add(this);

        while (!statementStack.isEmpty()) {
            Statement statement = statementStack.pop();
            statements.add(statement);
            statement.properties().stream().flatMap(varProperty -> varProperty.statements()).forEach(statementStack::add);
        }

        return statements;
    }

    /**
     * Get all inner variables, including implicit variables such as in a has property
     */
    @CheckReturnValue
    public Collection<Statement> implicitInnerStatements() {
        Stack<Statement> newVars = new Stack<>();
        List<Statement> vars = new ArrayList<>();

        newVars.add(this);

        while (!newVars.isEmpty()) {
            Statement var = newVars.pop();
            vars.add(var);
            var.properties().stream().flatMap(varProperty -> varProperty.statementsImplicit()).forEach(newVars::add);
        }

        return vars;
    }

    @Override
    public Set<Variable> variables() {
        return innerStatements().stream()
                .filter(v -> v.var().isUserDefinedName())
                .map(statement -> statement.var())
                .collect(toSet());
    }

    /**
     * @return the name of this variable, as it would be referenced in a native Graql query (e.g. '$x', 'movie')
     */
    @CheckReturnValue
    public String getPrintableName() {
        if (properties().size() == 0) {
            // If there are no properties, we display the variable name
            return var().toString();
        } else if (properties().size() == 1) {
            // If there is only a label, we display that
            Optional<Label> label = getTypeLabel();
            if (label.isPresent()) {
                return StringUtil.typeLabelToString(label.get());
            }
        }

        // Otherwise, we print the entire pattern
        return "`" + toString() + "`";
    }

    void validateNonUniqueOrThrow(VarProperty property) {
        if (property.isUnique()) {
            getProperty(property.getClass()).filter(other -> !other.equals(property)).ifPresent(other -> {
                throw GraqlQueryException.conflictingProperties(this, property, other);
            });
        }
    }

    private Statement removeProperty(VarProperty property) {
        Variable name = var();
        LinkedHashSet<VarProperty> newProperties = new LinkedHashSet<>(this.properties);
        newProperties.remove(property);
        return new Statement(name, newProperties);
    }

    @Override // TODO: Remove this method altogether once we make compile time validation more strict
    public String toString() {
        throw new IllegalStateException(
                "Attempted to print invalid statement: with just a Variable [" + var + "] and no properties"
        );
    }

    @Override
    public boolean equals(Object o) {
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
    public int hashCode() {
        if (hashCode == 0) {
            // This hashCode implementation is special: it considers all non-user-defined vars as equivalent
            hashCode = properties().hashCode();
            if (var().isUserDefinedName()) hashCode = 31 * hashCode + var().hashCode();
            hashCode = 31 * hashCode + (var().isUserDefinedName() ? 1 : 0);
        }
        return hashCode;
    }
}
