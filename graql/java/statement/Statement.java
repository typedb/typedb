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

package graql.lang.statement;

import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Pattern;
import graql.lang.property.RelationProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.VarProperty;
import graql.lang.statement.builder.StatementAttributeBuilder;
import graql.lang.statement.builder.StatementInstanceBuilder;
import graql.lang.statement.builder.StatementRelationBuilder;
import graql.lang.statement.builder.StatementThingBuilder;
import graql.lang.statement.builder.StatementTypeBuilder;
import graql.lang.exception.GraqlException;
import graql.lang.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
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
public class Statement implements Pattern,
                                  StatementTypeBuilder,
                                  StatementInstanceBuilder,
                                  StatementThingBuilder,
                                  StatementRelationBuilder,
                                  StatementAttributeBuilder {

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
                throw new IllegalStateException("Not allowed to mix Properties for different Instance statements");
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

    @Override
    public StatementType type(VarProperty property) {
        return StatementType.create(this, property);
    }

    @Override
    public StatementInstance statementInstance(VarProperty property) {
        return StatementInstance.create(this, property);
    }

    @Override
    public StatementThing thing(VarProperty property) {
        return StatementThing.create(this, property);
    }

    @Override
    public StatementRelation relation(RelationProperty.RolePlayer rolePlayer) {
        return StatementRelation.create(this, rolePlayer);
    }

    @Override
    public StatementRelation relation(VarProperty property) {
        return StatementRelation.create(this, property);
    }

    @Override
    public StatementAttribute attribute(VarProperty property) {
        return StatementAttribute.create(this, property);
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
    public Optional<String> getType() {
        return getProperty(TypeProperty.class).map(TypeProperty::name);
    }

    /**
     * @return all type names that this variable refers to
     */
    @CheckReturnValue
    public Set<String> getTypes() {
        return properties().stream()
                .flatMap(VarProperty::types)
                .map(Statement::getType)
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
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
     * @return all statements that this statement contains
     */
    @CheckReturnValue
    public Collection<Statement> innerStatements() {
        Stack<Statement> statementStack = new Stack<>();
        List<Statement> statements = new ArrayList<>();

        statementStack.add(this);

        while (!statementStack.isEmpty()) {
            Statement statement = statementStack.pop();
            statements.add(statement);
            statement.properties().stream()
                    .flatMap(VarProperty::statements)
                    .forEach(statementStack::add);
        }

        return statements;
    }

    @Override
    public Set<Variable> variables() {
        return innerStatements().stream().map(Statement::var)
                .filter(Variable::isUserDefinedName)
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
            Optional<String> label = getType();
            if (label.isPresent()) {
                return StringUtil.escapeLabelOrId(label.get());
            }
        }

        // Otherwise, we print the entire pattern
        return "`" + toString() + "`";
    }

    void validateNoConflictOrThrow(VarProperty property) {
        if (property == null) {
            throw new NullPointerException("VarProperty is null");
        }
        if (property.isUnique()) {
            getProperty(property.getClass()).filter(other -> !other.equals(property)).ifPresent(other -> {
                throw GraqlException.conflictingProperties(this.getPrintableName(), property.toString(), other.toString());
            });
        }
    }

    @Override // TODO: Remove this method altogether once we make compile time validation more strict
    public String toString() {
        return var.toString();
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
