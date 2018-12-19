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

package grakn.core.graql.query.pattern.property;

import grakn.core.common.util.CommonUtil;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.Transaction;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A property of a {@link Statement}, such as "isa movie" or "has name 'Jim'"
 */
public abstract class VarProperty {

    public abstract String getName();

    public abstract String getProperty();

    /**
     * True if there is at most one of these properties for each {@link Statement}
     */
    @CheckReturnValue
    public abstract boolean isUnique();

    /**
     * True if this property only considers direct types when dealing with type hierarchies
     */
    @CheckReturnValue
    public boolean isExplicit() {
        return false;
    }

    /**
     * Get a stream of {@link Statement} that must be types.
     */
    @CheckReturnValue
    public Stream<Statement> getTypes() {
        return Stream.empty();
    }

    /**
     * Get a stream of any inner {@link Statement} within this `VarProperty`.
     */
    @CheckReturnValue
    public Stream<Statement> innerStatements() {
        return Stream.empty();
    }

    /**
     * Get a stream of any inner {@link Statement} within this `VarProperty`, including any that may have been
     * implicitly created (such as with "has").
     */
    @CheckReturnValue
    public Stream<Statement> implicitInnerStatements() {
        return innerStatements();
    }

    /**
     * Whether this property will uniquely identify a concept in the graph, if one exists.
     * This is used for recognising equivalent variables in insert queries.
     */
    public boolean uniquelyIdentifiesConcept() {
        return false;
    }

    /**
     * Get the Graql string representation of this property
     */
    @Override
    public String toString() {
        return getName() + " " + getProperty();
    }

    /**
     * Check if the given property can be used in a match clause
     */
    public final void checkValid(Transaction graph, Statement var) throws GraqlQueryException {
        checkValidProperty(graph, var);

        innerStatements().map(Statement::getTypeLabel).flatMap(CommonUtil::optionalToStream).forEach(label -> {
            if (graph.getSchemaConcept(label) == null) {
                throw GraqlQueryException.labelNotFound(label);
            }
        });
    }

    void checkValidProperty(Transaction graph, Statement var) { }

    /**
     * @param statement parent statement
     * @return true if this property has an atomic equivalent
     */
    @CheckReturnValue
    public boolean mapsToAtom(Statement statement){ return true;}

    /**
     * maps this var property to a reasoner atom
     *
     * @param var    {@link Statement} this property belongs to
     * @param vars   Vars constituting the pattern this property belongs to
     * @param parent reasoner query this atom should belong to
     * @return created atom
     */
    @CheckReturnValue
    public abstract Atomic mapToAtom(Statement var, Set<Statement> vars, ReasonerQuery parent);

    /**
     * Return a collection of {@link EquivalentFragmentSet} to match the given property in the graph
     */
    public abstract Collection<EquivalentFragmentSet> match(Variable start);

    /**
     * Returns a {@link PropertyExecutor} that describes how to insert the given {@link VarProperty} into.
     *
     * @throws GraqlQueryException if this {@link VarProperty} cannot be inserted
     */
    public Collection<PropertyExecutor> insert(Variable var) throws GraqlQueryException {
        throw GraqlQueryException.insertUnsupportedProperty(getName());
    }

    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        throw GraqlQueryException.defineUnsupportedProperty(getName());
    }

    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        throw GraqlQueryException.defineUnsupportedProperty(getName());
    }
}
