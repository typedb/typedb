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
 */

package grakn.core.graql.planning.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

/**
 * A fragment representing a negation.
 * Used for concept comparison, not attribute values
 *
 */
public class NeqFragment extends FragmentImpl {

    private final Variable other;

    NeqFragment(
            @Nullable VarProperty varProperty,
            Variable start,
            Variable other) {
        super(varProperty, start);
        this.other = other;
    }

    public Variable other() {
        return other;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> applyTraversalInner(
            GraphTraversal<Vertex, Vertex> traversal, ConceptManager conceptManager, Collection<Variable> vars) {

        // neq may be between two edges
        // we save the point the traversal was at
        // recall the start variable, compare it to the other variable
        // and restore the traversal point

        Variable checkpoint = new Variable();
        traversal.as(checkpoint.symbol());
        traversal.select(start().symbol()).where(P.neq(other().symbol()));
        traversal.select(checkpoint.symbol());
        return traversal;
    }

    @Override
    public String name() {
        return "[neq:" + other().symbol() + "]";
    }

    @Override
    public double internalFragmentCost() {
        // This is arbitrary - we imagine about half the results are filtered out
        return COST_NODE_NEQ;
    }

    @Override
    public Fragment getInverse() {
        return Fragments.neq(varProperty(), other(), start());
    }

    @Override
    public ImmutableSet<Variable> dependencies() {
        return ImmutableSet.of(other());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof NeqFragment) {
            NeqFragment that = (NeqFragment) o;
            return ((this.varProperty == null) ? (that.varProperty() == null) : this.varProperty.equals(that.varProperty()))
                    && (this.start.equals(that.start()))
                    && (this.other.equals(that.other()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(varProperty, start, other);
    }
}
