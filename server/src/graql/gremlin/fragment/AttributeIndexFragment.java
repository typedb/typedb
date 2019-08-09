/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.gremlin.fragment;

import com.google.auto.value.AutoValue;
import grakn.core.concept.Label;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.server.kb.Schema;
import grakn.core.server.session.TransactionOLTP;
import grakn.core.server.statistics.KeyspaceStatistics;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.stream.Stream;

import static grakn.core.server.kb.Schema.VertexProperty.INDEX;

@AutoValue
public abstract class AttributeIndexFragment extends Fragment {

    public abstract Label attributeLabel();

    abstract String attributeValue();

    @Override
    public GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, TransactionOLTP tx, Collection<Variable> vars) {

        return traversal.has(INDEX.name(), attributeIndex());
    }

    @Override
    public String name() {
        return "[index:" + attributeIndex() + "]";
    }

    @Override
    public double internalFragmentCost() {
        return COST_NODE_INDEX;
    }

    @Override
    public boolean hasFixedFragmentCost() {
        return true;
    }

    private String attributeIndex() {
        String attributeIndex = Schema.generateAttributeIndex(attributeLabel(), attributeValue());
        return attributeIndex;
    }

    @Override
    public double estimatedCostAsStartingPoint(TransactionOLTP tx) {
        KeyspaceStatistics statistics = tx.session().keyspaceStatistics();
        // here we estimate the number of owners of an attribute instance of this type
        // as this is the most common usage/expensive component of an attribute
        // given that there's only 1 attribute of a type and value at any time
        Label attributeLabel = attributeLabel();

        AttributeType attributeType = tx.getSchemaConcept(attributeLabel).asAttributeType();
        Stream<AttributeType> attributeSubs = attributeType.subs();

        Label implicitAttributeType = Schema.ImplicitType.HAS.getLabel(attributeLabel);
        SchemaConcept implicitAttributeRelationType = tx.getSchemaConcept(implicitAttributeType);
        double totalImplicitRels = 0.0;
        if (implicitAttributeRelationType != null) {
            RelationType implicitRelationType = implicitAttributeRelationType.asRelationType();
            Stream<RelationType> implicitSubs = implicitRelationType.subs();
            totalImplicitRels = implicitSubs.mapToLong(t -> statistics.count(tx, t.label())).sum();
        }

        double totalAttributes = attributeSubs.mapToLong(t -> statistics.count(tx, t.label())).sum();
        if (totalAttributes == 0) {
            // check against division by 0 and
            // short circuit can be done quickly if starting here
            return 0.0;
        } else {
            // may well be 0 or 1 if there are many attributes and not many owners!
            return totalImplicitRels / totalAttributes;
        }
    }
}
