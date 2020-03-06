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

package grakn.core.graph.core;


import grakn.core.graph.core.schema.JanusGraphSchemaType;

/**
 * RelationType defines the schema for JanusGraphRelation. RelationType can be configured through RelationTypeMaker to
 * provide data verification, better storage efficiency, and higher retrieval performance.
 * <br>
 * Each JanusGraphRelation has a unique type which defines many important characteristics of that relation.
 * <br>
 * RelationTypes are constructed through RelationTypeMaker which is accessed in the context of a JanusGraphTransaction
 * via JanusGraphTransaction#makePropertyKey(String) for property keys or JanusGraphTransaction#makeEdgeLabel(String)
 * for edge labels. Identical methods exist on JanusGraph.
 * Note, relation types will only be visible once the transaction in which they were created has been committed.
 * <br>
 * RelationType names must be unique in a graph database. Many methods allow the name of the type as an argument
 * instead of the actual type reference. That also means, that edge labels and property keys may not have the same name.
 *
 * see JanusGraphRelation
 * see RelationTypeMaker
 * see <a href="https://docs.janusgraph.org/latest/schema.html">"Schema and Data Modeling" manual chapter</a>
 */
public interface RelationType extends JanusGraphVertex, JanusGraphSchemaType {

    /**
     * Checks if this relation type is a property key
     *
     * @return true, if this relation type is a property key, else false.
     * see PropertyKey
     */
    boolean isPropertyKey();

    /**
     * Checks if this relation type is an edge label
     *
     * @return true, if this relation type is a edge label, else false.
     * see EdgeLabel
     */
    boolean isEdgeLabel();

}
