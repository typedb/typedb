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

package grakn.core.graph.core.schema;

import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;

/**
 * RelationTypeMaker is a factory for RelationTypes. RelationType can be configured to provide data verification,
 * better storage efficiency, and higher performance. The RelationType defines the schema for all JanusGraphRelations
 * of that type.
 * <p>
 * There are two kinds of RelationTypes: EdgeLabel and PropertyKey which
 * are defined via their builders EdgeLabelMaker and PropertyKeyMaker respectively. This interface just defines builder methods
 * common to both of them.
 * <p>
 *
 * see RelationType
 */
public interface RelationTypeMaker {

    /**
     * Returns the name of this configured relation type.
     */
    String getName();

    /**
     * Configures the signature of this relation type.
     * <p>
     * Specifying the signature of a type tells the graph database to <i>expect</i> that relations of this type
     * always have or are likely to have an incident property or unidirected edge of the type included in the
     * signature. This allows the graph database to store such relations more compactly and retrieve them more quickly.
     * <br>
     * For instance, if all edges with label <i>friend</i> have a property with key <i>createdOn</i>, then specifying
     * (<i>createdOn</i>) as the signature for label <i>friend</i> allows friend edges to be stored more efficiently.
     * <br>
     * RelationTypes used in the signature must be either property out-unique keys or out-unique unidirected edge labels.
     * <br>
     * The signature is empty by default.
     *
     * @param keys PropertyKey composing the signature for the configured relation type. The order is irrelevant.
     * @return this RelationTypeMaker
     */
    RelationTypeMaker signature(PropertyKey... keys);

    /**
     * Builds the configured relation type
     *
     * @return the configured RelationType
     */
    RelationType make();
}
