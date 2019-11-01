// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.core.schema;

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;

/**
 * RelationTypeMaker is a factory for {@link RelationType}s. RelationType can be configured to provide data verification,
 * better storage efficiency, and higher performance. The RelationType defines the schema for all {@link JanusGraphRelation}s
 * of that type.
 * <p>
 * There are two kinds of RelationTypes: {@link EdgeLabel} and {@link PropertyKey} which
 * are defined via their builders {@link EdgeLabelMaker} and {@link PropertyKeyMaker} respectively. This interface just defines builder methods
 * common to both of them.
 * <p>
 *
 * @see RelationType
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
     * {@link RelationType}s used in the signature must be either property out-unique keys or out-unique unidirected edge labels.
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
     * @return the configured {@link RelationType}
     */
    RelationType make();
}
