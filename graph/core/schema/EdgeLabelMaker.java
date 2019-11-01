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
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;

/**
 * Used to define new {@link EdgeLabel}s.
 * An edge label is defined by its name, {@link Multiplicity}, its directionality, and its signature - all of which
 * can be specified in this builder.
 */
public interface EdgeLabelMaker extends RelationTypeMaker {

    /**
     * Sets the multiplicity of this label. The default multiplicity is {@link Multiplicity#MULTI}.
     *
     * @return this EdgeLabelMaker
     * @see Multiplicity
     */
    EdgeLabelMaker multiplicity(Multiplicity multiplicity);

    /**
     * Configures the label to be directed.
     * <p>
     * By default, the label is directed.
     *
     * @return this EdgeLabelMaker
     * @see EdgeLabel#isDirected()
     */
    EdgeLabelMaker directed();

    /**
     * Configures the label to be unidirected.
     * <p>
     * By default, the type is directed.
     *
     * @return this EdgeLabelMaker
     * @see EdgeLabel#isUnidirected()
     */
    EdgeLabelMaker unidirected();


    @Override
    EdgeLabelMaker signature(PropertyKey... types);


    /**
     * Defines the {@link EdgeLabel} specified by this EdgeLabelMaker and returns the resulting label
     *
     * @return the created {@link EdgeLabel}
     */
    @Override
    EdgeLabel make();

}
