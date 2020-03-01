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

import grakn.core.graph.core.EdgeLabel;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;

/**
 * Used to define new EdgeLabels.
 * An edge label is defined by its name, Multiplicity, its directionality, and its signature - all of which
 * can be specified in this builder.
 */
public interface EdgeLabelMaker extends RelationTypeMaker {

    /**
     * Sets the multiplicity of this label. The default multiplicity is Multiplicity#MULTI.
     *
     * @return this EdgeLabelMaker
     * see Multiplicity
     */
    EdgeLabelMaker multiplicity(Multiplicity multiplicity);

    /**
     * Configures the label to be directed.
     * <p>
     * By default, the label is directed.
     *
     * @return this EdgeLabelMaker
     * see EdgeLabel#isDirected()
     */
    EdgeLabelMaker directed();

    /**
     * Configures the label to be unidirected.
     * <p>
     * By default, the type is directed.
     *
     * @return this EdgeLabelMaker
     * see EdgeLabel#isUnidirected()
     */
    EdgeLabelMaker unidirected();


    @Override
    EdgeLabelMaker signature(PropertyKey... types);


    /**
     * Defines the EdgeLabel specified by this EdgeLabelMaker and returns the resulting label
     *
     * @return the created EdgeLabel
     */
    @Override
    EdgeLabel make();

}
