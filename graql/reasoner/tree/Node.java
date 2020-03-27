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
 *
 */
package grakn.core.graql.reasoner.tree;

import grakn.core.concept.answer.ConceptMap;
import java.util.List;
import java.util.Set;

public interface Node {


    String graphString();

    /**
     * @param child node to be added as a child of this node.
     */
    void addChild(Node child);

    /**
     * @param answer to be associated with this node.
     */
    void addAnswer(ConceptMap answer);


    /**
     *
     * @return total time spent on processing the state corresponding to this node
     */
    long totalTime();

    /**
     *
     * @return children nodes of this node
     */
    List<Node> children();

    /**
     *
     * @return answer associated with this node (corresponding state)
     */
    Set<ConceptMap> answers();

    /**
     * Acknowledge completion of processing of this node.
     */
    void ackCompletion();
}
