/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.procedure;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.producer.FunctionalProducer;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.Traversal;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typedb.core.traversal.common.VertexMap;

public interface PermutationProcedure {

    FunctionalProducer<VertexMap> producer(GraphManager graphMgr, Traversal.Parameters params,
                                           Modifiers modifiers, int parallelisation);

    FunctionalIterator<VertexMap> iterator(GraphManager graphMgr, Traversal.Parameters params,
                                           Modifiers modifiers);
}
