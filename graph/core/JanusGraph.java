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

package grakn.core.graph.core;

import grakn.core.graph.core.schema.JanusGraphManagement;
import grakn.core.graph.graphdb.configuration.JanusGraphConstants;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Gremlin;

/**
 * JanusGraph graph database implementation of the Blueprint's interface.
 * Use {@link JanusGraphFactory} to open and configure JanusGraph instances.
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
//------------------------
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyAddition",
        method = "shouldHandleSetVertexProperties",
        reason = "JanusGraph can only handle SET cardinality for properties when defined in the schema.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldOnlyAllowReadingVertexPropertiesInMapReduce",
        reason = "JanusGraph simply throws the wrong exception -- should not be a ReadOnly transaction exception but a specific one for MapReduce. This is too cumbersome to refactor in JanusGraph.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldProcessResultGraphNewWithPersistVertexProperties",
        reason = "The result graph should return an empty iterator when vertex.edges() or vertex.vertices() is called.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithNoEdgeLabels",
        reason = "JanusGraph does not support default edge label (edge) used when GraphML is missing edge labels.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithoutEdgeIds",
        reason = "JanusGraph does not support default edge label (edge) used when GraphML is missing edge ids.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldSupportGraphFilter",
        reason = "JanusGraph test graph computer (FulgoraGraphComputer) " +
                "currently does not support graph filters but does not throw proper exception because doing so breaks numerous " +
                "tests in gremlin-test ProcessComputerSuite.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgramTest",
        method = "*",
        reason = "ShortestPathVertexProgram currently has two bugs that prevent us from using it correctly. See " +
                "https://issues.apache.org/jira/browse/TINKERPOP-2187 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ShortestPathTest$Traversals",
        method = "*",
        reason = "ShortestPathVertexProgram currently has two bugs that prevent us from using it correctly. See " +
                "https://issues.apache.org/jira/browse/TINKERPOP-2187 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX",
        reason = "The test assumes that a certain vertex has always the lowest id which is not the case for " +
                "JanusGraph. See https://issues.apache.org/jira/browse/TINKERPOP-2189 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "g_V_connectedComponent_withXEDGES_bothEXknowsXX_withXPROPERTY_NAME_clusterX_project_byXnameX_byXclusterX",
        reason = "The test assumes that a certain vertex has always the lowest id which is not the case for " +
                "JanusGraph. See https://issues.apache.org/jira/browse/TINKERPOP-2189 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
        method = "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
        reason = "The test assumes that a certain vertex has always the lowest id which is not the case for " +
                "JanusGraph. See https://issues.apache.org/jira/browse/TINKERPOP-2189 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "g_V_dedup_connectedComponent_hasXcomponentX",
        reason = "The test involves serializing and deserializing of vertices, especially of CacheVertex. This class" +
                "is however not serializable and it is non-trivial to enable serialization as the class is tied to a" +
                "transaction. See #1519 for more information.")
public interface JanusGraph extends Graph {

    /* ---------------------------------------------------------------
     * Transactions and general admin
     * ---------------------------------------------------------------
     */

    /**
     * Opens a new thread-independent {@link JanusGraphTransaction}.
     * <p>
     * The transaction is open when it is returned but MUST be explicitly closed by calling {@link JanusGraphTransaction#commit()}
     * or {@link JanusGraphTransaction#rollback()} when it is no longer needed.
     * <p>
     * Note, that this returns a thread independent transaction object. It is not necessary to call this method
     * to use Blueprint's standard transaction framework which will automatically start a transaction with the first
     * operation on the graph.
     *
     * @return Transaction object representing a transactional context.
     */
    JanusGraphTransaction newTransaction();

    /**
     * Returns a {@link TransactionBuilder} to construct a new thread-independent {@link JanusGraphTransaction}.
     *
     * @return a new TransactionBuilder
     * @see TransactionBuilder
     * @see #newTransaction()
     */
    TransactionBuilder buildTransaction();

    /**
     * Returns the management system for this graph instance. The management system provides functionality
     * to change global configuration options, install indexes and inspect the graph schema.
     * <p>
     * The management system operates in its own transactional context which must be explicitly closed.
     */
    JanusGraphManagement openManagement();

    /**
     * Checks whether the graph is open.
     *
     * @return true, if the graph is open, else false.
     * @see #close()
     */
    boolean isOpen();

    /**
     * Checks whether the graph is closed.
     *
     * @return true, if the graph has been closed, else false
     */
    boolean isClosed();

    /**
     * Closes the graph database.
     * <p>
     * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
     * and a release of all occupied resources by this graph database.
     * Closing a graph database requires that all open thread-independent transactions have been closed -
     * otherwise they will be left abandoned.
     *
     * @throws JanusGraphException if closing the graph database caused errors in the storage backend
     */
    @Override
    void close() throws JanusGraphException;

    /**
     * The version of this JanusGraph graph database
     */
    static String version() {
        return JanusGraphConstants.VERSION;
    }

    static void main(String[] args) {
        System.out.println("JanusGraph " + JanusGraph.version() + ", Apache TinkerPop " + Gremlin.version());
    }
}
