/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.reasoner.computation.actor.Connection;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Provider;
import com.vaticle.typedb.core.reasoner.computation.reactive.Receiver;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_CALL_TO_FINISH_BEFORE_START;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_CALL_TO_WRITE_BEFORE_START;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_HAS_NOT_BEEN_INITIALISED;

public final class ResolutionTracer {

    private static final Logger LOG = LoggerFactory.getLogger(ResolutionTracer.class);

    private static ResolutionTracer INSTANCE;
    private static Path logDir = null;
    private static final Map<Trace, RootRequestTracer> rootRequestTracers = new HashMap<>();
    private final Trace defaultTrace;

    private ResolutionTracer(Path logDir) {
        ResolutionTracer.logDir = logDir;
        this.defaultTrace = ResolutionTracer.Trace.create(UUID.randomUUID(), 0);
    }

    public static void initialise(Path logDir) {
        if (ResolutionTracer.logDir != null && !ResolutionTracer.logDir.equals(logDir)) {
            throw TypeDBException.of(ILLEGAL_STATE);
        }
        if (INSTANCE == null) {
            INSTANCE = new ResolutionTracer(logDir);
        }
    }

    public static ResolutionTracer get() {
        if (INSTANCE == null) throw TypeDBException.of(REASONER_TRACING_HAS_NOT_BEEN_INITIALISED);
        return INSTANCE;
    }

    public static Optional<ResolutionTracer> getIfEnabled() {
        return Optional.ofNullable(INSTANCE);
    }

    public synchronized void pull(@Nullable Receiver<?> receiver, Provider<?> provider) {
        String receiverString;
        if (receiver == null) receiverString = "root";
        else {
            receiverString = simpleClassId(receiver);
            addNodeGroup(simpleClassId(provider), receiver.groupName(), defaultTrace);
        }
        addMessage(receiverString, simpleClassId(provider), defaultTrace, EdgeType.PULL, "pull");
        addNodeGroup(receiverString, provider.groupName(), defaultTrace);

    }

    public synchronized void pull(Connection<?, ?, ?> receiver, Provider<?> provider) {
        addMessage(simpleClassId(receiver), simpleClassId(provider), defaultTrace, EdgeType.PULL, "pull");
        addNodeGroup(simpleClassId(provider), provider.groupName(), defaultTrace);
    }

    public synchronized void pull(Provider<?> receiver, Connection<?, ?, ?> provider) {
        addMessage(simpleClassId(receiver), simpleClassId(provider), defaultTrace, EdgeType.PULL, "pull");
        addNodeGroup(simpleClassId(receiver), receiver.groupName(), defaultTrace);
    }

    public synchronized <PACKET> void receive(Provider<PACKET> provider, Receiver<PACKET> receiver, PACKET packet) {
        addMessage(simpleClassId(provider), simpleClassId(receiver), defaultTrace, EdgeType.RECEIVE, packet.toString());
        addNodeGroup(simpleClassId(receiver), receiver.groupName(), defaultTrace);
        addNodeGroup(simpleClassId(provider), provider.groupName(), defaultTrace);
    }

    public synchronized <PACKET> void receive(Connection<PACKET, ?, ?> provider, Processor.InletEndpoint<PACKET> receiver, PACKET packet) {
        addMessage(simpleClassId(provider), simpleClassId(receiver), defaultTrace, EdgeType.RECEIVE, packet.toString());
        addNodeGroup(simpleClassId(receiver), receiver.groupName(), defaultTrace);
    }

    public synchronized <PACKET> void receive(Processor.OutletEndpoint<PACKET> provider, Connection<PACKET,?,?> receiver, PACKET packet) {
        addMessage(simpleClassId(provider), simpleClassId(receiver), defaultTrace, EdgeType.RECEIVE, packet.toString());
        addNodeGroup(simpleClassId(provider), provider.groupName(), defaultTrace);
    }

    private static String simpleClassId(Object obj) {
        return obj.getClass().getSimpleName() + "@" + System.identityHashCode(obj);
    }

    public synchronized void visit(Request.Visit request) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String conceptMap = request.partialAnswer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, request.trace(), EdgeType.VISIT, conceptMap);
    }

    public synchronized void visit(Materialiser.Request request) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String conceptMap = request.partialAnswer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, request.trace(), EdgeType.VISIT, conceptMap);
    }

    public void revisit(Request.Revisit request) {
        String sender = request.visit().sender().name();
        String receiver = request.visit().receiver().name();
        String conceptMap = request.visit().partialAnswer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, request.trace(), EdgeType.REVISIT, conceptMap);
    }

    public void responseAnswer(Materialiser.Response request, Map<Identifier.Variable, Concept> materialisation) {
        // Not static due to having one Materialiser
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String concepts = materialisation.keySet().toString();
        addMessage(sender, receiver, request.trace(), EdgeType.ANSWER, concepts);
    }

    public synchronized void responseAnswer(Response.Answer response) {
        String sender = response.sender().name();
        String receiver = response.receiver().name();
        String conceptMap = response.answer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, response.trace(), EdgeType.ANSWER, conceptMap);
    }

    public synchronized void responseExhausted(Response.Fail response) {
        String sender = response.sender().name();
        String receiver = response.receiver().name();
        addMessage(sender, receiver, response.trace(), EdgeType.EXHAUSTED, "");
    }

    public synchronized void responseExhausted(Materialiser.Response response) {
        String sender = response.sender().name();
        String receiver = response.receiver().name();
        addMessage(sender, receiver, response.trace(), EdgeType.EXHAUSTED, "");
    }

    public synchronized void responseBlocked(Response.Blocked response) {
        String sender = response.sender().name();
        String receiver = response.receiver().name();
        addMessage(sender, receiver, response.trace(), EdgeType.BLOCKED, "");
    }

    private void addMessage(String sender, String receiver, Trace trace, EdgeType edgeType,
                            String conceptMap) {
        rootRequestTracers.get(trace).addMessage(sender, receiver, edgeType, conceptMap);
    }

    private void addNodeGroup(String node, String group, Trace trace) {
        rootRequestTracers.get(trace).addNodeGroup(node, group);
    }

    public synchronized void start(Request.Visit request) {
        assert !rootRequestTracers.containsKey(request.trace());
        rootRequestTracers.put(request.trace(), new RootRequestTracer(request.trace()));
        rootRequestTracers.get(request.trace()).start();
    }

    public synchronized void startDefaultTrace() {
        Trace trace = defaultTrace;
        assert !rootRequestTracers.containsKey(trace);
        rootRequestTracers.put(trace, new RootRequestTracer(trace));
        rootRequestTracers.get(trace).start();
    }

    public synchronized void finish(Request request) {
        rootRequestTracers.get(request.trace()).finish();
    }

    public synchronized void finishDefaultTrace() {
        rootRequestTracers.get(defaultTrace).finish();
    }

    public synchronized void exception() {
        rootRequestTracers.forEach((r, t) -> t.finish());
    }

    private static class RootRequestTracer {

        private final AtomicReference<Path> path = new AtomicReference<>(null);
        private final Trace trace;
        private final Map<String, Integer> clusterMap;
        private int messageNumber = 0;
        private PrintWriter writer;
        private int clusterIndex;

        private RootRequestTracer(Trace trace) {
            this.trace = trace;
            this.clusterMap = new HashMap<>();
            this.clusterIndex = 0;
        }

        public void start() {
            path.set(logDir.resolve(filename()));
            try {
                File file = path.get().toFile();
                boolean ignore = file.getParentFile().mkdirs();
                writer = new PrintWriter(file, "UTF-8");
                startFile();
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                e.printStackTrace();
                LOG.trace("Resolution tracing failed to start writing");
            }
        }

        private void startFile() {
            write(String.format(
                    "digraph request_%s_%d {\n" +
                            "graph [fontsize=10 fontname=arial width=0.5]\n" +
                            "node [fontsize=12 fontname=arial width=0.5 shape=box style=filled]\n" +
                            "edge [fontsize=10 fontname=arial width=0.5]",
                    trace.scope().toString().replace('-', '_'), trace.root()));
        }

        public void finish() {
            if (path.get() == null) throw TypeDBException.of(REASONER_TRACING_CALL_TO_FINISH_BEFORE_START);
            endFile();
            try {
                LOG.debug("Resolution traces written to {}", path.get().toAbsolutePath());
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.debug("Resolution tracing failed to write to file");
            }
            path.set(null);
        }

        private String filename() {
            return String.format("resolution_trace_request_%s_%d.dot", trace.scope(), trace.root());
        }

        private void endFile() {
            write("}");
        }

        private void write(String toWrite) {
            if (writer == null) throw TypeDBException.of(REASONER_TRACING_CALL_TO_WRITE_BEFORE_START);
            writer.println(toWrite);
        }

        private synchronized void addMessage(String sender, String receiver, EdgeType edgeType, String packet) {
            writeEdge(sender, receiver, edgeType.colour(), "m" + messageNumber + "_" + packet);
            messageNumber++;
        }

        private synchronized void addNodeGroup(String node, String group) {
            writeNodeToCluster(node, group);
        }

        private void writeEdge(String fromId, String toId, String colour, String label) {
            write(String.format("%s -> %s [style=bold,label=%s,color=%s];",
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(fromId))),
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(toId))),
                                doubleQuotes(label),
                                doubleQuotes(colour)));

        }

        private int nextClusterId() {
            clusterIndex += 1;
            return clusterIndex;
        }

        private void writeNodeToCluster(String nodeId, String clusterLabel) {
            int ci = clusterMap.computeIfAbsent(clusterLabel, ignored -> nextClusterId());
            write(String.format("subgraph cluster_%d {%s; label=%s;}", ci, doubleQuotes(escapeNewlines(nodeId)),
                                doubleQuotes(escapeNewlines(clusterLabel))));
        }

        private String escapeNewlines(String toFormat) {
            return toFormat.replaceAll("\n", "\\\\l") + "\\l";
        }

        private String escapeDoubleQuotes(String toFormat) {
            return toFormat.replaceAll("\"", "\\\\\"");
        }

        private String doubleQuotes(String toFormat) {
            return "\"" + toFormat + "\"";
        }
    }

    enum EdgeType {
        EXHAUSTED("red"),
        BLOCKED("orange"),
        ANSWER("green"),
        VISIT("blue"),
        REVISIT("purple"),
        PULL("blue"),
        RECEIVE("green");

        private final String colour;

        EdgeType(String colour) {
            this.colour = colour;
        }

        public String colour() {
            return colour;
        }
    }

    public static class Trace {

        private final UUID scope;
        private final int root;

        private Trace(UUID scope, int root) {
            this.scope = scope;
            this.root = root;
        }

        public static Trace create(UUID scope, int rootRequestId) {
            return new Trace(scope, rootRequestId);
        }

        public int root() {
            return root;
        }

        public UUID scope() {
            return scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Trace trace = (Trace) o;
            return scope == trace.scope &&
                    root == trace.root;
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, root);
        }

        @Override
        public String toString() {
            return "Trace{" +
                    "root=" + root +
                    ", scope=" + scope +
                    '}';
        }
    }

}
