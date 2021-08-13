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
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_CALL_TO_FINISH_BEFORE_START;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_CALL_TO_WRITE_BEFORE_START;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_HAS_NOT_BEEN_INITIALISED;

public final class ResolutionTracer {

    private static final Logger LOG = LoggerFactory.getLogger(ResolutionTracer.class);

    private static ResolutionTracer INSTANCE;
    private static Path logDir = null;
    private static final Map<TraceId, RootRequestTracer> rootRequestTracers = new HashMap<>();

    private ResolutionTracer(Path logDir) {
        ResolutionTracer.logDir = logDir;
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

    public synchronized void request(Request request, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String conceptMap = request.partialAnswer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.REQUEST, conceptMap);
    }

    public synchronized void request(Materialiser.Request request, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String conceptMap = request.partialAnswer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.REQUEST, conceptMap);
    }

    public void responseAnswer(Materialiser.Request request, Map<Identifier.Variable, Concept> materialisation, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String concepts = materialisation.keySet().toString();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.ANSWER, concepts);
    }

    public synchronized void responseAnswer(Request request, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        String conceptMap = request.partialAnswer().conceptMap().concepts().keySet().toString();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.ANSWER, conceptMap);
    }

    public synchronized void responseExhausted(Request request, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.EXHAUSTED, "");
    }

    public synchronized void responseExhausted(Materialiser.Request request, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.EXHAUSTED, "");
    }

    public synchronized void responseBlocked(Request request, int iteration) {
        String sender = request.sender().name();
        String receiver = request.receiver().name();
        addMessage(sender, receiver, request.traceId(), iteration, EdgeType.BLOCKED, "");
    }

    private void addMessage(String sender, String receiver, TraceId traceId, int iteration, EdgeType edgeType,
                            String conceptMap) {
        rootRequestTracers.get(traceId).addMessage(sender, receiver, iteration, edgeType, conceptMap);
    }

    public synchronized void start(Request request) {
        assert !rootRequestTracers.containsKey(request.traceId());
        rootRequestTracers.put(request.traceId(), new RootRequestTracer(request.traceId()));
        rootRequestTracers.get(request.traceId()).start();
    }

    public synchronized void finish(Request request) {
        rootRequestTracers.get(request.traceId()).finish();
    }

    public synchronized void exception() {
        rootRequestTracers.forEach((r, t) -> t.finish());
    }

    private static class RootRequestTracer {

        private final AtomicReference<Path> path = new AtomicReference<>(null);
        private final TraceId traceId;
        private int messageNumber = 0;
        private PrintWriter writer;

        private RootRequestTracer(TraceId traceId) {
            this.traceId = traceId;
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
                    "digraph request_%d_%d {\n" +
                            "node [fontsize=12 fontname=arial width=0.5 shape=box style=filled]\n" +
                            "edge [fontsize=10 fontname=arial width=0.5]",
                    traceId.scopeId, traceId.rootId()));
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
            return String.format("resolution_trace_request_%d_%d.dot", traceId.scopeId(), traceId.rootId());
        }

        private void endFile() {
            write("}");
        }

        private void write(String toWrite) {
            if (writer == null) throw TypeDBException.of(REASONER_TRACING_CALL_TO_WRITE_BEFORE_START);
            writer.println(toWrite);
        }

        private synchronized void addMessage(String sender, String receiver, int iteration, EdgeType edgeType, String conceptMap) {
            writeEdge(sender, receiver, iteration, edgeType.colour(), messageNumber, conceptMap);
            messageNumber++;
        }

        private void writeEdge(String fromId, String toId, int iteration, String colour, int messageNumber, String conceptMap) {
            write(String.format("%s -> %s [style=bold,label=%s,color=%s];",
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(fromId))),
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(toId))),
                                doubleQuotes("m" + messageNumber + "_it" + iteration + "_" + conceptMap),
                                doubleQuotes(colour)));

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
        REQUEST("blue");

        private final String colour;

        EdgeType(String colour) {
            this.colour = colour;
        }

        public String colour() {
            return colour;
        }
    }

    public static class TraceId {

        private final int scopeId;
        private final int rootId;

        private TraceId(int scopeId, int rootId) {
            this.scopeId = scopeId;
            this.rootId = rootId;
        }

        public static TraceId create(int scopeId, int rootRequestId) {
            return new TraceId(scopeId, rootRequestId);
        }

        public static TraceId downstreamId() {
            return new TraceId(-1, -1);  // TODO
        }

        public int rootId() {
            return rootId;
        }

        public int scopeId() {
            return scopeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TraceId traceId = (TraceId) o;
            return scopeId == traceId.scopeId &&
                    rootId == traceId.rootId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(scopeId, rootId);
        }
    }

}
