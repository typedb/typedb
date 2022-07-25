/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Monitor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_FILE_COULD_NOT_BE_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_WRITE_FAILED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Reasoner.REASONER_TRACING_DIRECTORY_COULD_NOT_BE_FOUND;

public class Tracer {

    private static final Logger LOG = LoggerFactory.getLogger(Tracer.class);
    private TraceWriter traceWriter;
    private final long id;
    private final Path logDir;

    public Tracer(long id, Path logDir) {
        this.id = id;
        this.logDir = logDir;
        traceWriter();
    }

    private TraceWriter traceWriter() {
        if (traceWriter == null) {
            traceWriter = new TraceWriter(id, logDir);
            traceWriter.start();
        }
        return traceWriter;
    }

    public synchronized void pull(Identifier subscriberId, Identifier publisherId) {
        pull(subscriberId, publisherId, EdgeType.PULL, "pull");
    }

    public synchronized void pullRetry(Identifier subscriberId, Identifier publisherId) {
        pull(subscriberId, publisherId, EdgeType.RETRY, "retry");
    }

    private void pull(Identifier subscriberId, Identifier publisherId, EdgeType edgeType, String edgeLabel) {
        String subscriberString;
        if (subscriberId == null) subscriberString = "root";
        else {
            subscriberString = subscriberId.toString();
            traceWriter().addNodeGroup(subscriberId.toString(), subscriberId.toString());
        }
        traceWriter().addMessage(subscriberString, publisherId.toString(), edgeType, edgeLabel);
        traceWriter().addNodeGroup(publisherId.toString(), publisherId.toString());
    }

    public <PACKET> void receive(Identifier publisherId, Identifier subscriberId, PACKET packet) {
        traceWriter().addMessage(publisherId.toString(), subscriberId.toString(), EdgeType.RECEIVE, packet.toString());
        traceWriter().addNodeGroup(subscriberId.toString(), subscriberId.toString());
        traceWriter().addNodeGroup(publisherId.toString(), publisherId.toString());
    }

    public void registerRoot(Identifier root, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(root.toString(), monitor.debugName().get(), EdgeType.ROOT, "reg_root");
    }

    public void rootFinalised(Identifier root, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(root.toString(), monitor.debugName().get(), EdgeType.ROOT_FINALISED, "root_finished");
    }

    public void finishRootNode(Identifier root, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(monitor.debugName().get(), root.toString(), EdgeType.ROOT_FINISH, "finished");
    }

    public void registerPath(Identifier subscriber, Identifier publisher, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(subscriber.toString(), monitor.debugName().get(), EdgeType.REGISTER, "reg_" + publisher.toString());
    }

    public void registerSource(Identifier source, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(source.toString(), monitor.debugName().get(), EdgeType.SOURCE, "reg_source");
    }

    public void sourceFinished(Identifier source, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(source.toString(), monitor.debugName().get(), EdgeType.SOURCE_FINISH, "source_finished");
    }

    public void createAnswer(Identifier publisher, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(publisher.toString(), monitor.debugName().get(), EdgeType.CREATE, "create");
    }

    public void consumeAnswer(Identifier subscriber, Actor.Driver<Monitor> monitor) {
        traceWriter().addMessage(subscriber.toString(), monitor.debugName().get(), EdgeType.CONSUME, "consume");
    }

    public synchronized void finishTrace() {
        if (traceWriter != null) traceWriter.finish();
    }

    public synchronized void exception() {
        if (traceWriter != null) traceWriter.finish();
    }

    private static class TraceWriter {

        private final AtomicReference<Path> path = new AtomicReference<>(null);
        private final long id;
        private final Path logDir;
        private final Map<String, Integer> clusterMap;
        private int messageNumber = 0;
        private PrintWriter writer;
        private int clusterIndex;

        private TraceWriter(long id, Path logDir) {
            this.id = id;
            this.logDir = logDir;
            this.clusterMap = new HashMap<>();
            this.clusterIndex = 0;
        }

        private String filename() {
            return String.format("reasoner_trace_transaction_id_%s.dot", id);
        }

        private void start() {
            path.set(logDir.resolve(filename()));
            try {
                LOG.debug("Writing reasoner traces to {}", path.get().toAbsolutePath());
                File file = path.get().toFile();
                if (!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
                    throw TypeDBException.of(REASONER_TRACING_DIRECTORY_COULD_NOT_BE_FOUND);
                }
                writer = new PrintWriter(file, "UTF-8");
                write(String.format(
                        "digraph request_%s {\n" +
                                "graph [fontsize=10 fontname=arial width=0.5 clusterrank=global]\n" +
                                "node [fontsize=12 fontname=arial width=0.5 shape=box style=filled]\n" +
                                "edge [fontsize=10 fontname=arial width=0.5]", id
                ));
            } catch (FileNotFoundException | UnsupportedEncodingException e) {
                e.printStackTrace();
                LOG.trace("Resolution tracing failed to start writing");
            }
        }

        private void finish() {
            if (path.get() == null) throw TypeDBException.of(REASONER_TRACING_FILE_COULD_NOT_BE_FOUND);
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

        private void endFile() {
            write("}");
        }

        private void write(String toWrite) {
            if (writer == null) throw TypeDBException.of(REASONER_TRACING_WRITE_FAILED);
            writer.println(toWrite);
        }

        private synchronized void addMessage(String sender, String subscriber, EdgeType edgeType, String packet) {
            writeEdge(sender, subscriber, edgeType.colour(), "m" + messageNumber + "_" + packet);
            messageNumber++;
        }

        private synchronized void addNodeGroup(String node, String group) {
            writeNodeToCluster(node, group);
        }

        private void writeEdge(String fromId, String toId, String colour, String label) {
            write(String.format("%s -> %s [style=bold,label=%s,color=%s];",
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(fromId))),
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(toId))),
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(label))),
                                doubleQuotes(colour)));

        }

        private int nextClusterId() {
            clusterIndex += 1;
            return clusterIndex;
        }

        private void writeNodeToCluster(String nodeId, String clusterLabel) {
            int ci = clusterMap.computeIfAbsent(clusterLabel, ignored -> nextClusterId());
            write(String.format("subgraph cluster_%d {%s; label=%s;}", ci, doubleQuotes(escapeNewlines(escapeDoubleQuotes(nodeId))),
                                doubleQuotes(escapeNewlines(escapeDoubleQuotes(clusterLabel)))));
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
        PULL("blue"),
        RETRY("blue"),
        RECEIVE("green"),
        ROOT("black"),
        ROOT_FINISH("brown"),
        ROOT_FINALISED("pink"),
        REGISTER("orange"),
        SOURCE("purple"),
        SOURCE_FINISH("brown"),
        CREATE("cyan"),
        CONSUME("red"),
        FORK("yellow");

        private final String colour;

        EdgeType(String colour) {
            this.colour = colour;
        }

        String colour() {
            return colour;
        }
    }

}
