/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.reasoner.resolution.framework;

import grakn.core.common.exception.GraknException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Reasoner.REASONER_LOGGING_CALL_TO_FINISH_BEFORE_START;
import static grakn.core.common.exception.ErrorMessage.Reasoner.REASONER_LOGGING_HAS_NOT_BEEN_INITIALISED;

public final class ResolutionLogger {

    private static final Logger LOG = LoggerFactory.getLogger(ResolutionLogger.class);

    private static ResolutionLogger INSTANCE;
    private static PrintWriter writer;
    private static int rootRequestNumber = 0;
    private static int messageNumber = 0;
    private static Path logDir = null;
    private static final AtomicReference<Path> path = new AtomicReference<>(null);

    private ResolutionLogger(Path logDir) {
        ResolutionLogger.logDir = logDir;
    }

    public static ResolutionLogger initialiseOrGet(Path logDir) {
        if (ResolutionLogger.logDir != null && !ResolutionLogger.logDir.equals(logDir)) {
            throw GraknException.of(ILLEGAL_STATE);
        }
        if (INSTANCE == null) {
            INSTANCE = new ResolutionLogger(logDir);
        }
        return INSTANCE;
    }

    public static ResolutionLogger get() {
        if (INSTANCE == null) throw GraknException.of(REASONER_LOGGING_HAS_NOT_BEEN_INITIALISED);
        return INSTANCE;
    }

    synchronized void request(Resolver<?> sender, Resolver<?> receiver, int iteration, String conceptMap) {
        addMessage(sender, receiver, iteration, EdgeType.REQUEST, conceptMap);
    }

    synchronized void responseAnswer(Resolver<?> sender, Resolver<?> receiver, int iteration, String conceptMap) {
        addMessage(sender, receiver, iteration, EdgeType.ANSWER, conceptMap);
    }

    synchronized void responseExhausted(Resolver<?> sender, Resolver<?> receiver, int iteration) {
        addMessage(sender, receiver, iteration, EdgeType.EXHAUSTED, "");
    }

    private void addMessage(Resolver<?> sender, Resolver<?> receiver, int iteration, EdgeType edgeType, String conceptMap) {
        writeEdge(sender.name(), receiver.name(), iteration, edgeType.colour(), messageNumber, conceptMap);
        messageNumber ++;
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

    public synchronized void start() {
        messageNumber = 0;
        path.set(logDir.resolve(filename()));
        try {
            File file = path.get().toFile();
            boolean ignore = file.getParentFile().mkdirs();
            writer = new PrintWriter(file, "UTF-8");
            startFile();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
            LOG.trace("Resolution logging failed to start writing");
        }
    }

    private void startFile() {
        write(String.format(
                "digraph %s {\n" +
                        "node [fontsize=12 fontname=arial width=0.5 shape=box style=filled]\n" +
                        "edge [fontsize=10 fontname=arial width=0.5]",
                filename()));
    }

    private String filename() {
        return String.format("resolution_log_request_%d.dot", rootRequestNumber);
    }

    private void endFile() {
        write("}");
    }

    private void write(String toWrite) {
        assert writer != null;
        writer.println(toWrite);
    }

    public synchronized void finish() {
        if (path.get() == null) throw GraknException.of(REASONER_LOGGING_CALL_TO_FINISH_BEFORE_START);
        endFile();
        try {
            LOG.trace("Resolution log written to {}", path.get().toAbsolutePath());
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.trace("Resolution logging failed to write to file");
        }
        rootRequestNumber += 1;
        path.set(null);
    }

    enum EdgeType {
        EXHAUSTED("red"),
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

}
