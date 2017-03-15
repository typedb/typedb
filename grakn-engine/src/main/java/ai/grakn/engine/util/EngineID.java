/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static org.apache.commons.lang.StringEscapeUtils.escapeJava;

/**
 * <p>
 *     Assigns a random ID to the current instance of Engine.
 * </p>
 *
 * @author Denis Lobanov, Felix Chapman
 */
public class EngineID implements Serializable {

    private static final long serialVersionUID = 8846772120873129437L;

    private static final Logger LOG = LoggerFactory.getLogger(EngineID.class);

    private final String value;

    private EngineID(String value) {
        this.value = value;
    }

    public static EngineID of(String value) {
        return new EngineID(value);
    }

    public static EngineID me() {
        String hostName = "";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            LOG.error("Could not get system hostname: ", e);
        }

        String value = hostName+"-"+UUID.randomUUID().toString();

        return EngineID.of(value);
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EngineID engineID = (EngineID) o;

        return value.equals(engineID.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "EngineId.of(" + escapeJava(value) + ")";
    }
}
