/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package ai.grakn.engine.util;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * <p>
 *     Assigns a random ID to the current instance of Engine.
 * </p>
 *
 * @author Denis Lobanov, Felix Chapman
 */
@AutoValue
public abstract class EngineID implements Serializable {
    private static final long serialVersionUID = 8846772120873129437L;
    private static final Logger LOG = LoggerFactory.getLogger(EngineID.class);

    @CheckReturnValue
    @JsonValue
    public abstract String getValue();

    @CheckReturnValue
    public static EngineID of(String value) {
        return new AutoValue_EngineID(value);
    }

    @CheckReturnValue
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
}
