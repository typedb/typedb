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

package ai.grakn.factory;

import ai.grakn.Keyspace;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.GraknTestUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * <p>
 *     Test {@link ai.grakn.GraknSession}
 * </p>
 *
 * <p>
 *     A {@link ai.grakn.GraknSession} used for testing which bypasses the need to hit the REST controller responsible
 *     for feeding in the appropriate config file.
 *
 *     This session can be used to provide {@link ai.grakn.GraknTx}'s dependent on either a TinkerGraph or
 *     JanusGraph
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class GraknSessionLocal extends GraknSessionImpl{
    private final static String JANUS_CONFIG_LOCATION = CommonUtil.getProjectPath() + "/../conf/test/janus/grakn.properties";

    private GraknSessionLocal(Keyspace keyspace, String engineUri, Properties properties) {
        super(keyspace, engineUri, properties, false);
    }

    public static GraknSessionLocal create(Keyspace keyspace) {
        return new GraknSessionLocal(keyspace, "fake-engine-uri", null);
    }

    public static GraknSessionLocal create(Keyspace keyspace, String engineUri, Properties properties) {
        return new GraknSessionLocal(keyspace, engineUri, properties);
    }

    @Override
    Properties getTxProperties() {
        if (GraknTestUtil.usingJanus()) {
            return getTxJanusProperties();
        } else {
            return getTxInMemoryProperties();
        }
    }

    /**
     * Gets the Janus Properties directly by reading them from disk.
     * This is the part which bypasses the need for a REST endpoint.
     *
     * @return the properties needed to build a JanusGraph
     */
    private Properties getTxJanusProperties() {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(JANUS_CONFIG_LOCATION)){
            properties.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(JANUS_CONFIG_LOCATION), e);
        }
        return properties;
    }
}
