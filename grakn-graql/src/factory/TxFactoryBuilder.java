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

package grakn.core.factory;

import grakn.core.util.GraknConfigKey;
import grakn.core.janus.TxFactoryJanus;
import grakn.core.janus.TxFactoryJanusHadoop;
import grakn.core.util.ErrorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.MissingResourceException;
import java.util.concurrent.ConcurrentHashMap;

/**
*
 * A Transaction factory builder implementation used in Grakn Core.
 *
 * The factories in this class are cached based on factoryType+keyspace
 *
 * @author Marco Scoppetta
 */
public class TxFactoryBuilder {

    private static TxFactoryBuilder instance = null;
    private static final Map<String, TxFactory<?>> openFactories = new ConcurrentHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(TxFactoryBuilder.class);

    static final String IN_MEMORY = "in-memory";
    static final String PRODUCTION = "production";
    static final String DISTRIBUTED = "distributed";

    private TxFactoryBuilder() {
    }

    synchronized public static TxFactoryBuilder getInstance() {
        if (instance == null) {
            instance = new TxFactoryBuilder();
        }
        return instance;
    }

    public TxFactory<?> getFactory(EmbeddedGraknSession session, boolean isComputerFactory) {
        try {
            String factoryKey = session.config().getProperty(GraknConfigKey.KB_MODE);
            if (isComputerFactory) {
                factoryKey = session.config().getProperty(GraknConfigKey.KB_ANALYTICS);
            }

            return getFactory(factoryKey, session);
        } catch (MissingResourceException e) {
            throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
        }
    }

    /**
     * @param facetoryKey The string defining which factory should be used for creating the grakn graph.
     *                    A valid example includes: in-memory
     * @return A graph factory which produces the relevant expected graph.
     */
    private static TxFactory<?> getFactory(String facetoryKey, EmbeddedGraknSession session) {
        String key = facetoryKey + session.keyspace();
        return openFactories.computeIfAbsent(key, (k) -> newFactory(facetoryKey, session));
    }

    private static TxFactory<?> newFactory(String factoryKey, EmbeddedGraknSession session) {
        TxFactory<?> txFactory;
        switch (factoryKey) {
            case IN_MEMORY:
                txFactory = new TxFactoryTinker(session);
                break;
            case PRODUCTION:
                txFactory = new TxFactoryJanus(session);
                break;
            case DISTRIBUTED:
                txFactory = new TxFactoryJanusHadoop(session);
                break;
            default:
                throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryKey));
        }

        LOG.trace("New factory created " + txFactory);
        return txFactory;
    }
}
