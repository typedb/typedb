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

package grakn.core.server.session;

import grakn.core.server.session.olap.TransactionOLAPFactory;
import grakn.core.server.session.oltp.TransactionOLTPFactory;
import grakn.core.util.GraknConfigKey;
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
 */
public class TransactionFactoryBuilder {

    private static TransactionFactoryBuilder instance = null;
    private static final Map<String, TransactionFactory<?>> openFactories = new ConcurrentHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(TransactionFactoryBuilder.class);

    static final String PRODUCTION = "production";
    static final String DISTRIBUTED = "distributed";

    private TransactionFactoryBuilder() {
    }

    synchronized public static TransactionFactoryBuilder getInstance() {
        if (instance == null) {
            instance = new TransactionFactoryBuilder();
        }
        return instance;
    }

    public TransactionFactory<?> getFactory(SessionImpl session, boolean isComputerFactory) {
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
     * @return A graph factory which produces the relevant expected graph.
     */
    private static TransactionFactory<?> getFactory(String facetoryKey, SessionImpl session) {
        String key = facetoryKey + session.keyspace();
        return openFactories.computeIfAbsent(key, (k) -> newFactory(facetoryKey, session));
    }

    private static TransactionFactory<?> newFactory(String factoryKey, SessionImpl session) {
        TransactionFactory<?> transactionFactory;
        switch (factoryKey) {
            case PRODUCTION:
                transactionFactory = new TransactionOLTPFactory(session);
                break;
            case DISTRIBUTED:
                transactionFactory = new TransactionOLAPFactory(session);
                break;
            default:
                throw new IllegalArgumentException(ErrorMessage.INVALID_FACTORY.getMessage(factoryKey));
        }

        LOG.trace("New factory created " + transactionFactory);
        return transactionFactory;
    }
}
