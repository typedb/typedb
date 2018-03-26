/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

import ai.grakn.GraknConfigKey;
import ai.grakn.util.ErrorMessage;
import com.google.common.annotations.VisibleForTesting;

import java.util.Map;
import java.util.MissingResourceException;
import java.util.concurrent.ConcurrentHashMap;

/**
*
 * {@link TxFactoryBuilder} implementation used in Grakn core.
 *
 * The factories in this class are cached based on factoryType+keyspace
 *
 * @author Marco Scoppetta
 */
public class GraknTxFactoryBuilder extends TxFactoryBuilder {

    private static final Map<String, TxFactory<?>> openFactories = new ConcurrentHashMap<>();

    private static TxFactoryBuilder instance = null;

    private GraknTxFactoryBuilder() {
    }

    synchronized public static TxFactoryBuilder getInstance() {
        if (instance == null) {
            instance = new GraknTxFactoryBuilder();
        }
        return instance;
    }

    public TxFactory<?> getFactory(EmbeddedGraknSession session, boolean isComputerFactory) {
        try {
            String factoryKey = session.config().getProperty(GraknConfigKey.KB_MODE);
            if (isComputerFactory) {
                factoryKey = session.config().getProperty(GraknConfigKey.KB_ANALYTICS);
            }

            String factoryType = factoryMapper.get(factoryKey);
            return getFactory(factoryType, session);
        } catch (MissingResourceException e) {
            throw new IllegalArgumentException(ErrorMessage.MISSING_FACTORY_DEFINITION.getMessage());
        }
    }

    /**
     * @param factoryType The string defining which factory should be used for creating the grakn graph.
     *                    A valid example includes: ai.grakn.factory.TxFactoryTinker
     * @return A graph factory which produces the relevant expected graph.
     */
    private static TxFactory<?> getFactory(String factoryType, EmbeddedGraknSession session) {
        String key = factoryType + session.keyspace();
        return openFactories.computeIfAbsent(key, (k) -> newFactory(factoryType, session));
    }

    /**
     * Clears all connections.
     */
    //TODO Should this close each of the factories (and wait for all open transactions to be closed?)
    //TODO Calling this from within the code causes a memory leak
    @VisibleForTesting
    public static void refresh() {
        openFactories.clear();
    }
}
