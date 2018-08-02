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

package ai.grakn.factory;

import ai.grakn.GraknConfigKey;
import ai.grakn.util.ErrorMessage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

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

    //This is used to map grakn value properties into the underlying properties
    private static final Map<String, String> factoryMapper = ImmutableMap.of(
            "in-memory", "ai.grakn.factory.TxFactoryTinker",
            "production", "ai.grakn.factory.TxFactoryJanus",
            "distributed", "ai.grakn.factory.TxFactoryJanusHadoop");

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
