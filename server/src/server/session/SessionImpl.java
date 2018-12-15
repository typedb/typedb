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

import grakn.core.common.config.Config;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.keyspace.Keyspace;
import grakn.core.server.session.olap.TransactionOLAP;
import grakn.core.server.session.olap.TransactionOLAPFactory;
import grakn.core.server.session.oltp.TransactionOLTP;
import grakn.core.server.session.oltp.TransactionOLTPFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class facilitates the construction of Transaction by determining which factory should be built.
 */
public class SessionImpl implements Session {
    private static final Logger LOG = LoggerFactory.getLogger(SessionImpl.class);

    private static final Map<String, TransactionOLTPFactory> openOLTPFactories = new ConcurrentHashMap<>();
    private static final Map<String, TransactionOLAPFactory> openOLAPFactories = new ConcurrentHashMap<>();

    private static final String PRODUCTION = "production";
    private static final String DISTRIBUTED = "distributed";

    private final TransactionOLTPFactory transactionOLTPFactory;
    private final TransactionOLAPFactory transactionOLAPFactory;

    private final Keyspace keyspace;
    private final Config config;

    //References so we don't have to open a tx just to check the count of the transactions
    private TransactionImpl<?> tx = null;

    /**
     * Instantiates {@link SessionImpl}
     *
     * @param keyspace to which keyspace the session should be bound to
     * @param config   config to be used. If null is supplied, it will be created
     */
    SessionImpl(Keyspace keyspace, @Nullable Config config) {
        Objects.requireNonNull(keyspace);

        this.keyspace = keyspace;
        this.config = config;
        this.transactionOLTPFactory = getOLTPFactory(this);
        this.transactionOLAPFactory = getOLAPFactory(this);
    }

    /**
     * Creates a {@link SessionImpl} specific for internal use (within Grakn Server),
     * using provided Grakn configuration
     */
    public static SessionImpl create(Keyspace keyspace, Config config) {
        return new SessionImpl(keyspace, config);
    }

    public static SessionImpl create(Keyspace keyspace) {
        return new SessionImpl(keyspace, Config.create());
    }

    /**
     * @return A graph factory which produces the relevant expected graph.
     */
    private static TransactionOLTPFactory getOLTPFactory(SessionImpl session) {
        String key = PRODUCTION + "_" + session.keyspace();
        return openOLTPFactories.computeIfAbsent(key, (k) -> {
            TransactionOLTPFactory transactionFactory = new TransactionOLTPFactory(session);
            LOG.trace("New factory created " + transactionFactory);
            return transactionFactory;
        });
    }

    private static TransactionOLAPFactory getOLAPFactory(SessionImpl session) {
        String key = DISTRIBUTED + "_" + session.keyspace();
        return openOLAPFactories.computeIfAbsent(key, (k) -> {
            TransactionOLAPFactory transactionFactory = new TransactionOLAPFactory(session);
            LOG.trace("New factory created " + transactionFactory);
            return transactionFactory;
        });
    }


    @Override
    public TransactionImpl transaction(Transaction.Type transactionType) {
        tx = transactionOLTPFactory.openOLTP(transactionType);
        return tx;
    }

    /**
     * Get a new or existing TransactionOLAP.
     *
     * @return A new or existing Grakn graph computer
     * @see TransactionOLAP
     */
    @CheckReturnValue
    public TransactionOLAP transactionOLAP() {
        return transactionOLAPFactory.openOLAP();
    }

    @Override
    public void close() throws TransactionException {
        if (tx != null) {
            tx.closeSession();
            if (tx instanceof TransactionOLTP) {
                ((TransactionOLTP) tx).closeOpenTransactions();
            }
        }
    }

    @Override
    public Keyspace keyspace() {
        return keyspace;
    }

    /**
     * The config options of this {@link Session} which were passed in at the time of construction
     *
     * @return The config options of this {@link Session}
     */
    public Config config() {
        return config;
    }
}
