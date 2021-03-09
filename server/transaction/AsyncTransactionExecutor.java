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
 *
 */

package grakn.core.server.transaction;

import grakn.common.concurrent.NamedThreadFactory;
import grakn.core.concurrent.executor.EventLoopExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncTransactionExecutor extends EventLoopExecutor<TransactionService.Event> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncTransactionExecutor.class);
    private static final String GRAKN_CORE_TRANSACTION_THREAD_NAME = "grakn-core-transaction";

    public AsyncTransactionExecutor(int executors, int queuePerExecutor) {
        super(executors, queuePerExecutor, NamedThreadFactory.create(GRAKN_CORE_TRANSACTION_THREAD_NAME));
    }

    @Override
    public void onEvent(TransactionService.Event event) {
        event.transactionService().executeAsync(event.request());
    }

    @Override
    public void onException(TransactionService.Event event, Throwable exception) {
        LOG.error(event.toString(), exception);
    }
}
