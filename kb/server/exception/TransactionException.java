/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.kb.server.exception;

import com.google.common.base.Preconditions;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.kb.server.Transaction;

import javax.annotation.Nullable;

/**
 * Illegal Mutation Exception
 * This exception is thrown to prevent the user from incorrectly mutating the graph.
 * For example, when attempting to create instances for an abstract type, this exception is thrown.
 */
public class TransactionException extends GraknException {

    TransactionException(String error) {
        super(error);
    }

    protected TransactionException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static TransactionException create(String error) {
        return new TransactionException(error);
    }

    /**
     * Thrown when using an unsupported datatype with resources
     */
    public static TransactionException unsupportedDataType(Object value) {
        return unsupportedDataType(value.getClass());
    }

    public static TransactionException unsupportedDataType(Class<?> clazz) {
        return unsupportedDataType(clazz.getName());
    }

    /**
     * Thrown when attempting to open a transaction which is already open
     */
    public static TransactionException transactionOpen(Transaction tx) {
        return create(ErrorMessage.TRANSACTION_ALREADY_OPEN.getMessage(tx.keyspace()));
    }

    /**
     * Thrown when attempting to open an invalid type of transaction
     */
    public static TransactionException transactionInvalid(Object tx) {
        return create("Unknown type of transaction [" + tx + "]");
    }

    /**
     * Thrown when attempting to mutate a read only transaction
     */
    public static TransactionException transactionReadOnly(Transaction tx) {
        return create(ErrorMessage.TRANSACTION_READ_ONLY.getMessage(tx.keyspace()));
    }

    /**
     * Thrown when attempting to use the graph when the transaction is closed
     */
    public static TransactionException transactionClosed(@Nullable Transaction tx, @Nullable String reason) {
        if (reason == null) {
            Preconditions.checkNotNull(tx);
            return create(ErrorMessage.TX_CLOSED.getMessage(tx.keyspace()));
        } else {
            return create(reason);
        }
    }

    /**
     * Thrown when a thread tries to do operations across thread boundaries, which is disallowed with thread bound janus transactions
     */
    public static TransactionException notInOriginatingThread() {
        return new TransactionException(ErrorMessage.TRANSACTION_CHANGED_THREAD.getMessage());
    }


    /**
     * Thrown when creating an invalid KeyspaceImpl
     */
    public static TransactionException invalidKeyspaceName(String keyspace) {
        return create(ErrorMessage.INVALID_KEYSPACE_NAME.getMessage(keyspace));
    }

}
