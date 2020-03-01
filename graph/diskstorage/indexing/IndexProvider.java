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
 */

package grakn.core.graph.diskstorage.indexing;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.BaseTransaction;
import grakn.core.graph.diskstorage.BaseTransactionConfig;
import grakn.core.graph.diskstorage.BaseTransactionConfigurable;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * External index for querying.
 * An index can contain an arbitrary number of index stores which are updated and queried separately.
 */

public interface IndexProvider extends IndexInformation {
    /*
     * An obscure unicode character (•) provided as a convenience for implementations of #mapKey2Field, for
     * instance to replace spaces in property keys. See #777.
     */
    char REPLACEMENT_CHAR = '•';

    static void checkKeyValidity(String key) {
        Preconditions.checkArgument(!StringUtils.containsAny(key, new char[]{IndexProvider.REPLACEMENT_CHAR}),
                "Invalid key name containing reserved character %c provided: %s", IndexProvider.REPLACEMENT_CHAR, key);
    }

    /**
     * This method registers a new key for the specified index store with the given data type. This allows the IndexProvider
     * to prepare the index if necessary.
     * <p>
     * It is expected that this method is first called with each new key to inform the index of the expected type before the
     * key is used in any documents.
     *
     * @param store       Index store
     * @param key         New key to register
     * @param information Information on the key to register
     * @param tx          enclosing transaction
     */
    void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException;

    /**
     * Mutates the index (adds and removes fields or entire documents)
     *
     * @param mutations   Updates to the index. First map contains all the mutations for each store. The inner map contains
     *                    all changes for each document in an IndexMutation.
     * @param information Information on the keys used in the mutation accessible through KeyInformation.IndexRetriever.
     * @param tx          Enclosing transaction
     * see IndexMutation
     */
    void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Restores the index to the state of the primary data store as given in the {@code documents} variable. When this method returns, the index records
     * for the given documents exactly matches the provided data. Unlike #mutate(Map, KeyInformation.IndexRetriever, BaseTransaction)
     * this method does not do a delta-update, but entirely replaces the documents with the provided data or deletes them if the document content is empty.
     *
     * @param documents   The outer map maps stores to documents, the inner contains the documents mapping document ids to the document content which is a
     *                    list of IndexEntry. If that list is empty, that means this document should not exist and ought to be deleted.
     * @param information Information on the keys used in the mutation accessible through KeyInformation.IndexRetriever.
     * @param tx          Enclosing transaction
     */
    void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Executes the given query against the index.
     *
     * @param query       Query to execute
     * @param information Information on the keys used in the query accessible through KeyInformation.IndexRetriever.
     * @param tx          Enclosing transaction
     * @return The ids of all matching documents
     * see IndexQuery
     */
    Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Executes the given raw query against the index
     *
     * @param query       Query to execute
     * @param information Information on the keys used in the query accessible through KeyInformation.IndexRetriever.
     * @param tx          Enclosing transaction
     * @return Results objects for all matching documents (i.e. document id and score)
     * see RawQuery
     */
    Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Executes the given raw query against the index and returns the total hits. e.g. limit=0
     *
     * @param query       Query to execute
     * @param information Information on the keys used in the query accessible through KeyInformation.IndexRetriever.
     * @param tx          Enclosing transaction
     * @return Long total hits for query
     * see RawQuery
     */
    Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException;

    /**
     * Returns a transaction handle for a new index transaction.
     *
     * @return New Transaction Handle
     */
    BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException;

    /**
     * Closes the index
     */
    void close() throws BackendException;

    /**
     * Clears the index and removes all entries in all stores.
     */
    void clearStorage() throws BackendException;

    /**
     * Checks whether the index exists.
     *
     * @return Flag indicating whether index exists
     */
    boolean exists() throws BackendException;

}
