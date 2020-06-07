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

import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.schema.Parameter;

/**
 * Helper class that provides information on the data type and additional parameters that
 * form the definition of a key in an index.
 * <p>
 * <p>
 * So, given a key, its associated KeyInformation provides the details on what data type the key's associated values
 * have and whether this key has been configured with any additional parameters (that might provide information on how
 * the key should be indexed).
 *
 * <p>
 * <p>
 * IndexRetriever returns KeyInformation for a given store and given key. This will be provided to an
 * index when the key is not fixed in the context, e.g. in IndexProvider#mutate(java.util.Map, IndexRetriever, BaseTransaction)
 *
 * <p>
 * <p>
 * Retriever returns IndexRetriever for a given index identified by its name. This is only used
 * internally to pass IndexRetrievers around.
 */
public interface KeyInformation {

    /**
     * Returns the data type of the key's values.return
     */
    Class<?> getDataType();

    /**
     * Returns the parameters of the key's configuration.
     */
    Parameter[] getParameters();

    /**
     * Returns the Cardinality for this key.
     */
    Cardinality getCardinality();


    interface StoreRetriever {

        /**
         * Returns the KeyInformation for a particular key for this store
         */
        KeyInformation get(String key);

    }

    interface IndexRetriever {

        /**
         * Returns the KeyInformation for a particular key in a given store.
         */
        KeyInformation get(String store, String key);

        /**
         * Returns a StoreRetriever for the given store on this IndexRetriever
         */
        StoreRetriever get(String store);

    }

    interface Retriever {

        /**
         * Returns the IndexRetriever for a given index.
         */
        IndexRetriever get(String index);

    }

}
