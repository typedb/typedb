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

import grakn.core.graph.graphdb.query.JanusGraphPredicate;

/**
 * An IndexInformation gives basic information on what a particular IndexProvider supports.
 *
 */

public interface IndexInformation {

    /**
     * Whether the index supports executing queries with the given predicate against a key with the given information
     * @param information
     * @param janusgraphPredicate
     * @return
     */
    boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate);

    /**
     * Whether the index supports indexing a key with the given information
     * @param information
     * @return
     */
    boolean supports(KeyInformation information);


    /**
     * Adjusts the name of the key so that it is a valid field name that can be used in the index.
     * JanusGraph stores this information and will use the returned name in all interactions with the index.
     * <p>
     * Note, that mapped field names (either configured on a per key basis or through a global configuration)
     * are not adjusted and handed to the index verbatim.
     *
     * @param key
     * @param information
     * @return
     */
    String mapKey2Field(String key, KeyInformation information);

    /**
     * The features of this index
     * @return
     */
    IndexFeatures getFeatures();

}
