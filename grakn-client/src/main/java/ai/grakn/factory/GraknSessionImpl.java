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

import ai.grakn.GraknSession;
import ai.grakn.Keyspace;
import ai.grakn.remote.GraknRemoteSession;
import ai.grakn.util.SimpleURI;

/**
 * @author Felix Chapman
 */
public class GraknSessionImpl {

    // TODO: this is sneaky of me
    //This must remain public because it is accessed via reflection
    public static GraknSession create(Keyspace keyspace, String engineUri){
        return GraknRemoteSession.create(keyspace, new SimpleURI(engineUri));
    }
}
