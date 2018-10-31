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

package ai.grakn.core.server.controller.response;

import ai.grakn.core.server.Jacksonisable;
import ai.grakn.util.REST;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * @author Felix Chapman
 */
@AutoValue
public abstract class Root implements Jacksonisable {

    public static Root create() {
        return create(Link.create(REST.WebPath.ROOT), Link.create(REST.WebPath.KB));
    }

    @JsonCreator
    public static Root create(@JsonProperty("@id") Link selfLink, @JsonProperty("keyspaces") Link kb) {
        return new AutoValue_Root(selfLink, kb);
    }

    @JsonProperty("@id")
    public abstract Link selfLink();

    @JsonProperty("keyspaces")
    public abstract Link kb();
}
