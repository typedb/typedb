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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller.response;

import ai.grakn.engine.Jacksonisable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.List;

/**
 * <p>
 *     Wraps the {@link Thing}s of a {@link Type} with some metadata
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
@JsonInclude(Include.NON_NULL)
public abstract class Things implements Jacksonisable{

    @JsonProperty("@id")
    public abstract Link selfLink();

    @JsonProperty
    public abstract List<Thing> instances();

    @JsonProperty
    @Nullable
    public abstract Link next();

    @JsonProperty
    @Nullable
    public abstract Link previous();

    public static Things create(
            @JsonProperty("@id") Link selfLink,
            @JsonProperty("instances") List<Thing> instances,
            @Nullable @JsonProperty("next") Link next,
            @Nullable @JsonProperty("previous") Link previous
    ){
        return new AutoValue_Things(selfLink, instances, next, previous);
    }
}
