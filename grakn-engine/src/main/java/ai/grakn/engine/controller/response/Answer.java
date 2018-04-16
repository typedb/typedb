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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 *     Response wrapper for {@link ai.grakn.graql.admin.Answer}
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
@AutoValue
public abstract class Answer {

    @JsonValue
    public abstract Map<String, Concept> conceptMap();


    @JsonCreator
    public static Answer create(Map<String, Concept> conceptMap){
        return new AutoValue_Answer(conceptMap);
    }

    public static Answer create(ai.grakn.graql.admin.Answer answer){
        Map<String, Concept> conceptMap = answer.map().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().getValue(),
                entry -> ConceptBuilder.build(entry.getValue())
        ));
        return create(conceptMap);
    }
}
