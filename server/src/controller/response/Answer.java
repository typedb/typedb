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

import ai.grakn.graql.answer.ConceptMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>
 *     Response wrapper for {@link ConceptMap}
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

    public static Answer create(ConceptMap map){
        Map<String, Concept> conceptMap = map.map().entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().getValue(),
                entry -> ConceptBuilder.build(entry.getValue())
        ));
        return create(conceptMap);
    }
}
