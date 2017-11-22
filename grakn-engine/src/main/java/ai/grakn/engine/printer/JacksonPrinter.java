/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
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

package ai.grakn.engine.printer;

import ai.grakn.concept.Concept;
import ai.grakn.engine.controller.response.Answer;
import ai.grakn.engine.controller.response.ConceptBuilder;
import ai.grakn.graql.Printer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *     This class is used to convert the responses from graql queries into objects which can be Jacksonised into their
 *     correct Json representation.
 * </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class JacksonPrinter implements Printer<Object>{
    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public String build(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error during serialising {%s}", object), e);
        }
    }

    @Override
    public Object graqlString(boolean inner, Concept concept) {
        return ConceptBuilder.build(concept);
    }

    @Override
    public Object graqlString(boolean inner, ai.grakn.graql.admin.Answer answer) {
        return Answer.create(answer);
    }

    @Override
    public Object graqlString(boolean inner, boolean bool) {
        return bool;
    }

    @Override
    public Object graqlStringDefault(boolean inner, Object object) {
        throw new UnsupportedOperationException(String.format("Cannot wrap {%s} due to it being an unknown format", object));
    }

    @Override
    public Object graqlString(boolean inner, Map map) {
        Set<Map.Entry> entries = map.<Map.Entry>entrySet();
        return entries.stream().collect(Collectors.toMap(
                entry -> graqlString(inner, entry.getKey()),
                entry -> graqlString(inner, entry.getKey())
        ));
    }

    @Override
    public Object graqlString(boolean inner, Collection collection) {
        return collection.stream().map(object -> graqlString(inner, object)).collect(Collectors.toSet());
    }

    @Override
    public Object graqlString(boolean inner, Optional optional) {
        return optional.map(o -> graqlString(inner, o)).orElseGet(null);
    }
}
