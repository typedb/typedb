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
import ai.grakn.engine.Jacksonisable;
import ai.grakn.graql.Printer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * <p>
 *     This class is used to convert the responses from graql queries into objects which can be Jacksonised into their
 *     correct Json representation.
 * </p>
 */
public class JacksonPrinter implements Printer<Jacksonisable>{
    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public String build(Jacksonisable object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error during serialising {%s}", object), e);
        }
    }

    @Override
    public Jacksonisable graqlString(boolean inner, Concept concept) {
        return null;
    }

    @Override
    public Jacksonisable graqlString(boolean inner, boolean bool) {
        return null;
    }

    @Override
    public Jacksonisable graqlStringDefault(boolean inner, Object object) {
        return null;
    }

    @Override
    public Jacksonisable graqlString(boolean inner, Map map) {
        return null;
    }

    @Override
    public Jacksonisable graqlString(boolean inner, Collection collection) {
        return null;
    }

    @Override
    public Jacksonisable graqlString(boolean inner, Optional optional) {
        return null;
    }
}
