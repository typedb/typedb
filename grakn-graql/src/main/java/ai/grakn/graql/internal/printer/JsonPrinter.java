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

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Concept;
import ai.grakn.graql.Printer;
import com.google.common.collect.Maps;
import mjson.Json;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class JsonPrinter implements Printer<Json> {
    @Override
    public String build(Json builder) {
        return builder.toString();
    }

    @Override
    public Json graqlString(boolean inner, Concept concept) {
        Json json = Json.object("id", concept.getId());

        if (concept.isType()) {
            json.set("name", concept.asType().getName());
        }

        if (concept.type() != null) {
            json.set("isa", concept.type().getName());
        }

        if (concept.isResource()) {
            json.set("value", concept.asResource().getValue());
        }

        if (concept.isRule()) {
            json.set("lhs", concept.asRule().getLHS().toString());
            json.set("rhs", concept.asRule().getRHS().toString());
        }

        return json;
    }

    @Override
    public Json graqlString(boolean inner, boolean bool) {
        return Json.make(bool);
    }

    @Override
    public Json graqlString(boolean inner, Optional<?> optional) {
        return optional.map(item -> graqlString(inner, item)).orElse(null);
    }

    @Override
    public Json graqlString(boolean inner, Collection<?> collection) {
        return Json.make(collection.stream().map(item -> graqlString(inner, item)).collect(toList()));
    }

    @Override
    public Json graqlString(boolean inner, Map<?, ?> map) {
        return Json.make(Maps.transformValues(map, value -> graqlString(true, value)));
    }

    @Override
    public Json graqlStringDefault(boolean inner, Object object) {
        return Json.make(object);
    }
}
