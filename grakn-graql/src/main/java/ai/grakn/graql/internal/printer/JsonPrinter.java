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
import ai.grakn.concept.Type;
import ai.grakn.graql.Printer;
import ai.grakn.graql.VarName;
import mjson.Json;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static mjson.Json.nil;

class JsonPrinter implements Printer<Json> {
    @Override
    public final String build(Json builder) {
        return builder.toString();
    }

    @Override
    public Json graqlString(boolean inner, Concept concept) {
        Json json = Json.object("id", concept.getId().getValue());

        if (concept.isType()) {
            json.set("name", concept.asType().getLabel().getValue());
            Type superType = concept.asType().superType();
            if (superType != null) json.set("sub", superType.getLabel().getValue());
        } else {
            json.set("isa", concept.asInstance().type().getLabel().getValue());
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
    public final Json graqlString(boolean inner, boolean bool) {
        return Json.make(bool);
    }

    @Override
    public final Json graqlString(boolean inner, Optional<?> optional) {
        return optional.map(item -> graqlString(inner, item)).orElse(nil());
    }

    @Override
    public final Json graqlString(boolean inner, Collection<?> collection) {
        return Json.make(collection.stream().map(item -> graqlString(inner, item)).collect(toList()));
    }

    @Override
    public final Json graqlString(boolean inner, Map<?, ?> map) {
        Json json = Json.object();

        map.forEach((Object key, Object value) -> {
            if (key instanceof VarName) key = ((VarName) key).getValue();
            String keyString = key == null ? "" : key.toString();
            json.set(keyString, graqlString(true, value));
        });

        return json;
    }

    @Override
    public final Json graqlStringDefault(boolean inner, Object object) {
        return Json.make(object);
    }
}
