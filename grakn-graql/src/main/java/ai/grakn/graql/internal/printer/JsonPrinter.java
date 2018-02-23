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

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Concept;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Var;
import ai.grakn.util.CommonUtil;
import mjson.Json;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static mjson.Json.nil;

class JsonPrinter implements Printer<Json> {
    @Override
    public final String complete(Json builder) {
        return builder.toString();
    }

    @Override
    public Json build(Concept concept) {
        Json json = Json.object("id", concept.getId().getValue());

        if (concept.isSchemaConcept()) {
            json.set("name", concept.asSchemaConcept().getLabel().getValue());
            SchemaConcept superConcept = concept.asSchemaConcept().sup();
            if (superConcept != null) json.set("sub", superConcept.getLabel().getValue());
        } else if (concept.isThing()) {
            json.set("isa", concept.asThing().type().getLabel().getValue());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }

        if (concept.isAttribute()) {
            json.set("value", concept.asAttribute().getValue());
        }

        if (concept.isRule()) {
            Pattern when = concept.asRule().getWhen();
            if (when != null) {
                json.set("when", when.toString());
            }
            Pattern then = concept.asRule().getThen();
            if (then != null) {
                json.set("then", then.toString());
            }
        }

        return json;
    }

    @Override
    public final Json build(boolean bool) {
        return Json.make(bool);
    }

    @Override
    public final Json build(Optional<?> optional) {
        return optional.map(item -> build(item)).orElse(nil());
    }

    @Override
    public final Json build(Collection<?> collection) {
        return Json.make(collection.stream().map(item -> build(item)).collect(toList()));
    }

    @Override
    public final Json build(Map<?, ?> map) {
        Json json = Json.object();

        map.forEach((Object key, Object value) -> {
            if (key instanceof Var) key = ((Var) key).getValue();
            String keyString = key == null ? "" : key.toString();
            json.set(keyString, build(value));
        });

        return json;
    }

    @Override
    public final Json buildDefault(Object object) {
        return Json.make(object);
    }
}
