package io.mindmaps.graql.internal.printer;

import com.google.common.collect.Maps;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.Printer;
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

        if (concept.type() != null) {
            json.set("isa", concept.type().getId());
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
