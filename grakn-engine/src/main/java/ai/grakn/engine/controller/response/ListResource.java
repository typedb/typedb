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

package ai.grakn.engine.controller.response;

import ai.grakn.util.CommonUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * <p>
 *     Wraps a list of items with a self-link and a custom named key
 * </p>
 *
 * <p>
 *     This uses a custom serializer and deserializer because the {@link ListResource#key()} can be dynamically
 *     specified. This reduces a lot of boilerplate response interfaces.
 * </p>
 *
 * @param <T> The type of things the list contains
 *
 * @author Felix Chapman
 */
@AutoValue
@JsonDeserialize(using=ListResource.Deserializer.class)
public abstract class ListResource<T> extends JsonSerializable.Base {

    public abstract Link selfLink();

    /**
     * The key for the items in the serialized JSON object.
     */
    public abstract String key();

    public abstract List<T> items();

    public static <T> ListResource<T> create(Link selfLink, String key, List<T> items) {
        return new AutoValue_ListResource<>(selfLink, key, ImmutableList.copyOf(items));
    }

    @Override
    public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeObjectField("@id", selfLink());
        gen.writeObjectField(key(), items());
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
            throws IOException {
        // TODO: maybe we need to do some other jackson wizardry here?
        serialize(gen, serializers);
    }

    static class Deserializer<T> extends JsonDeserializer<ListResource<T>> {

        private static final ImmutableSet<String> RECOGNISED_KEYS = ImmutableSet.of("@id");

        @Override
        public ListResource<T> deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);

            // Find the first unrecognised field that is an array
            Optional<Map.Entry<String, JsonNode>> field = CommonUtil.stream(node.fields())
                    .filter(f -> !RECOGNISED_KEYS.contains(f.getKey()))
                    .filter(f -> f.getValue().isArray())
                    // There might be multiple fields that are arrays, so we pick the first one and ignore the others
                    .sorted().findFirst();

            if (!field.isPresent()) {
                throw InvalidFormatException.from(parser, ListResource.class, "Expected a field containing a list");
            }

            String key = field.get().getKey();
            JsonNode value = field.get().getValue();
            List<T> items = value.traverse().readValueAs(new TypeReference<List<T>>(){});

            Link selfLink = node.get("@id").traverse().readValueAs(Link.class);
            return ListResource.create(selfLink, key, items);
        }
    }
}
