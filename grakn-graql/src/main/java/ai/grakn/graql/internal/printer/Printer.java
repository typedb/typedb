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

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.admin.Answer;
import mjson.Json;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * An interface for print objects in Graql responses (e.g. {@link Integer}s and {@link Answer}s into a String).
 *
 * The intermediate {@link Builder} type is used when the final type is different to the "in-progress" type when
 * creating it. For example, you may want to use a {@link StringBuilder} for {@link Builder} (for efficiency).
 *
 * If you don't need a {@link Builder} type, then set it to the same type as String and implement
 * {@link #complete(Builder)} to just return its argument.
 *
 * @param <Builder> An intermediate builder type that can be changed into a String
 * @author Haikal Pribadi
 */
public abstract class Printer<Builder> {

    /**
     * Constructs a special type of Printer: StringPrinter
     *
     * @param colorize boolean parameter to determine if the String output should be colorized
     * @param attributeTypes list of attribute types that should be included in the String output
     * @return a new StringPrinter object
     */
    public static Printer<StringBuilder> stringPrinter(boolean colorize, AttributeType... attributeTypes) {
        return new StringPrinter(colorize, attributeTypes);
    }

    /**
     * Constructs a special type of Printer: JsonPrinter
     *
     * @return a new JsonPrinter object
     */
    public static Printer<Json> jsonPrinter() {
        return new JsonPrinter();
    }

    /**
     * Convert a stream of objects into a stream of Strings to be printed
     *
     * @param objects the objects to be printed
     * @return the stream of String print output for the object
     */
    public Stream<String> toStream(Stream<?> objects) {
        if (objects == null) return Stream.empty();
        return objects.map(this::toString);
    }

    /**
     * Convert any object into a String to be printed
     *
     * @param object the object to be printed
     * @return the String print output for the object
     */
    @CheckReturnValue
    public String toString(Object object) {
        if (object == null) return "";

        Builder builder = build(object);
        return complete(builder);
    }

    /**
     * Convert any object into its print builder
     *
     * @param object the object to convert into its print builder
     * @return the object as a builder
     */
    @CheckReturnValue
    protected Builder build(Object object) {
        if (object instanceof Concept) return concept((Concept) object);
        else if (object instanceof Boolean) return bool((boolean) object);
        else if (object instanceof Optional) return optional((Optional<?>) object);
        else if (object instanceof Collection) return collection((Collection<?>) object);
        else if (object instanceof Answer) return queryAnswer((Answer) object);
        else if (object instanceof ComputeQuery.Answer) return computeAnswer((ComputeQuery.Answer) object);
        else if (object instanceof Map) return map((Map<?, ?>) object);
        else return object(object);
    }

    /**
     * Convert a builder into the final type
     *
     * @param builder the builder to convert into the final type
     * @return the converted builder
     */
    @CheckReturnValue
    protected abstract String complete(Builder builder);

    /**
     * Convert any concept into its print builder
     *
     * @param concept the concept to convert into its print builder
     * @return the concept as a builder
     */
    @CheckReturnValue
    protected abstract Builder concept(Concept concept);

    /**
     * Convert any boolean into its print builder
     *
     * @param bool the boolean to convert into its print builder
     * @return the boolean as a builder
     */
    @CheckReturnValue
    protected abstract Builder bool(boolean bool);

    /**
     * Convert any optional into its print builder
     *
     * @param optional the optional to convert into its print builder
     * @return the optional as a builder
     */
    @CheckReturnValue
    protected abstract Builder optional(Optional<?> optional);

    /**
     * Convert any collection into its print builder
     *
     * @param collection the collection to convert into its print builder
     * @return the collection as a builder
     */
    @CheckReturnValue
    protected abstract Builder collection(Collection<?> collection);

    /**
     * Convert any map into its print builder
     *
     * @param map the map to convert into its print builder
     * @return the map as a builder
     */
    @CheckReturnValue
    protected abstract Builder map(Map<?, ?> map);

    /**
     * Convert any {@link Answer} into its print builder
     *
     * @param answer the answer to convert into its print builder
     * @return the map as a builder
     */
    @CheckReturnValue
    protected Builder queryAnswer(Answer answer) {
        return map(answer.map());
    }

    /**
     * Convert any {@link ComputeQuery.Answer} into its print builder
     *
     * @param computeAnswer is the answer result of a Graql Compute queries
     * @return the computeAnswer as an output builder
     */
    @CheckReturnValue
    protected abstract Builder computeAnswer(ComputeQuery.Answer computeAnswer);

    /**
     * Default conversion behaviour if none of the more specific methods can be used
     *
     * @param object the object to convert into its print builder
     * @return the object as a builder
     */
    @CheckReturnValue
    protected abstract Builder object(Object object);
}
