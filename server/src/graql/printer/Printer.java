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

package grakn.core.graql.printer;

import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptList;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.ConceptSetMeasure;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;

import javax.annotation.CheckReturnValue;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 * An interface for print objects in Graql responses (e.g. {@link Integer}s and {@link ConceptMap}s into a String).
 * The intermediate {@link Builder} type is used when the final type is different to the "in-progress" type when
 * creating it. For example, you may want to use a {@link StringBuilder} for {@link Builder} (for efficiency).
 *
 * If you don't need a {@link Builder} type, then set it to the same type as String and implement
 * {@link #complete(Builder)} to just return its argument.
 *
 * @param <Builder> An intermediate builder type that can be changed into a String
 */
public abstract class Printer<Builder> {

    /**
     * Constructs a special type of Printer: StringPrinter
     *
     * @param colorize       boolean parameter to determine if the String output should be colorized
     * @param attributeTypes list of attribute types that should be included in the String output
     * @return a new StringPrinter object
     */
    public static Printer<StringBuilder> stringPrinter(boolean colorize, AttributeType... attributeTypes) {
        return new StringPrinter(colorize, attributeTypes);
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
        if (object instanceof Concept) {
            return concept((Concept) object);
        }
        else if (object instanceof Boolean) {
            return bool((boolean) object);
        }
        else if (object instanceof Collection) {
            return collection((Collection<?>) object);
        }
        else if (object instanceof AnswerGroup<?>) {
            return answerGroup((AnswerGroup<?>) object);
        }
        else if (object instanceof ConceptList) {
            return conceptList((ConceptList) object);
        }
        else if (object instanceof ConceptMap) {
            return conceptMap((ConceptMap) object);
        }
        else if (object instanceof ConceptSet) {
            if (object instanceof ConceptSetMeasure) {
                return conceptSetMeasure((ConceptSetMeasure) object);
            }
            else {
                return conceptSet((ConceptSet) object);
            }
        }
        else if (object instanceof Value) {
            return value((Value) object);
        }
        else if (object instanceof Map) {
            return map((Map<?, ?>) object);
        }
        else {
            return object(object);
        }
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
     * Convert any {@link AnswerGroup} into its print builder
     *
     * @param answer is the answer result of a Graql Compute queries
     * @return the grouped answers as an output builder
     */
    @CheckReturnValue
    protected abstract Builder answerGroup(AnswerGroup<?> answer);

    /**
     * Convert any {@link ConceptList} into its print builder
     *
     * @param answer is the answer result of a Graql Compute queries
     * @return the concept list as an output builder
     */
    @CheckReturnValue
    protected Builder conceptList(ConceptList answer) {
        return collection(answer.list());
    }

    /**
     * Convert any {@link ConceptMap} into its print builder
     *
     * @param answer is the answer result of a Graql Compute queries
     * @return the concept map as an output builder
     */
    @CheckReturnValue
    protected abstract Builder conceptMap(ConceptMap answer);

    /**
     * Convert any {@link ConceptSet} into its print builder
     *
     * @param answer is the answer result of a Graql Compute queries
     * @return the concept set as an output builder
     */
    @CheckReturnValue
    protected Builder conceptSet(ConceptSet answer) {
        return collection(answer.set());
    }

    /**
     * Convert any {@link ConceptSetMeasure} into its print builder
     *
     * @param answer is the answer result of a Graql Compute queries
     * @return the concept set measure as an output builder
     */
    @CheckReturnValue
    protected abstract Builder conceptSetMeasure(ConceptSetMeasure answer);

    /**
     * Convert any {@link Value} into its print builder
     *
     * @param answer is the answer result of a Graql Compute queries
     * @return the number as an output builder
     */
    @CheckReturnValue
    protected Builder value(Value answer) {
        return object(answer.number());
    }

    /**
     * Default conversion behaviour if none of the more specific methods can be used
     *
     * @param object the object to convert into its print builder
     * @return the object as a builder
     */
    @CheckReturnValue
    protected abstract Builder object(Object object);
}
