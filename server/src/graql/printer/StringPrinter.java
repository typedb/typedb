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

import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;
import graql.lang.Graql.Token.Char;
import graql.lang.Graql.Token.Property;
import graql.lang.util.StringUtil;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static graql.lang.util.StringUtil.escapeLabelOrId;

/**
 * Default printer that prints results in Graql syntax
 *
 */
public class StringPrinter extends Printer<StringBuilder> {

    private final AttributeType[] attributeTypes;
    private final boolean colorize;

    StringPrinter(boolean colorize, AttributeType... attributeTypes) {
        this.colorize = colorize;
        this.attributeTypes = attributeTypes;
    }

    /**
     * @param id an ID of a concept
     * @return
     * The id of the concept correctly escaped in graql.
     * If the ID doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String conceptId(ConceptId id) {
        return escapeLabelOrId(id.getValue());
    }

    /**
     * @param label a label of a type
     * @return
     * The label of the type correctly escaped in graql.
     * If the label doesn't begin with a number and is only comprised of alphanumeric characters, underscores and dashes,
     * then it will be returned as-is, otherwise it will be quoted and escaped.
     */
    public static String label(Label label) {
        return escapeLabelOrId(label.getValue());
    }

    @Override
    protected String complete(StringBuilder output) {
        return output.toString();
    }

    @Override
    protected StringBuilder concept(Concept concept) {
        StringBuilder output = new StringBuilder();

        // Display values for resources and ids for everything else
        if (concept.isAttribute()) {
            output.append(StringUtil.valueToString(concept.asAttribute().value()));
        } else if (concept.isSchemaConcept()) {
            SchemaConcept ontoConcept = concept.asSchemaConcept();
            output.append(colorKeyword(Property.TYPE.toString()))
                    .append(Char.SPACE)
                    .append(colorType(ontoConcept));

            SchemaConcept superConcept = ontoConcept.sup();

            if (superConcept != null) {
                output.append(Char.SPACE)
                        .append(colorKeyword(Property.SUB.toString()))
                        .append(Char.SPACE)
                        .append(colorType(superConcept));
            }
        } else {
            output.append(colorKeyword(Property.ID.toString()))
                    .append(Char.SPACE)
                    .append(conceptId(concept.id()));
        }

        if (concept.isRelation()) {
            List<String> rolePlayerList = new LinkedList<>();
            for (Map.Entry<Role, Set<Thing>> rolePlayers : concept.asRelation().rolePlayersMap().entrySet()) {
                Role role = rolePlayers.getKey();
                Set<Thing> things = rolePlayers.getValue();

                for (Thing thing : things) {
                    rolePlayerList.add(
                            colorType(role) + Char.COLON + Char.SPACE +
                                    Property.ID + Char.SPACE + conceptId(thing.id()));
                }
            }

            String relationString = rolePlayerList.stream().collect(Collectors.joining(Char.COMMA_SPACE.toString()));
            output.append(Char.SPACE).append(Char.PARAN_OPEN).append(relationString).append(Char.PARAN_CLOSE);
        }

        // Display type of each instance
        if (concept.isThing()) {
            Type type = concept.asThing().type();
            output.append(Char.SPACE)
                    .append(colorKeyword(Property.ISA.toString()))
                    .append(Char.SPACE)
                    .append(colorType(type));
        }

        // Display when and then for rules
        if (concept.isRule()) {
            output.append(Char.SPACE).append(colorKeyword(Property.WHEN.toString())).append(Char.SPACE)
                    .append(Char.CURLY_OPEN).append(Char.SPACE)
                    .append(concept.asRule().when())
                    .append(Char.SPACE).append(Char.CURLY_CLOSE);
            output.append(Char.SPACE).append(colorKeyword(Property.THEN.toString())).append(Char.SPACE)
                    .append(Char.CURLY_OPEN).append(Char.SPACE)
                    .append(concept.asRule().then())
                    .append(Char.SPACE).append(Char.CURLY_CLOSE);
        }

        // Display any requested resources
        if (concept.isThing() && attributeTypes.length > 0) {
            concept.asThing().attributes(attributeTypes).forEach(resource -> {
                String attributeType = colorType(resource.type());
                String value = StringUtil.valueToString(resource.value());
                output.append(Char.SPACE).append(colorKeyword(Property.HAS.toString())).append(Char.SPACE)
                        .append(attributeType).append(Char.SPACE).append(value);
            });
        }

        return output;
    }

    @Override
    protected StringBuilder bool(boolean bool) {
        StringBuilder builder = new StringBuilder();

        if (bool) {
            return builder.append(ANSI.color("true", ANSI.GREEN));
        } else {
            return builder.append(ANSI.color("false", ANSI.RED));
        }
    }

    @Override
    protected StringBuilder collection(Collection<?> collection) {
        StringBuilder builder = new StringBuilder();

        builder.append(Char.CURLY_OPEN);
        collection.stream().findFirst().ifPresent(item -> builder.append(build(item)));
        collection.stream().skip(1).forEach(item -> builder.append(Char.COMMA_SPACE).append(build(item)));
        builder.append(Char.CURLY_CLOSE);

        return builder;
    }

    @Override
    protected StringBuilder map(Map<?, ?> map) {
        return collection(map.entrySet());
    }

    @Override
    protected StringBuilder answerGroup(AnswerGroup<?> answer) {
        StringBuilder builder = new StringBuilder();
        return builder.append(Char.CURLY_OPEN)
                .append(concept(answer.owner()))
                .append(Char.COLON).append(Char.SPACE)
                .append(build(answer.answers()))
                .append(Char.CURLY_CLOSE);
    }

    @Override
    protected StringBuilder conceptMap(ConceptMap answer) {
        StringBuilder builder = new StringBuilder();

        answer.forEach((name, concept) -> builder.append(name).append(Char.SPACE)
                .append(concept(concept)).append(Char.SEMICOLON).append(Char.SPACE));
        return new StringBuilder(Char.CURLY_OPEN + builder.toString().trim() + Char.CURLY_CLOSE);
    }

    @Override
    protected StringBuilder conceptSetMeasure(ConceptSetMeasure answer) {
        StringBuilder builder = new StringBuilder();
        return builder.append(answer.measurement()).append(Char.COLON).append(Char.SPACE).append(collection(answer.set()));
    }

    @Override
    protected StringBuilder object(Object object) {
        StringBuilder builder = new StringBuilder();

        if (object instanceof Map.Entry<?, ?>) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;

            builder.append(build(entry.getKey()));
            builder.append(Char.COLON).append(Char.SPACE);
            builder.append(build(entry.getValue()));
        } else if (object != null) {
            builder.append(object);
        }

        return builder;
    }


    /**
     * Color-codes the keyword if colorization enabled
     * @param keyword a keyword to color-code using ANSI colors
     * @return the keyword, color-coded
     */
    private String colorKeyword(String keyword) {
        if(colorize) {
            return ANSI.color(keyword, ANSI.BLUE);
        } else {
            return keyword;
        }
    }

    /**
     * Color-codes the given type if colorization enabled
     * @param schemaConcept a type to color-code using ANSI colors
     * @return the type, color-coded
     */
    private String colorType(SchemaConcept schemaConcept) {
        if(colorize) {
            return ANSI.color(label(schemaConcept.label()), ANSI.PURPLE);
        } else {
            return label(schemaConcept.label());
        }
    }

    /**
     * Includes ANSI unicode commands for different colours
     *
     */
    @SuppressWarnings("unused")
    public static class ANSI {

        private static final String RESET = "\u001B[0m";
        public static final String BLACK = "\u001B[30m";
        public static final String RED = "\u001B[31m";
        public static final String GREEN = "\u001B[32m";
        public static final String YELLOW = "\u001B[33m";
        public static final String BLUE = "\u001B[34m";
        public static final String PURPLE = "\u001B[35m";
        public static final String CYAN = "\u001B[36m";
        public static final String WHITE = "\u001B[37m";

        /**
         * @param string the string to set the color on
         * @param color the color to set on the string
         * @return a new string with the color set
         */
        public static String color(String string, String color) {
            return color + string + RESET;
        }
    }
}
