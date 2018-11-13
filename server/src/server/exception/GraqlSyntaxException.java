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

package grakn.core.server.exception;

import java.util.Map;

import static grakn.core.util.ErrorMessage.INVALID_STATEMENT;
import static grakn.core.util.ErrorMessage.TEMPLATE_MISSING_KEY;

/**
 * <p>
 *     Syntax Exception
 * </p>
 *
 * <p>
 *     This is thrown when a parsing error occurs.
 *     It means the user has input an invalid graql query which cannot be parsed.
 * </p>
 *
 */
public class GraqlSyntaxException extends GraknException {

    private final String NAME = "GraqlSyntaxException";

    private GraqlSyntaxException(String error) {
        super(error);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Thrown when a parsing error occurs during parsing a graql file
     */
    public static GraqlSyntaxException create(String error){
        return new GraqlSyntaxException(error);
    }

    /**
     * Thrown when there is a syntactic error in a Graql template
     */
    public static GraqlSyntaxException parsingIncorrectValueType(Object object, Class clazz, Map data){
        return new GraqlSyntaxException(INVALID_STATEMENT.getMessage(object, clazz.getName(), data.toString()));
    }

    /**
     * Thrown when a key is missing during parsing a template with matching data
     */
    public static GraqlSyntaxException parsingTemplateMissingKey(String invalidText, Map<String, Object> data){
        return new GraqlSyntaxException(TEMPLATE_MISSING_KEY.getMessage(invalidText, data));
    }
}
