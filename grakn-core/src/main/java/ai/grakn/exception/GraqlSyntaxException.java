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

package ai.grakn.exception;

/*-
 * #%L
 * grakn-core
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.Map;

import static ai.grakn.util.ErrorMessage.INVALID_STATMENT;
import static ai.grakn.util.ErrorMessage.TEMPLATE_MISSING_KEY;

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
 * @author fppt
 */
public class GraqlSyntaxException extends GraknException {
    private GraqlSyntaxException(String error) {
        super(error);
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
        return new GraqlSyntaxException(INVALID_STATMENT.getMessage(object, clazz.getName(), data.toString()));
    }

    /**
     * Thrown when a key is missing during parsing a template with matching data
     */
    public static GraqlSyntaxException parsingTemplateMissingKey(String invalidText, Map<String, Object> data){
        return new GraqlSyntaxException(TEMPLATE_MISSING_KEY.getMessage(invalidText, data));
    }
}
