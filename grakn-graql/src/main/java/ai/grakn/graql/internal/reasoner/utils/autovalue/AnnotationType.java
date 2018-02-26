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

package ai.grakn.graql.internal.reasoner.utils.autovalue;

import java.util.Set;


/**
 *
 * <p>
 * Auxiliary enum for {@link AutoValueIgnoreHashEqualsExtension}.
 * </p>
 *
 * @author Robert Eggar
 *
 */
enum AnnotationType {
    ERROR(""), // Error case
    INCLUDE("IncludeHashEquals"),
    IGNORE("IgnoreHashEquals"),
    NOT_PRESENT(""); // Empty case

    private final String annotationName;

    AnnotationType(String annotationName) {
        this.annotationName = annotationName;
    }

    public boolean shouldBeIncluded(Set<String> propertyAnnotations) {
        boolean annotationPresent = propertyAnnotations.contains(annotationName);
        switch (this) {
            case INCLUDE:
                return annotationPresent;
            case IGNORE:
                return !annotationPresent;
            default:
                return true;
        }
    }

    public static AnnotationType from(Set<String> annotations) {
        boolean ignoreHashEqualsPresent = annotations.contains(IGNORE.annotationName);
        boolean includeHashEqualsPresent = annotations.contains(INCLUDE.annotationName);

        if (ignoreHashEqualsPresent && includeHashEqualsPresent) {
            return ERROR;
        } else if (ignoreHashEqualsPresent) {
            return IGNORE;
        } else if (includeHashEqualsPresent) {
            return INCLUDE;
        } else {
            return NOT_PRESENT;
        }
    }
}
