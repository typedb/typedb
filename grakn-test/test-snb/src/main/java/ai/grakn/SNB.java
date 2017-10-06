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
 *
 */

package ai.grakn;

import ai.grakn.graql.VarPattern;

import static ai.grakn.graql.Graql.label;

/**
 * @author Felix Chapman
 */
public class SNB {
    static String placeId = "place-id";
    static String tagId = "tag-id";
    static String organisationId = "organisationId";
    static String classYear = "class-year";
    static String workFrom = "work-from";
    static String personId = "person-id";
    static String firstName = "first-name";
    static String lastName = "last-name";
    static String birthday = "birth-day";
    static String creationDate = "creation-date";
    static String locationIp = "location-ip";
    static String browserUsed = "browser-used";
    static String gender = "gender";
    static String speaks = "speaks";
    static String email = "email";

    static VarPattern tag = label("tag");
    static VarPattern university = label("university");
    static VarPattern company = label("company");
    static VarPattern person = label("person");
    static VarPattern interested = label("interested");
    static VarPattern interest = label("interest");
    static VarPattern hasInterest = label("has-interest");
    static VarPattern student = label("student");
    static VarPattern school = label("school");
    static VarPattern studyAt = label("study-at");
    static VarPattern employee = label("employee");
    static VarPattern employer = label("employer");
    static VarPattern workAt = label("work-at");
    static VarPattern located = label("located");
    static VarPattern region = label("region");
    static VarPattern locatedIn = label("located-in");
}
