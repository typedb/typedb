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
import ai.grakn.util.Schema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

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
    static String messageId = "message-id";
    static String forumId = "forum-id";
    static String firstName = "first-name";
    static String lastName = "last-name";
    static String birthday = "birth-day";
    static String creationDate = "creation-date";
    static String locationIp = "location-ip";
    static String browserUsed = "browser-used";
    static String gender = "gender";
    static String speaks = "speaks";
    static String email = "email";
    static String title = "title";
    static String joinDate = "join-date";
    static String length = "length";
    static String language = "language";
    static String imageFile = "image-file";
    static String content = "content";

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
    static VarPattern isLocatedIn = label("is-located-in");
    static VarPattern admirer = label("admirer");
    static VarPattern like = label("like");
    static VarPattern likes = label("likes");
    static VarPattern tagged = label("tagged");
    static VarPattern topic = label("topic");
    static VarPattern hasTag = label("has-tag");
    static VarPattern moderator = label("moderator");
    static VarPattern moderated = label("moderated");
    static VarPattern hasModerator = label("has-moderator");
    static VarPattern member = label("member");
    static VarPattern group = label("group");
    static VarPattern hasMember = label("has-member");
    static VarPattern post = label("post");
    static VarPattern product = label("product");
    static VarPattern creator = label("creator");
    static VarPattern hasCreator = label("has-creator");
    static VarPattern contained = label("contained");
    static VarPattern container = label("container");
    static VarPattern containerOf = label("container-of");
    static VarPattern comment = label("comment");
    static VarPattern reply = label("reply");
    static VarPattern original = label("original");
    static VarPattern replyOf = label("reply-of");
    static VarPattern friend = label("friend");
    static VarPattern knows = label("knows");

    static VarPattern has(String label) {
        return label(Schema.ImplicitType.HAS.getLabel(label));
    }

    static VarPattern key(String label) {
        return label(Schema.ImplicitType.KEY.getLabel(label));
    }

    static LocalDateTime fromDate(Date date) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneOffset.UTC);
    }

    static long toEpoch(LocalDateTime localDateTime) {
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
