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

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.util.Schema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.function.Function;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;

/**
 * @author Felix Chapman
 */
public class SNB {
    static String PLACE_ID = "place-id";
    static String TAG_ID = "tag-id";
    static String ORGANISATION_ID = "organisationId";
    static String CLASS_YEAR = "class-year";
    static String WORK_FROM = "work-from";
    static String PERSON_ID = "person-id";
    static String MESSAGE_ID = "message-id";
    static String FORUM_ID = "forum-id";
    static String FIRST_NAME = "first-name";
    static String LAST_NAME = "last-name";
    static String BIRTHDAY = "birth-day";
    static String CREATION_DATE = "creation-date";
    static String LOCATION_IP = "location-ip";
    static String BROWSER_USED = "browser-used";
    static String GENDER = "gender";
    static String SPEAKS = "speaks";
    static String EMAIL = "email";
    static String TITLE = "title";
    static String JOIN_DATE = "join-date";
    static String LENGTH = "length";
    static String LANGUAGE = "language";
    static String IMAGE_FILE = "image-file";
    static String CONTENT = "content";
    static String NAME = "name";

    static VarPattern TAG = label("tag");
    static VarPattern UNIVERSITY = label("university");
    static VarPattern COMPANY = label("company");
    static VarPattern PERSON = label("person");
    static VarPattern INTERESTED = label("interested");
    static VarPattern INTEREST = label("interest");
    static VarPattern HAS_INTEREST = label("has-interest");
    static VarPattern STUDENT = label("student");
    static VarPattern SCHOOL = label("school");
    static VarPattern STUDY_AT = label("study-at");
    static VarPattern EMPLOYEE = label("employee");
    static VarPattern EMPLOYER = label("employer");
    static VarPattern WORK_AT = label("work-at");
    static VarPattern LOCATED = label("located");
    static VarPattern REGION = label("region");
    static VarPattern LOCATED_IN = label("located-in");
    static VarPattern IS_LOCATED_IN = label("is-located-in");
    static VarPattern ADMIRER = label("admirer");
    static VarPattern LIKE = label("like");
    static VarPattern LIKES = label("likes");
    static VarPattern TAGGED = label("tagged");
    static VarPattern TOPIC = label("topic");
    static VarPattern HAS_TAG = label("has-tag");
    static VarPattern MODERATOR = label("moderator");
    static VarPattern MODERATED = label("moderated");
    static VarPattern HAS_MODERATOR = label("has-moderator");
    static VarPattern MEMBER = label("member");
    static VarPattern HAS_MEMBER = label("has-member");
    static VarPattern POST = label("post");
    static VarPattern PRODUCT = label("product");
    static VarPattern CREATOR = label("creator");
    static VarPattern HAS_CREATOR = label("has-creator");
    static VarPattern CONTAINED = label("contained");
    static VarPattern CONTAINER = label("container");
    static VarPattern CONTAINER_OF = label("container-of");
    static VarPattern COMMENT = label("comment");
    static VarPattern REPLY = label("reply");
    static VarPattern ORIGINAL = label("original");
    static VarPattern REPLY_OF = label("reply-of");
    static VarPattern FRIEND = label("friend");
    static VarPattern KNOWS = label("knows");
    static VarPattern CHILD_MESSAGE = label("child-message");
    static VarPattern PARENT_MESSAGE = label("parent-message");
    static VarPattern ORIGINAL_POST = label("original-post");
    static VarPattern MEMBER_MESSAGE = label("member-message");
    static VarPattern GROUP_FORUM = label("group-forum");
    static VarPattern FORUM_MEMBER = label("forum-member");
    static VarPattern MESSAGE = label("message");

    static final Var $person = var("person");
    static final Var $city = var("city");
    static final Var $message = var("message");
    static final Var $mod = var("mod");
    static final Var $modId = var("modId");
    static final Var $forum = var("forum");
    static final Var $author = var("author");
    static final Var $author1 = var("author1");
    static final Var $author2 = var("author2");
    static final Var $country = var("country");
    static final Var $post = var("post");
    static final Var $original = var("original");
    static final Var $comment = var("comment");
    static final Var $commentId = var("commentId");
    static final Var $firstName = var("first-name");
    static final Var $lastName = var("last-name");
    static final Var $birthday = var("birthday");
    static final Var $locationIp = var("location-ip");
    static final Var $browserUsed = var("browser-used");
    static final Var $gender = var("gender");
    static final Var $creationDate = var("creation-date");
    static final Var $place = var("place");
    static final Var $placeId = var("placeID");
    static final Var $date = var("date");
    static final Var $messageId = var("messageId");
    static final Var $content = var("content");
    static final Var $originalPost = var("originalPost");
    static final Var $opId = var("opId");
    static final Var $authorId = var("authorId");
    static final Var $friend = var("friend");
    static final Var $friendId = var("friendId");
    static final Var $personId = var("personId");
    static final Var $forumId = var("forumId");
    static final Var $title = var("title");

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

    static <T> Function<Answer, T> by(Var var) {
        return map -> resource(map, var);
    }

    static <T> T resource(Answer result, Var var) {
        return result.get(var).<T>asAttribute().getValue();
    }
}
