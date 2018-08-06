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

package ai.grakn;

import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.answer.ConceptMap;
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
    static final String PLACE_ID = "place-id";
    static final String TAG_ID = "tag-id";
    static final String ORGANISATION_ID = "organisation-id";
    static final String CLASS_YEAR = "class-year";
    static final String WORK_FROM = "work-from";
    static final String PERSON_ID = "person-id";
    static final String MESSAGE_ID = "message-id";
    static final String FORUM_ID = "forum-id";
    static final String FIRST_NAME = "first-name";
    static final String LAST_NAME = "last-name";
    static final String BIRTHDAY = "birth-day";
    static final String CREATION_DATE = "creation-date";
    static final String LOCATION_IP = "location-ip";
    static final String BROWSER_USED = "browser-used";
    static final String GENDER = "gender";
    static final String SPEAKS = "speaks";
    static final String EMAIL = "email";
    static final String TITLE = "title";
    static final String JOIN_DATE = "join-date";
    static final String LENGTH = "length";
    static final String LANGUAGE = "language";
    static final String IMAGE_FILE = "image-file";
    static final String CONTENT = "content";
    static final String NAME = "name";

    static final VarPattern FORUM = label("forum");
    static final VarPattern TAG = label("tag");
    static final VarPattern UNIVERSITY = label("university");
    static final VarPattern COMPANY = label("company");
    static final VarPattern PERSON = label("person");
    static final VarPattern INTERESTED = label("interested");
    static final VarPattern INTEREST = label("interest");
    static final VarPattern HAS_INTEREST = label("has-interest");
    static final VarPattern STUDENT = label("student");
    static final VarPattern SCHOOL = label("school");
    static final VarPattern STUDY_AT = label("study-at");
    static final VarPattern EMPLOYEE = label("employee");
    static final VarPattern EMPLOYER = label("employer");
    static final VarPattern WORK_AT = label("work-at");
    static final VarPattern LOCATED = label("located");
    static final VarPattern REGION = label("region");
    static final VarPattern IS_LOCATED_IN = label("is-located-in");
    static final VarPattern ADMIRER = label("admirer");
    static final VarPattern LIKE = label("like");
    static final VarPattern LIKES = label("likes");
    static final VarPattern TAGGED = label("tagged");
    static final VarPattern TOPIC = label("topic");
    static final VarPattern HAS_TAG = label("has-tag");
    static final VarPattern MODERATOR = label("moderator");
    static final VarPattern MODERATED = label("moderated");
    static final VarPattern HAS_MODERATOR = label("has-moderator");
    static final VarPattern MEMBER = label("member");
    static final VarPattern GROUP = label("group");
    static final VarPattern HAS_MEMBER = label("has-member");
    static final VarPattern POST = label("post");
    static final VarPattern PRODUCT = label("product");
    static final VarPattern CREATOR = label("creator");
    static final VarPattern HAS_CREATOR = label("has-creator");
    static final VarPattern CONTAINED = label("contained");
    static final VarPattern CONTAINER = label("container");
    static final VarPattern CONTAINER_OF = label("container-of");
    static final VarPattern COMMENT = label("comment");
    static final VarPattern REPLY = label("reply");
    static final VarPattern ORIGINAL = label("original");
    static final VarPattern REPLY_OF = label("reply-of");
    static final VarPattern FRIEND = label("friend");
    static final VarPattern KNOWS = label("knows");
    static final VarPattern CHILD_MESSAGE = label("child-message");
    static final VarPattern PARENT_MESSAGE = label("parent-message");
    static final VarPattern ORIGINAL_POST = label("original-post");
    static final VarPattern MEMBER_MESSAGE = label("member-message");
    static final VarPattern GROUP_FORUM = label("group-forum");
    static final VarPattern FORUM_MEMBER = label("forum-member");
    static final VarPattern MESSAGE = label("message");

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
    static final Var $firstName = var(FIRST_NAME);
    static final Var $lastName = var(LAST_NAME);
    static final Var $birthday = var("birthday");
    static final Var $locationIp = var(LOCATION_IP);
    static final Var $browserUsed = var(BROWSER_USED);
    static final Var $gender = var(GENDER);
    static final Var $creationDate = var(CREATION_DATE);
    static final Var $place = var("place");
    static final Var $placeId = var("placeID");
    static final Var $date = var("date");
    static final Var $messageId = var("messageId");
    static final Var $content = var(CONTENT);
    static final Var $originalPost = var("originalPost");
    static final Var $opId = var("opId");
    static final Var $authorId = var("authorId");
    static final Var $friend = var("friend");
    static final Var $friendId = var("friendId");
    static final Var $personId = var("personId");
    static final Var $forumId = var("forumId");
    static final Var $title = var(TITLE);

    private SNB(){}

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

    static <T> Function<ConceptMap, T> by(Var var) {
        return map -> resource(map, var);
    }

    static <T> T resource(ConceptMap result, Var var) {
        return result.get(var).<T>asAttribute().value();
    }
}
