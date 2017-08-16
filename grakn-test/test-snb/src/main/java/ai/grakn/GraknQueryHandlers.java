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
 */
package ai.grakn;

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Attribute;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Order;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.analytics.PathQuery;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.compute;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;

/**
 * Implementations of the LDBC SNB complex queries.
 *
 * @author sheldon
 */
public class GraknQueryHandlers {

    static VarPattern knowsType = var().label("knows");
    static VarPattern hasCreatorType = var().label("has-creator");
    static VarPattern replyOf = var().label("reply-of");
    static VarPattern reply = var().label("reply");
    static VarPattern isLocatedIn = var().label("is-located-in");
    static VarPattern workAt = var().label("work-at");
    static VarPattern studyAt = var().label("study-at");

    static Label personID = Label.of("person-id");
    static Label creationDate = Label.of("creation-date");
    static Label messageID = Label.of("message-id");
    static Label personFirstName = Label.of("first-name");
    static Label personLastName = Label.of("last-name");
    static Label messageContent = Label.of("content");
    static Label messageImageFile = Label.of("image-file");
    static Label personBirthday = Label.of("birth-day");
    static Label gender = Label.of("gender");
    static Label browserUsed = Label.of("browser-used");
    static Label locationIp = Label.of("location-ip");
    static Label email = Label.of("email");
    static Label speaks = Label.of("speaks");
    static Label name = Label.of("name");
    static Label classYear = Label.of("class-year");
    static Label workFrom = Label.of("work-from");

    static Var thePerson = var("person");
    static Var aMessage = var("aMessage");
    static Var aMessageDate = var("aMessageDate");
    static Var aMessageId = var("aMessageID");
    static Var someContent = var("content");
    static Var aFriend = var("aFriend");
    static Var aFriendId = var("aFriendID");
    static Var aFriendLastName = var("aFriendLastName");

    /**
     * Retrieves the value of a resource and infers the type.
     *
     * @param result a single result of a graql query
     * @param resource a var representing the resource
     * @param <T> the data type of the resource value
     * @return the resource value
     */
    private static <T> T resource(Answer result, Var resource) {
        return result.get(resource).<T>asResource().getValue();
    }

    /**
     * Retrieves the LocalDateTime value of a resource and returns the epoch milli using UTC time.
     *
     * @param result a single result of a graql query
     * @param resource a var representing the resource
     * @return the time as an epoch milli
     */
    private static long timeResource(Answer result, Var resource) {
        return result.get(resource).<LocalDateTime>asResource().getValue().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * Complex Query 2
     */
    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 ldbcQuery2, GraknDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graknGraph = session.open(GraknTxType.READ)) {
                Var aFriendFirstName = var("aFriendFirstName");
                LocalDateTime maxDate = LocalDateTime.ofInstant(ldbcQuery2.maxDate().toInstant(), ZoneOffset.UTC);

                // to make this query execute faster split it into two parts:
                //     the first does the ordering
                //     the second fetches the resources
                MatchQuery graknLdbcQuery2 = match(
                        var().rel(thePerson.has(personID, var().val(ldbcQuery2.personId()))).rel(aFriend).isa(knowsType),
                        var().rel(aFriend).rel(aMessage).isa(hasCreatorType),
                        aMessage.has(creationDate, aMessageDate).has(messageID, aMessageId),
                        aMessageDate.val(lte(maxDate)));

                List<Answer> rawResult = graknLdbcQuery2.orderBy(aMessageDate, Order.desc)
                        .limit(ldbcQuery2.limit()).withGraph(graknGraph).execute();

                // sort first by date and then by message id
                Comparator<Answer> ugly = Comparator.<Answer>comparingLong(
                        map -> timeResource(map, aMessageDate)).reversed().
                        thenComparingLong(map -> resource(map, aMessageId));

                // process the query results
                List<LdbcQuery2Result> result = rawResult.stream().sorted(ugly).map(map -> {
                    // fetch the resources attached to entities in the queries
                    MatchQuery queryExtendedInfo = match(
                            aFriend.has(personFirstName, aFriendFirstName).has(personLastName, aFriendLastName).has(personID, aFriendId),
                            var().rel(aFriend).rel(aMessage).isa(hasCreatorType),
                            aMessage.has(creationDate, aMessageDate)
                                    .has(messageID, var().val(GraknQueryHandlers.<Long>resource(map, aMessageId))),
                            or(aMessage.has(messageContent, someContent), aMessage.has(messageImageFile, someContent)));
                    Answer extendedInfo = queryExtendedInfo.withGraph(graknGraph).execute().iterator().next();

                    // prepare the answer from the original query and the query for extended information
                    return new LdbcQuery2Result(
                            resource(extendedInfo, aFriendId),
                            resource(extendedInfo, aFriendFirstName),
                            resource(extendedInfo, aFriendLastName),
                            resource(map, aMessageId),
                            resource(extendedInfo, someContent),
                            timeResource(map, aMessageDate));
                }).collect(Collectors.toList());

                resultReporter.report(0,result,ldbcQuery2);
            }
        }

    }

    /**
     * Complex Query 8
     */
    public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, GraknDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery8 ldbcQuery8, GraknDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graknGraph = session.open(GraknTxType.READ)) {
                // for speed the query is again split into the ordering and limit phase
                Var aReply = var("aReply");
                Var responder = var("responder");
                Var responderId = var("responderId");
                Var responderFirst = var("responderFirst");
                Var responderLast = var("responderLast");
                MatchQuery orderQuery = match(
                        thePerson.has(personID, var().val(ldbcQuery8.personId())),
                        var().rel(thePerson).rel(aMessage).isa(hasCreatorType),
                        var().rel(aMessage).rel(reply, aReply).isa(replyOf),
                        aReply.has(creationDate, aMessageDate).has(messageID, aMessageId)
                );
                List<Answer> rawResult = orderQuery.withGraph(graknGraph)
                        .orderBy(aMessageDate, Order.desc).limit(ldbcQuery8.limit()).execute();

                // sort first by date and then by message id
                Comparator<Answer> ugly = Comparator.<Answer>comparingLong(
                        map -> timeResource(map, aMessageDate)).reversed().
                        thenComparingLong(map -> resource(map, aMessageId));

                // process the query results
                List<LdbcQuery8Result> result = rawResult.stream().sorted(ugly).map(map -> {
                    // fetch the resources attached to entities in the queries
                    MatchQuery queryExtendedInfo = match(
                            aReply.has(messageID, var().val(GraknQueryHandlers.<Long>resource(map, aMessageId))),
                            or(aReply.has(messageContent, someContent), aReply.has(messageImageFile, someContent)),
                            var().rel(aReply).rel(responder).isa(hasCreatorType),
                            responder.has(personID, responderId).has(personFirstName, responderFirst).has(personLastName, responderLast)
                            );
                    Answer extendedInfo = queryExtendedInfo.withGraph(graknGraph).execute().iterator().next();

                    // prepare the answer from the original query and the query for extended information
                    return new LdbcQuery8Result(
                            resource(extendedInfo, responderId),
                            resource(extendedInfo, responderFirst),
                            resource(extendedInfo, responderLast),
                            timeResource(map, aMessageDate),
                            resource(map, aMessageId),
                            resource(extendedInfo, someContent));
                }).collect(Collectors.toList());

                resultReporter.report(0,result,ldbcQuery8);
            }
        }
    }

    /**
     * Complex Query 1
     */
    public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, GraknDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery1 ldbcQuery1, GraknDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graknGraph = session.open(GraknTxType.READ)) {
                Var anyone = var("anyone");
                Var anyoneElse = var("anyoneElse");

                // for speed fetch the Grakn id first
                ConceptId graknPersonId = match(thePerson.has(personID, var().val(ldbcQuery1.personId()))).withGraph(graknGraph).
                        execute().iterator().next().get(thePerson).getId();

                // sort by lastname and then id
                Comparator<Answer> ugly = Comparator.<Answer,String>comparing(
                        map -> resource(map, aFriendLastName)).
                        thenComparingLong(map -> resource(map, aFriendId));

                // This query has to be split into 3 parts, each fetching people a further distance away
                // The longer queries only need be executed if there are not enough shorter queries
                // The last ordering by id must be done after each query has been executed
                MatchQuery matchQuery = match(thePerson.id(graknPersonId),
                        var().rel(thePerson).rel(aFriend).isa(knowsType),
                        aFriend.has(personFirstName,var().val(ldbcQuery1.firstName())).
                                has(personLastName,aFriendLastName).
                                has(personID, aFriendId),
                        thePerson.neq(aFriend));
                List<Answer> distance1Result = matchQuery.withGraph(graknGraph).execute();
                List<LdbcQuery1Result> distance1LdbcResult = populateResults(distance1Result.stream().sorted(ugly), ldbcQuery1, graknGraph, 1);
                if (distance1Result.size() < ldbcQuery1.limit()) {
                    matchQuery = match(thePerson.id(graknPersonId),
                            var().rel(thePerson).rel(anyone).isa(knowsType),
                            var().rel(anyone).rel(aFriend).isa(knowsType),
                            aFriend.has(personFirstName,var().val(ldbcQuery1.firstName())).
                                    has(personLastName,aFriendLastName).
                                    has(personID, aFriendId),
                            thePerson.neq(aFriend)
                            );
                    List<Answer> distance2Result = matchQuery.withGraph(graknGraph).execute();
                    distance1LdbcResult.addAll(populateResults(distance2Result.stream().sorted(ugly),ldbcQuery1,graknGraph, 2));
                    if (distance1Result.size() + distance2Result.size() < ldbcQuery1.limit()) {
                        matchQuery = match(thePerson.id(graknPersonId),
                                var().rel(thePerson).rel(anyone).isa(knowsType),
                                var().rel(anyone).rel(anyoneElse).isa(knowsType),
                                var().rel(anyoneElse).rel(aFriend).isa(knowsType),
                                aFriend.has(personFirstName,var().val(ldbcQuery1.firstName())).
                                        has(personLastName,aFriendLastName).
                                        has(personID, aFriendId),
                                thePerson.neq(aFriend),
                                aFriend.neq(anyone)
                        );
                        List<Answer> distance3Result = matchQuery.withGraph(graknGraph).execute();
                        distance1LdbcResult.addAll(populateResults(distance3Result.stream().sorted(ugly),ldbcQuery1,graknGraph, 3));
                    }
                }
                resultReporter.report(0, distance1LdbcResult, ldbcQuery1);
            }
        }

        /**
         * Populate the LdbcQuery1Result object from graql results. As part of this extra queries are executed to fetch related information.
         *
         * @param graqlResults the graql results used to populate the ldbc results
         * @param ldbcQuery1 the ldbc query parameters
         * @param graknGraph the graph for additional queries
         * @param distance the number of knows relations between initial person and these results
         * @return the ldbc results
         */
        private static List<LdbcQuery1Result> populateResults(Stream<Answer> graqlResults, LdbcQuery1 ldbcQuery1, GraknGraph graknGraph, int distance) {
            return graqlResults.limit(ldbcQuery1.limit()).map(map -> {
                // these queries get all of the additional related material, excluding resources
                Var aLocation = var("aLocation");
                MatchQuery locationQuery = match(
                        aFriend.id(map.get(aFriend).getId()),
                        var().rel(aFriend).rel(aLocation).isa(isLocatedIn));
                Answer locationResult = locationQuery.withGraph(graknGraph).execute().iterator().next();

                Var aYear = var("aYear");
                Var aOrganisation = var("aOrganisation");
                MatchQuery universityQuery = match(
                        aFriend.id(map.get(aFriend).getId()),
                        var().rel(aFriend).rel(aOrganisation).isa(studyAt).has(classYear, aYear),
                        var().rel(aOrganisation).rel(aLocation).isa(isLocatedIn)
                );
                List<Answer> universityResults = universityQuery.withGraph(graknGraph).execute();
                List<List<Object>> universityProcessedResults = universityResults.stream().map(answer -> {
                    List<Object> result = new ArrayList<Object>();
                    result.add(getSingleResource(answer.get(aOrganisation).asEntity(), name, graknGraph));
                    result.add(resource(answer, aYear));
                    result.add(getSingleResource(answer.get(aLocation).asEntity(),name,graknGraph));
                    return result;
                }).collect(Collectors.toList());

                MatchQuery workQuery = match(
                        aFriend.id(map.get(aFriend).getId()),
                        var().rel(aFriend).rel(aOrganisation).isa(workAt).has(workFrom, aYear),
                        var().rel(aOrganisation).rel(aLocation).isa(isLocatedIn)
                );
                List<Answer> workResults = workQuery.withGraph(graknGraph).execute();
                List<List<Object>> workProcessedResults = workResults.stream().map(answer -> {
                    List<Object> result = new ArrayList<Object>();
                    result.add(getSingleResource(answer.get(aOrganisation).asEntity(), name, graknGraph));
                    result.add(resource(answer, aYear));
                    result.add(getSingleResource(answer.get(aLocation).asEntity(),name,graknGraph));
                    return result;
                }).collect(Collectors.toList());

                // populate the result with resources using graphAPI and relations from additional info query
                return new LdbcQuery1Result(
                        resource(map, aFriendId),
                        resource(map, aFriendLastName),
                        distance,
                        getSingleDateResource(map.get(aFriend).asEntity(), personBirthday, graknGraph),
                        getSingleDateResource(map.get(aFriend).asEntity(), creationDate, graknGraph),
                        getSingleResource(map.get(aFriend).asEntity(), gender, graknGraph),
                        getSingleResource(map.get(aFriend).asEntity(), browserUsed, graknGraph),
                        getSingleResource(map.get(aFriend).asEntity(), locationIp, graknGraph),
                        getListResources(map.get(aFriend).asEntity(), email, graknGraph),
                        getListResources(map.get(aFriend).asEntity(), speaks, graknGraph),
                        getSingleResource(locationResult.get(aLocation).asEntity(), name, graknGraph),
                        universityProcessedResults,
                        workProcessedResults);
            }).collect(Collectors.toList());
        }
        private static <T> T getSingleResource(Entity entity, Label resourceType, GraknGraph graknGraph) {
            return (T) entity.resources(graknGraph.getResourceType(resourceType.toString())).
                    iterator().next().getValue();
        }
        private static long getSingleDateResource(Entity entity, Label resourceType, GraknGraph graknGraph) {
            return ((LocalDateTime) getSingleResource(entity, resourceType, graknGraph)).
                    toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        private static <T> List<T> getListResources(Entity entity, Label resourceType, GraknGraph graknGraph) {
            Stream<Attribute<?>> rawResources = entity.resources(graknGraph.getResourceType(resourceType.toString()));
            return rawResources.map((resource) -> (T) resource.getValue()).collect(Collectors.<T>toList());
        }
    }

    /**
     * Complex Query 13
     */
    public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, GraknDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery13 ldbcQuery13, GraknDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknGraph graknGraph = session.open(GraknTxType.READ)) {
                MatchQuery matchQuery = match(thePerson.has(personID,var().val(ldbcQuery13.person1Id())));
                Concept person1 = matchQuery.withGraph(graknGraph).execute().iterator().next().get(thePerson);
                matchQuery = match(thePerson.has(personID,var().val(ldbcQuery13.person2Id())));
                Concept person2 = matchQuery.withGraph(graknGraph).execute().iterator().next().get(thePerson);

                PathQuery pathQuery = compute().path().from(person1.getId()).to(person2.getId())
                        .in("knows", "person");

                List<Concept> path = pathQuery.withGraph(graknGraph).execute().orElseGet(ArrayList::new);

                // our path is either:
                //     empty if there is none
                //     one if source = destination
                //     2*l+1 where l is the length of the path
                int l = path.size()-1;
                LdbcQuery13Result result;
                if (l<1) {result = new LdbcQuery13Result(l);}
                else {result = new LdbcQuery13Result(l/2);}

                resultReporter.report(0, result, ldbcQuery13);
            }
        }
    }
}
