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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */
package ai.grakn;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.Order;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ai.grakn.SNB.$content;
import static ai.grakn.SNB.$date;
import static ai.grakn.SNB.$firstName;
import static ai.grakn.SNB.$friend;
import static ai.grakn.SNB.$friendId;
import static ai.grakn.SNB.$lastName;
import static ai.grakn.SNB.$message;
import static ai.grakn.SNB.$messageId;
import static ai.grakn.SNB.$person;
import static ai.grakn.SNB.BIRTHDAY;
import static ai.grakn.SNB.BROWSER_USED;
import static ai.grakn.SNB.CLASS_YEAR;
import static ai.grakn.SNB.CONTENT;
import static ai.grakn.SNB.CREATION_DATE;
import static ai.grakn.SNB.EMAIL;
import static ai.grakn.SNB.FIRST_NAME;
import static ai.grakn.SNB.GENDER;
import static ai.grakn.SNB.HAS_CREATOR;
import static ai.grakn.SNB.IMAGE_FILE;
import static ai.grakn.SNB.IS_LOCATED_IN;
import static ai.grakn.SNB.KNOWS;
import static ai.grakn.SNB.LAST_NAME;
import static ai.grakn.SNB.LOCATION_IP;
import static ai.grakn.SNB.MESSAGE_ID;
import static ai.grakn.SNB.NAME;
import static ai.grakn.SNB.PERSON_ID;
import static ai.grakn.SNB.REPLY;
import static ai.grakn.SNB.REPLY_OF;
import static ai.grakn.SNB.SPEAKS;
import static ai.grakn.SNB.STUDY_AT;
import static ai.grakn.SNB.WORK_AT;
import static ai.grakn.SNB.WORK_FROM;
import static ai.grakn.SNB.by;
import static ai.grakn.SNB.resource;
import static ai.grakn.SNB.toEpoch;
import static ai.grakn.graql.Graql.compute;
import static ai.grakn.graql.Graql.lte;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.or;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.GraqlSyntax.Compute.Method.PATH;

/**
 * Implementations of the LDBC SNB complex queries.
 *
 * @author sheldon
 */
public class GraknQueryHandlers {

    private GraknQueryHandlers() {
    }

    /**
     * Complex Query 2
     */
    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, GraknDbConnectionState> {

        @Override
        public void executeOperation(LdbcQuery2 ldbcQuery2, GraknDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graknTx = session.transaction(GraknTxType.READ)) {
                LocalDateTime maxDate = SNB.fromDate(ldbcQuery2.maxDate());

                // to make this query execute faster split it into two parts:
                //     the first does the ordering
                //     the second fetches the resources
                Match graknLdbcQuery2 = match(
                        var().rel($person.has(PERSON_ID, ldbcQuery2.personId())).rel($friend).isa(KNOWS),
                        var().rel($friend).rel($message).isa(HAS_CREATOR),
                        $message.has(CREATION_DATE, $date).has(MESSAGE_ID, $messageId),
                        $date.val(lte(maxDate)));

                List<Answer> rawResult = graknLdbcQuery2.orderBy($date, Order.desc)
                        .limit(ldbcQuery2.limit()).withTx(graknTx).get().execute();

                // process the query results
                List<LdbcQuery2Result> result = rawResult.stream()
                        // sort first by date and then by message id
                        .sorted(Comparator.comparing(by($date)).reversed().thenComparing(by($messageId)))
                        .map(map -> {
                            // fetch the resources attached to entities in the queries
                            Match queryExtendedInfo = match(
                                    $friend.has(FIRST_NAME, $firstName).has(LAST_NAME, $lastName).has(PERSON_ID, $friendId),
                                    var().rel($friend).rel($message).isa(HAS_CREATOR),
                                    $message.has(CREATION_DATE, $date)
                                            .has(MESSAGE_ID, SNB.<Long>resource(map, $messageId)),
                                    or($message.has(CONTENT, $content), $message.has(IMAGE_FILE, $content)));
                            Answer extendedInfo = queryExtendedInfo.withTx(graknTx).get().execute().iterator().next();

                            // prepare the answer from the original query and the query for extended information
                            return new LdbcQuery2Result(
                                    resource(extendedInfo, $friendId),
                                    resource(extendedInfo, $firstName),
                                    resource(extendedInfo, $lastName),
                                    resource(map, $messageId),
                                    resource(extendedInfo, $content),
                                    toEpoch(resource(map, $date)));
                        }).collect(Collectors.toList());

                resultReporter.report(0, result, ldbcQuery2);
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
            try (GraknTx graknTx = session.transaction(GraknTxType.READ)) {
                // for speed the query is again split into the ordering and limit phase
                Var reply = var("aReply");
                Var responder = var("responder");
                Var responderId = var("responderId");
                Match orderQuery = match(
                        $person.has(PERSON_ID, ldbcQuery8.personId()),
                        var().rel($person).rel($message).isa(HAS_CREATOR),
                        var().rel($message).rel(REPLY, reply).isa(REPLY_OF),
                        reply.has(CREATION_DATE, $date).has(MESSAGE_ID, $messageId)
                );
                List<Answer> rawResult = orderQuery.withTx(graknTx)
                        .orderBy($date, Order.desc).limit(ldbcQuery8.limit()).get().execute();

                // sort first by date and then by message id

                // process the query results
                List<LdbcQuery8Result> result = rawResult.stream()
                        .sorted(Comparator.comparing(by($date)).reversed().thenComparing(by($messageId)))
                        .map(map -> {
                            // fetch the resources attached to entities in the queries
                            Match queryExtendedInfo = match(
                                    reply.has(MESSAGE_ID, SNB.<Long>resource(map, $messageId)),
                                    or(reply.has(CONTENT, $content), reply.has(IMAGE_FILE, $content)),
                                    var().rel(reply).rel(responder).isa(HAS_CREATOR),
                                    responder.has(PERSON_ID, responderId).has(FIRST_NAME, $firstName).has(LAST_NAME, $lastName)
                            );
                            Answer extendedInfo = queryExtendedInfo.withTx(graknTx).get().execute().iterator().next();

                            // prepare the answer from the original query and the query for extended information
                            return new LdbcQuery8Result(
                                    resource(extendedInfo, responderId),
                                    resource(extendedInfo, $firstName),
                                    resource(extendedInfo, $lastName),
                                    toEpoch(resource(map, $date)),
                                    resource(map, $messageId),
                                    resource(extendedInfo, $content));
                        }).collect(Collectors.toList());

                resultReporter.report(0, result, ldbcQuery8);
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
            try (GraknTx graknTx = session.transaction(GraknTxType.READ)) {
                Var anyone = var("anyone");
                Var anyoneElse = var("anyoneElse");

                // for speed fetch the Grakn id first
                ConceptId graknPersonId = match($person.has(PERSON_ID, ldbcQuery1.personId())).withTx(graknTx).
                        get().execute().iterator().next().get($person).getId();

                // sort by lastname and then id
                Comparator<Answer> byLastNameAndId = Comparator.comparing(by($lastName)).thenComparing(by($friendId));

                // This query has to be split into 3 parts, each fetching people a further distance away
                // The longer queries only need be executed if there are not enough shorter queries
                // The last ordering by id must be done after each query has been executed
                Match match = match($person.id(graknPersonId),
                        var().rel($person).rel($friend).isa(KNOWS),
                        $friend.has(FIRST_NAME, ldbcQuery1.firstName()).
                                has(LAST_NAME, $lastName).
                                has(PERSON_ID, $friendId),
                        $person.neq($friend));
                List<Answer> distance1Result = match.withTx(graknTx).get().execute();
                List<LdbcQuery1Result> distance1LdbcResult = populateResults(distance1Result.stream().sorted(byLastNameAndId), ldbcQuery1, graknTx, 1);
                if (distance1Result.size() < ldbcQuery1.limit()) {
                    match = match($person.id(graknPersonId),
                            var().rel($person).rel(anyone).isa(KNOWS),
                            var().rel(anyone).rel($friend).isa(KNOWS),
                            $friend.has(FIRST_NAME, ldbcQuery1.firstName()).
                                    has(LAST_NAME, $lastName).
                                    has(PERSON_ID, $friendId),
                            $person.neq($friend)
                    );
                    List<Answer> distance2Result = match.withTx(graknTx).get().execute();
                    distance1LdbcResult.addAll(populateResults(distance2Result.stream().sorted(byLastNameAndId), ldbcQuery1, graknTx, 2));
                    if (distance1Result.size() + distance2Result.size() < ldbcQuery1.limit()) {
                        match = match($person.id(graknPersonId),
                                var().rel($person).rel(anyone).isa(KNOWS),
                                var().rel(anyone).rel(anyoneElse).isa(KNOWS),
                                var().rel(anyoneElse).rel($friend).isa(KNOWS),
                                $friend.has(FIRST_NAME, ldbcQuery1.firstName()).
                                        has(LAST_NAME, $lastName).
                                        has(PERSON_ID, $friendId),
                                $person.neq($friend),
                                $friend.neq(anyone)
                        );
                        List<Answer> distance3Result = match.withTx(graknTx).get().execute();
                        distance1LdbcResult.addAll(populateResults(distance3Result.stream().sorted(byLastNameAndId), ldbcQuery1, graknTx, 3));
                    }
                }
                resultReporter.report(0, distance1LdbcResult, ldbcQuery1);
            }
        }

        /**
         * Populate the LdbcQuery1Result object from graql results. As part of this extra queries are executed to fetch related information.
         *
         * @param graqlResults the graql results used to populate the ldbc results
         * @param ldbcQuery1   the ldbc query parameters
         * @param graknTx      the graph for additional queries
         * @param distance     the number of knows relations between initial person and these results
         * @return the ldbc results
         */
        private static List<LdbcQuery1Result> populateResults(Stream<Answer> graqlResults, LdbcQuery1 ldbcQuery1, GraknTx graknTx, int distance) {
            return graqlResults.limit(ldbcQuery1.limit()).map(map -> {
                // these queries get all of the additional related material, excluding resources
                Var location = var("aLocation");
                Match locationQuery = match(
                        $friend.id(map.get($friend).getId()),
                        var().rel($friend).rel(location).isa(IS_LOCATED_IN));
                Answer locationResult = locationQuery.withTx(graknTx).get().execute().iterator().next();

                Var year = var("aYear");
                Var oganisation = var("aOrganisation");
                Match universityQuery = match(
                        $friend.id(map.get($friend).getId()),
                        var().rel($friend).rel(oganisation).isa(STUDY_AT).has(CLASS_YEAR, year),
                        var().rel(oganisation).rel(location).isa(IS_LOCATED_IN)
                );
                List<Answer> universityResults = universityQuery.withTx(graknTx).get().execute();
                List<List<Object>> universityProcessedResults = universityResults.stream().map(answer -> {
                    List<Object> result = new ArrayList<>();
                    result.add(getSingleResource(answer.get(oganisation).asEntity(), NAME, graknTx));
                    result.add(resource(answer, year));
                    result.add(getSingleResource(answer.get(location).asEntity(), NAME, graknTx));
                    return result;
                }).collect(Collectors.toList());

                Match workQuery = match(
                        $friend.id(map.get($friend).getId()),
                        var().rel($friend).rel(oganisation).isa(WORK_AT).has(WORK_FROM, year),
                        var().rel(oganisation).rel(location).isa(IS_LOCATED_IN)
                );
                List<Answer> workResults = workQuery.withTx(graknTx).get().execute();
                List<List<Object>> workProcessedResults = workResults.stream().map(answer -> {
                    List<Object> result = new ArrayList<>();
                    result.add(getSingleResource(answer.get(oganisation).asEntity(), NAME, graknTx));
                    result.add(resource(answer, year));
                    result.add(getSingleResource(answer.get(location).asEntity(), NAME, graknTx));
                    return result;
                }).collect(Collectors.toList());

                // populate the result with resources using graphAPI and relations from additional info query
                return new LdbcQuery1Result(
                        resource(map, $friendId),
                        resource(map, $lastName),
                        distance,
                        toEpoch(getSingleResource(map.get($friend).asEntity(), BIRTHDAY, graknTx)),
                        toEpoch(getSingleResource(map.get($friend).asEntity(), CREATION_DATE, graknTx)),
                        getSingleResource(map.get($friend).asEntity(), GENDER, graknTx),
                        getSingleResource(map.get($friend).asEntity(), BROWSER_USED, graknTx),
                        getSingleResource(map.get($friend).asEntity(), LOCATION_IP, graknTx),
                        getListResources(map.get($friend).asEntity(), EMAIL, graknTx),
                        getListResources(map.get($friend).asEntity(), SPEAKS, graknTx),
                        getSingleResource(locationResult.get(location).asEntity(), NAME, graknTx),
                        universityProcessedResults,
                        workProcessedResults);
            }).collect(Collectors.toList());
        }

        private static <T> T getSingleResource(Entity entity, String resourceType, GraknTx graknTx) {
            return (T) entity.attributes(graknTx.getAttributeType(resourceType)).
                    iterator().next().getValue();
        }

        private static <T> List<T> getListResources(Entity entity, String resourceType, GraknTx graknTx) {
            Stream<Attribute<?>> rawResources = entity.attributes(graknTx.getAttributeType(resourceType));
            return rawResources.map(resource -> (T) resource.getValue()).collect(Collectors.toList());
        }
    }

    /**
     * Complex Query 13
     */
    public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, GraknDbConnectionState> {
        @Override
        public void executeOperation(LdbcQuery13 ldbcQuery13, GraknDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            GraknSession session = dbConnectionState.session();
            try (GraknTx graknTx = session.transaction(GraknTxType.READ)) {
                Match match = match($person.has(PERSON_ID, ldbcQuery13.person1Id()));
                Concept person1 = match.withTx(graknTx).get().execute().iterator().next().get($person);
                match = match($person.has(PERSON_ID, ldbcQuery13.person2Id()));
                Concept person2 = match.withTx(graknTx).get().execute().iterator().next().get($person);

                ComputeQuery pathQuery = compute(PATH).from(person1.getId()).to(person2.getId())
                        .in("knows", "person");

                List<List<ConceptId>> paths = pathQuery.withTx(graknTx).execute().getPaths().get();

                List<ConceptId> path = Collections.emptyList();
                if (!paths.isEmpty()) path = paths.get(0);

                // our path is either:
                //     empty if there is none
                //     one if source = destination
                //     2*l+1 where l is the length of the path
                int l = path.size() - 1;
                LdbcQuery13Result result;
                if (l < 1) {
                    result = new LdbcQuery13Result(l);
                } else {
                    result = new LdbcQuery13Result(l / 2);
                }

                resultReporter.report(0, result, ldbcQuery13);
            }
        }
    }
}
