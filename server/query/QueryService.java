/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.server.query;

import grabl.tracing.client.GrablTracingThreadStatic;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.query.QueryManager;
import grakn.core.server.TransactionService;
import grakn.core.server.common.ResponseBuilder;
import grakn.protocol.QueryProto;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;
import graql.lang.query.GraqlUpdate;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.server.common.RequestReader.applyDefaultOptions;
import static grakn.core.server.common.RequestReader.applyQueryOptions;
import static grakn.core.server.common.ResponseBuilder.Answer.numeric;

public class QueryService {

    private final TransactionService transactionSrv;
    private final QueryManager queryMgr;

    public QueryService(TransactionService transactionSrv, QueryManager queryMgr) {
        this.queryMgr = queryMgr;
        this.transactionSrv = transactionSrv;
    }

    public void execute(Transaction.Req request) {
        try (GrablTracingThreadStatic.ThreadTrace ignored = traceOnThread("query")) {
            QueryProto.Query.Req queryReq = request.getQueryReq();
            Options.Query options = new Options.Query();
            applyDefaultOptions(options, queryReq.getOptions());
            applyQueryOptions(options, queryReq.getOptions());
            switch (queryReq.getReqCase()) {
                case DEFINE_REQ:
                    this.define(queryReq.getDefineReq().getQuery(), options, request);
                    return;
                case UNDEFINE_REQ:
                    this.undefine(queryReq.getUndefineReq().getQuery(), options, request);
                    return;
                case MATCH_REQ:
                    this.match(queryReq.getMatchReq().getQuery(), options, request);
                    return;
                case MATCH_AGGREGATE_REQ:
                    this.matchAggregate(queryReq.getMatchAggregateReq().getQuery(), options, request);
                    return;
                case MATCH_GROUP_REQ:
                    this.matchGroup(queryReq.getMatchGroupReq().getQuery(), options, request);
                    return;
                case MATCH_GROUP_AGGREGATE_REQ:
                    this.matchGroupAggregate(queryReq.getMatchGroupAggregateReq().getQuery(), options, request);
                    return;
                case INSERT_REQ:
                    this.insert(queryReq.getInsertReq().getQuery(), options, request);
                    return;
                case DELETE_REQ:
                    this.delete(queryReq.getDeleteReq().getQuery(), options, request);
                    return;
                case UPDATE_REQ:
                    this.update(queryReq.getUpdateReq().getQuery(), options, request);
                    return;
                case REQ_NOT_SET:
                default:
                    throw GraknException.of(UNKNOWN_REQUEST_TYPE);
            }
        }
    }

    private static TransactionProto.Transaction.Res response(Transaction.Req request, QueryProto.Query.Res.Builder response) {
        return TransactionProto.Transaction.Res.newBuilder().setId(request.getId()).setQueryRes(response).build();
    }

    private void define(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlDefine query = Graql.parseQuery(queryStr).asDefine();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        queryMgr.define(query, context);
        transactionSrv.respond(response(request, QueryProto.Query.Res.newBuilder()
                .setDefineRes(QueryProto.Query.Define.Res.getDefaultInstance())));
    }

    private void undefine(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlUndefine query = Graql.parseQuery(queryStr).asUndefine();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        queryMgr.undefine(query, context);
        transactionSrv.respond(response(request, QueryProto.Query.Res.newBuilder()
                .setUndefineRes(QueryProto.Query.Undefine.Res.getDefaultInstance())));
    }

    private void match(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlMatch query = Graql.parseQuery(queryStr).asMatch();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        FunctionalIterator<ConceptMap> answers = queryMgr.match(query, context);
        transactionSrv.stream(answers, request.getId(), context.options(), as -> response(
                request, QueryProto.Query.Res.newBuilder().setMatchRes(
                        QueryProto.Query.Match.Res.newBuilder().addAllAnswers(
                                iterate(as).map(ResponseBuilder.Answer::conceptMap).toList()))
        ));
    }

    private void matchAggregate(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlMatch.Aggregate query = Graql.parseQuery(queryStr).asMatchAggregate();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        Numeric answer = queryMgr.match(query, context);
        transactionSrv.respond(
                response(request, QueryProto.Query.Res.newBuilder().setMatchAggregateRes(
                        QueryProto.Query.MatchAggregate.Res.newBuilder().setAnswer(numeric(answer)))));
    }

    private void matchGroup(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlMatch.Group query = Graql.parseQuery(queryStr).asMatchGroup();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        FunctionalIterator<ConceptMapGroup> answers = queryMgr.match(query, context);
        transactionSrv.stream(answers, request.getId(), context.options(), as -> response(
                request, QueryProto.Query.Res.newBuilder().setMatchGroupRes(
                        QueryProto.Query.MatchGroup.Res.newBuilder().addAllAnswers(
                                iterate(as).map(ResponseBuilder.Answer::conceptMapGroup).toList()))
        ));
    }

    private void matchGroupAggregate(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlMatch.Group.Aggregate query = Graql.parseQuery(queryStr).asMatchGroupAggregate();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        FunctionalIterator<NumericGroup> answers = queryMgr.match(query, context);
        transactionSrv.stream(answers, request.getId(), context.options(), as -> response(
                request, QueryProto.Query.Res.newBuilder().setMatchGroupAggregateRes(
                        QueryProto.Query.MatchGroupAggregate.Res.newBuilder().addAllAnswers(
                                iterate(as).map(ResponseBuilder.Answer::numericGroup).toList()))
        ));
    }

    private void insert(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlInsert query = Graql.parseQuery(queryStr).asInsert();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        FunctionalIterator<ConceptMap> answers = queryMgr.insert(query, context);
        transactionSrv.stream(answers, request.getId(), context.options(), as -> response(
                request, QueryProto.Query.Res.newBuilder().setInsertRes(
                        QueryProto.Query.Insert.Res.newBuilder().addAllAnswers(
                                iterate(as).map(ResponseBuilder.Answer::conceptMap).toList()))
        ));
    }

    private void delete(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlDelete query = Graql.parseQuery(queryStr).asDelete();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        queryMgr.delete(query, context);
        transactionSrv.respond(response(request, QueryProto.Query.Res.newBuilder()
                .setDeleteRes(QueryProto.Query.Delete.Res.getDefaultInstance())));
    }

    private void update(String queryStr, Options.Query options, Transaction.Req request) {
        GraqlUpdate query = Graql.parseQuery(queryStr).asUpdate();
        Context.Query context = new Context.Query(transactionSrv.context(), options.query(query), query);
        FunctionalIterator<ConceptMap> answers = queryMgr.update(query, context);
        transactionSrv.stream(answers, request.getId(), context.options(), as -> response(
                request, QueryProto.Query.Res.newBuilder().setUpdateRes(
                        QueryProto.Query.Update.Res.newBuilder().addAllAnswers(
                                iterate(as).map(ResponseBuilder.Answer::conceptMap).toList()))
        ));
    }
}
