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

package grakn.core.server.rpc.query;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Context;
import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.ConceptMapGroup;
import grakn.core.concept.answer.Numeric;
import grakn.core.concept.answer.NumericGroup;
import grakn.core.query.QueryManager;
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.common.ResponseBuilder;
import grakn.protocol.QueryProto;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;

import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.iterator.Iterators.iterate;
import static grakn.core.server.rpc.common.RequestReader.setDefaultOptions;
import static grakn.core.server.rpc.common.RequestReader.setQueryOptions;
import static grakn.core.server.rpc.common.ResponseBuilder.Answer.numeric;
import static java.util.stream.Collectors.toList;

public class QueryHandler {

    private final TransactionRPC transactionRPC;
    private final QueryManager queryManager;

    public QueryHandler(TransactionRPC transactionRPC, QueryManager queryManager) {
        this.queryManager = queryManager;
        this.transactionRPC = transactionRPC;
    }

    public void handleRequest(Transaction.Req request) {
        QueryProto.Query.Req req = request.getQueryReq();
        Options.Query options = new Options.Query();
        setDefaultOptions(options, req.getOptions());
        setQueryOptions(options, req.getOptions());
        switch (req.getReqCase()) {
            case DELETE_REQ:
                this.delete(request, req.getDeleteReq(), options);
                return;
            case DEFINE_REQ:
                this.define(request, req.getDefineReq(), options);
                return;
            case UNDEFINE_REQ:
                this.undefine(request, req.getUndefineReq(), options);
                return;
            case MATCH_REQ:
                this.match(request, req.getMatchReq(), options);
                return;
            case MATCH_AGGREGATE_REQ:
                this.match(request, req.getMatchAggregateReq(), options);
                return;
            case MATCH_GROUP_REQ:
                this.match(request, req.getMatchGroupReq(), options);
                return;
            case MATCH_GROUP_AGGREGATE_REQ:
                this.match(request, req.getMatchGroupAggregateReq(), options);
                return;
            case INSERT_REQ:
                this.insert(request, req.getInsertReq(), options);
                return;
            case REQ_NOT_SET:
            default:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static TransactionProto.Transaction.Res response(Transaction.Req request, QueryProto.Query.Res.Builder response) {
        return TransactionProto.Transaction.Res.newBuilder().setId(request.getId()).setQueryRes(response).build();
    }

    private void match(Transaction.Req request, QueryProto.Query.Match.Req req, Options.Query options) {
        GraqlMatch query = Graql.parseQuery(req.getQuery()).asMatch();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        ResourceIterator<ConceptMap> answers = queryManager.match(query, context);
        transactionRPC.respond(
                request, answers, context,
                as -> response(request, QueryProto.Query.Res.newBuilder().setMatchRes(
                        QueryProto.Query.Match.Res.newBuilder().addAllAnswers(
                                as.stream().map(ResponseBuilder.Answer::conceptMap).collect(toList()))))
        );
    }

    private void match(Transaction.Req request, QueryProto.Query.MatchAggregate.Req req, Options.Query options) {
        GraqlMatch.Aggregate query = Graql.parseQuery(req.getQuery()).asMatchAggregate();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        Numeric answer = queryManager.match(query, context);
        transactionRPC.respond(
                response(request, QueryProto.Query.Res.newBuilder().setMatchAggregateRes(
                        QueryProto.Query.MatchAggregate.Res.newBuilder().setAnswer(numeric(answer))))
        );
    }

    private void match(Transaction.Req request, QueryProto.Query.MatchGroup.Req req, Options.Query options) {
        GraqlMatch.Group query = Graql.parseQuery(req.getQuery()).asMatchGroup();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        ResourceIterator<ConceptMapGroup> answers = queryManager.match(query, context);
        transactionRPC.respond(
                request, answers, context,
                as -> response(request, QueryProto.Query.Res.newBuilder().setMatchGroupRes(
                        QueryProto.Query.MatchGroup.Res.newBuilder()
                                .addAllAnswers(iterate(as).map(ResponseBuilder.Answer::conceptMapGroup).toList()))
                )
        );
    }

    private void match(Transaction.Req request, QueryProto.Query.MatchGroupAggregate.Req req, Options.Query options) {
        GraqlMatch.Group.Aggregate query = Graql.parseQuery(req.getQuery()).asMatchGroupAggregate();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        ResourceIterator<NumericGroup> answers = queryManager.match(query, context);
        transactionRPC.respond(
                request, answers, context,
                as -> response(request, QueryProto.Query.Res.newBuilder().setMatchGroupAggregateRes(
                        QueryProto.Query.MatchGroupAggregate.Res.newBuilder().addAllAnswers(
                                as.stream().map(ResponseBuilder.Answer::numericGroup).collect(toList())
                        )
                ))
        );
    }

    private void insert(Transaction.Req request, QueryProto.Query.Insert.Req req, Options.Query options) {
        GraqlInsert query = Graql.parseQuery(req.getQuery()).asInsert();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        ResourceIterator<ConceptMap> answers = queryManager.insert(query, context);
        transactionRPC.respond(
                request, answers, context,
                as -> response(request, QueryProto.Query.Res.newBuilder().setInsertRes(
                        QueryProto.Query.Insert.Res.newBuilder().addAllAnswers(
                                as.stream().map(ResponseBuilder.Answer::conceptMap).collect(toList()))))
        );
    }

    private void delete(Transaction.Req request, QueryProto.Query.Delete.Req req, Options.Query options) {
        GraqlDelete query = Graql.parseQuery(req.getQuery()).asDelete();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        queryManager.delete(query, context);
        transactionRPC.respond(response(request, QueryProto.Query.Res.newBuilder()
                .setDeleteRes(QueryProto.Query.Delete.Res.getDefaultInstance())));
    }

    private void define(Transaction.Req request, QueryProto.Query.Define.Req req, Options.Query options) {
        GraqlDefine query = Graql.parseQuery(req.getQuery()).asDefine();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        queryManager.define(query, context);
        transactionRPC.respond(response(request, QueryProto.Query.Res.newBuilder()
                .setDefineRes(QueryProto.Query.Define.Res.getDefaultInstance())));
    }

    private void undefine(Transaction.Req request, QueryProto.Query.Undefine.Req req, Options.Query options) {
        GraqlUndefine query = Graql.parseQuery(req.getQuery()).asUndefine();
        Context.Query context = new Context.Query(transactionRPC.context(), options.query(query), query);
        queryManager.undefine(query, context);
        transactionRPC.respond(response(request, QueryProto.Query.Res.newBuilder()
                .setUndefineRes(QueryProto.Query.Undefine.Res.getDefaultInstance())));
    }
}
