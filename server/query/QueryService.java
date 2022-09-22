/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.server.query;

import com.vaticle.factory.tracing.client.FactoryTracingThreadStatic;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.NumericGroup;
import com.vaticle.typedb.core.query.QueryManager;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.protocol.QueryProto;
import com.vaticle.typedb.protocol.TransactionProto;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLUndefine;
import com.vaticle.typeql.lang.query.TypeQLUpdate;

import java.util.UUID;

import static com.vaticle.factory.tracing.client.FactoryTracingThreadStatic.traceOnThread;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.server.common.RequestReader.applyDefaultOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.applyQueryOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.defineRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.explainResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.insertResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.matchAggregateRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.matchGroupAggregateResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.matchGroupResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.matchResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.undefineRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.updateResPart;

public class QueryService {

    private final QueryManager queryMgr;
    private final TransactionService transactionSvc;

    public QueryService(TransactionService transactionSvc, QueryManager queryMgr) {
        this.queryMgr = queryMgr;
        this.transactionSvc = transactionSvc;
    }

    public void execute(TransactionProto.Transaction.Req req) {
        try (FactoryTracingThreadStatic.ThreadTrace ignored = traceOnThread("query")) {
            QueryProto.QueryManager.Req queryReq = req.getQueryManagerReq();
            Options.Query options = new Options.Query();
            applyDefaultOptions(options, queryReq.getOptions());
            applyQueryOptions(options, queryReq.getOptions());
            UUID reqID = byteStringAsUUID(req.getReqId());
            switch (queryReq.getReqCase()) {
                case DEFINE_REQ:
                    this.define(queryReq.getDefineReq().getQuery(), options, reqID);
                    return;
                case UNDEFINE_REQ:
                    this.undefine(queryReq.getUndefineReq().getQuery(), options, reqID);
                    return;
                case MATCH_REQ:
                    this.match(queryReq.getMatchReq().getQuery(), options, reqID);
                    return;
                case MATCH_AGGREGATE_REQ:
                    this.matchAggregate(queryReq.getMatchAggregateReq().getQuery(), options, reqID);
                    return;
                case MATCH_GROUP_REQ:
                    this.matchGroup(queryReq.getMatchGroupReq().getQuery(), options, reqID);
                    return;
                case MATCH_GROUP_AGGREGATE_REQ:
                    this.matchGroupAggregate(queryReq.getMatchGroupAggregateReq().getQuery(), options, reqID);
                    return;
                case INSERT_REQ:
                    this.insert(queryReq.getInsertReq().getQuery(), options, reqID);
                    return;
                case DELETE_REQ:
                    this.delete(queryReq.getDeleteReq().getQuery(), options, reqID);
                    return;
                case UPDATE_REQ:
                    this.update(queryReq.getUpdateReq().getQuery(), options, reqID);
                    return;
                case EXPLAIN_REQ:
                    this.explain(queryReq.getExplainReq().getExplainableId(), reqID);
                    return;
                case REQ_NOT_SET:
                default:
                    throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
            }
        }
    }

    private void define(String queryStr, Options.Query options, UUID reqID) {
        TypeQLDefine query = TypeQL.parseQuery(queryStr).asDefine();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        queryMgr.define(query, context);
        transactionSvc.respond(defineRes(reqID));
    }

    private void undefine(String queryStr, Options.Query options, UUID reqID) {
        TypeQLUndefine query = TypeQL.parseQuery(queryStr).asUndefine();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        queryMgr.undefine(query, context);
        transactionSvc.respond(undefineRes(reqID));
    }

    private void match(String queryStr, Options.Query options, UUID reqID) {
        TypeQLMatch query = TypeQL.parseQuery(queryStr).asMatch();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<? extends ConceptMap> answers = queryMgr.match(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> matchResPart(reqID, a));
    }

    private void matchAggregate(String queryStr, Options.Query options, UUID reqID) {
        TypeQLMatch.Aggregate query = TypeQL.parseQuery(queryStr).asMatchAggregate();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        transactionSvc.respond(matchAggregateRes(reqID, queryMgr.match(query, context)));
    }

    private void matchGroup(String queryStr, Options.Query options, UUID reqID) {
        TypeQLMatch.Group query = TypeQL.parseQuery(queryStr).asMatchGroup();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<ConceptMapGroup> answers = queryMgr.match(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> matchGroupResPart(reqID, a));
    }

    private void matchGroupAggregate(String queryStr, Options.Query options, UUID reqID) {
        TypeQLMatch.Group.Aggregate query = TypeQL.parseQuery(queryStr).asMatchGroupAggregate();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<NumericGroup> answers = queryMgr.match(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> matchGroupAggregateResPart(reqID, a));
    }

    private void insert(String queryStr, Options.Query options, UUID reqID) {
        TypeQLInsert query = TypeQL.parseQuery(queryStr).asInsert();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<ConceptMap> answers = queryMgr.insert(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> insertResPart(reqID, a));
    }

    private void delete(String queryStr, Options.Query options, UUID reqID) {
        TypeQLDelete query = TypeQL.parseQuery(queryStr).asDelete();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        queryMgr.delete(query, context);
        transactionSvc.respond(deleteRes(reqID));
    }

    private void update(String queryStr, Options.Query options, UUID reqID) {
        TypeQLUpdate query = TypeQL.parseQuery(queryStr).asUpdate();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<ConceptMap> answers = queryMgr.update(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> updateResPart(reqID, a));
    }

    private void explain(long explainableId, UUID reqID) {
        FunctionalIterator<Explanation> explanations = queryMgr.explain(explainableId);
        transactionSvc.stream(explanations, reqID, a -> explainResPart(reqID, a));
    }

}
