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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concept.answer.ConceptMapGroup;
import com.vaticle.typedb.core.concept.answer.ReadableConceptTree;
import com.vaticle.typedb.core.concept.answer.ValueGroup;
import com.vaticle.typedb.core.query.QueryManager;
import com.vaticle.typedb.core.reasoner.answer.Explanation;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.protocol.QueryProto;
import com.vaticle.typedb.protocol.TransactionProto;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import com.vaticle.typeql.lang.query.TypeQLDelete;
import com.vaticle.typeql.lang.query.TypeQLFetch;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLUndefine;
import com.vaticle.typeql.lang.query.TypeQLUpdate;

import java.util.UUID;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.server.common.RequestReader.applyDefaultOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.applyQueryOptions;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.defineRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.explainResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.fetchResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.insertResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.getAggregateRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.getGroupAggregateResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.getGroupResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.QueryManager.getResPart;
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
            case GET_REQ:
                this.get(queryReq.getGetReq().getQuery(), options, reqID);
                return;
            case GET_AGGREGATE_REQ:
                this.getAggregate(queryReq.getGetAggregateReq().getQuery(), options, reqID);
                return;
            case GET_GROUP_REQ:
                this.getGroup(queryReq.getGetGroupReq().getQuery(), options, reqID);
                return;
            case GET_GROUP_AGGREGATE_REQ:
                this.getGroupAggregate(queryReq.getGetGroupAggregateReq().getQuery(), options, reqID);
                return;
            case FETCH_REQ:
                this.fetch(queryReq.getFetchReq().getQuery(), options, reqID);
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

    private void get(String queryStr, Options.Query options, UUID reqID) {
        TypeQLGet query = TypeQL.parseQuery(queryStr).asGet();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<? extends ConceptMap> answers = queryMgr.get(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> getResPart(reqID, a));
    }

    private void getAggregate(String queryStr, Options.Query options, UUID reqID) {
        TypeQLGet.Aggregate query = TypeQL.parseQuery(queryStr).asGetAggregate();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        transactionSvc.respond(getAggregateRes(reqID, queryMgr.get(query, context)));
    }

    private void getGroup(String queryStr, Options.Query options, UUID reqID) {
        TypeQLGet.Group query = TypeQL.parseQuery(queryStr).asGetGroup();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<ConceptMapGroup> answers = queryMgr.get(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> getGroupResPart(reqID, a));
    }

    private void getGroupAggregate(String queryStr, Options.Query options, UUID reqID) {
        TypeQLGet.Group.Aggregate query = TypeQL.parseQuery(queryStr).asGetGroupAggregate();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<ValueGroup> answers = queryMgr.get(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> getGroupAggregateResPart(reqID, a));
    }

    private void fetch(String queryStr, Options.Query options, UUID reqID) {
        TypeQLFetch query = TypeQL.parseQuery(queryStr).asFetch();
        Context.Query context = new Context.Query(transactionSvc.context(), options.query(query), query);
        FunctionalIterator<ReadableConceptTree> answers = queryMgr.fetch(query, context);
        transactionSvc.stream(answers, reqID, context.options(), a -> fetchResPart(reqID, a));
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
