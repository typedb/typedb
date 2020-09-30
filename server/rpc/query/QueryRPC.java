/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Options;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.type.ThingType;
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.QueryProto;
import grakn.protocol.TransactionProto;
import graql.lang.Graql;
import graql.lang.query.GraqlDefine;
import graql.lang.query.GraqlDelete;
import graql.lang.query.GraqlInsert;
import graql.lang.query.GraqlMatch;
import graql.lang.query.GraqlUndefine;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.server.rpc.util.RequestReader.getOptions;
import static grakn.core.server.rpc.util.ResponseBuilder.Answer.conceptMap;

public class QueryRPC {

    private final Grakn.Transaction transaction;
    private final TransactionRPC.Iterators iterators;
    private final Consumer<TransactionProto.Transaction.Res> responder;

    public QueryRPC(final Grakn.Transaction transaction, final TransactionRPC.Iterators iterators, final Consumer<TransactionProto.Transaction.Res> responder) {
        this.transaction = transaction;
        this.iterators = iterators;
        this.responder = responder;
    }

    private static TransactionProto.Transaction.Res response(final QueryProto.Query.Res response) {
        return TransactionProto.Transaction.Res.newBuilder().setQueryRes(response).build();
    }

    public void execute(final QueryProto.Query.Req req) {
        final Options.Query options = getOptions(Options.Query::new, req.getOptions());
        switch (req.getReqCase()) {
            case DELETE_REQ:
                this.delete(options, req.getDeleteReq());
                return;
            case DEFINE_REQ:
                this.define(options, req.getDefineReq());
                return;
            case UNDEFINE_REQ:
                this.undefine(options, req.getUndefineReq());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    public void iterate(final QueryProto.Query.Iter.Req req) {
        final Options.Query options = getOptions(Options.Query::new, req.getOptions());
        switch (req.getReqCase()) {
            case MATCH_ITER_REQ:
                this.match(options, req.getMatchIterReq());
                return;
            case INSERT_ITER_REQ:
                this.insert(options, req.getInsertIterReq());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    private void match(final Options.Query options, final QueryProto.Graql.Match.Iter.Req req) {
        final GraqlMatch query = Graql.parse(req.getQuery()).asMatch();
        final Stream<ConceptMap> answers = transaction.query().match(query, options);
        final Stream<TransactionProto.Transaction.Res> responses = answers.map(a ->
                                                                                       ResponseBuilder.Transaction.Iter.query(QueryProto.Query.Iter.Res.newBuilder()
                                                                                                                                      .setMatchIterRes(QueryProto.Graql.Match.Iter.Res.newBuilder()
                                                                                                                                                               .setAnswer(conceptMap(a))).build()));
        iterators.startBatchIterating(responses.iterator());
    }

    private void insert(final Options.Query options, final QueryProto.Graql.Insert.Iter.Req req) {
        final GraqlInsert query = Graql.parse(req.getQuery()).asInsert();
        final Stream<ConceptMap> answers = transaction.query().insert(query, options);
        final Stream<TransactionProto.Transaction.Res> responses = answers.map(a ->
                                                                                       ResponseBuilder.Transaction.Iter.query(QueryProto.Query.Iter.Res.newBuilder()
                                                                                                                                      .setInsertIterRes(QueryProto.Graql.Insert.Iter.Res.newBuilder()
                                                                                                                                                                .setAnswer(conceptMap(a))).build()));
        iterators.startBatchIterating(responses.iterator());
    }

    private void delete(final Options.Query options, final QueryProto.Graql.Delete.Req req) {
        final GraqlDelete query = Graql.parse(req.getQuery()).asDelete();
        transaction.query().delete(query, options);
        responder.accept(null);
    }

    private void define(final Options.Query options, final QueryProto.Graql.Define.Req req) {
        final GraqlDefine query = Graql.parse(req.getQuery()).asDefine();
        final List<ThingType> thingTypes = transaction.query().define(query, options);
        final QueryProto.Query.Res response = QueryProto.Query.Res.newBuilder()
                .setDefineRes(QueryProto.Graql.Define.Res.newBuilder()
                                      .addAllThingType(thingTypes.stream()
                                                               .map(ResponseBuilder.Concept::type)
                                                               .collect(Collectors.toList()))).build();
        responder.accept(response(response));
    }

    private void undefine(final Options.Query options, final QueryProto.Graql.Undefine.Req req) {
        final GraqlUndefine query = Graql.parse(req.getQuery()).asUndefine();
        transaction.query().undefine(query, options);
        responder.accept(null);
    }
}
