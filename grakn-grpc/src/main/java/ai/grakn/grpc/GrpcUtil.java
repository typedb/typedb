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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.grpc;

/*-
 * #%L
 * grakn-grpc
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.exception.TemporaryWriteException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.AttributeValue;
import ai.grakn.rpc.generated.GrpcConcept.Concepts;
import ai.grakn.rpc.generated.GrpcConcept.OptionalConcept;
import ai.grakn.rpc.generated.GrpcConcept.OptionalDataType;
import ai.grakn.rpc.generated.GrpcConcept.OptionalPattern;
import ai.grakn.rpc.generated.GrpcConcept.OptionalRegex;
import ai.grakn.rpc.generated.GrpcConcept.RolePlayers;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.Commit;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteResponse;
import ai.grakn.rpc.generated.GrpcGrakn.Done;
import ai.grakn.rpc.generated.GrpcGrakn.ExecQuery;
import ai.grakn.rpc.generated.GrpcGrakn.Infer;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.PutAttributeType;
import ai.grakn.rpc.generated.GrpcGrakn.PutRule;
import ai.grakn.rpc.generated.GrpcGrakn.RunConceptMethod;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcGrakn.TxType;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.generated.GrpcIterator.Next;
import ai.grakn.rpc.generated.GrpcIterator.Stop;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimaps;
import io.grpc.Metadata;
import io.grpc.Metadata.AsciiMarshaller;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * @author Felix Chapman
 */
public class GrpcUtil {

    private static final GrpcConcept.Unit UNIT = GrpcConcept.Unit.getDefaultInstance();

    static class UnknownGraknException extends GraknException {

        private static final long serialVersionUID = 4354432748314041017L;

        UnknownGraknException(String error) {
            super(error);
        }

        public static UnknownGraknException create(String message) {
            return new UnknownGraknException(message);
        }
    }

    /**
     * Enumeration of all sub-classes of {@link GraknException} that can be thrown during gRPC calls.
     */
    public enum ErrorType {
        // TODO: it's likely some of these will NEVER be thrown normally, so shouldn't be here
        GRAQL_QUERY_EXCEPTION(GraqlQueryException::create),
        GRAQL_SYNTAX_EXCEPTION(GraqlSyntaxException::create),
        GRAKN_TX_OPERATION_EXCEPTION(GraknTxOperationException::create),
        TEMPORARY_WRITE_EXCEPTION(TemporaryWriteException::create),
        GRAKN_SERVER_EXCEPTION(GraknServerException::create),
        PROPERTY_NOT_UNIQUE_EXCEPTION(PropertyNotUniqueException::create),
        INVALID_KB_EXCEPTION(InvalidKBException::create),
        GRAKN_BACKEND_EXCEPTION(GraknBackendException::create),
        UNKNOWN(UnknownGraknException::create);

        // Enums are meant to be serializable, but functions can't be serialized
        private transient final Function<String, GraknException> converter;

        ErrorType(Function<String, GraknException> converter) {
            this.converter = converter;
        }

        public final GraknException toException(String message) {
            return converter.apply(message);
        }

        private static final AsciiMarshaller<ErrorType> ERROR_TYPE_ASCII_MARSHALLER = new AsciiMarshaller<ErrorType>() {
            @Override
            public String toAsciiString(ErrorType value) {
                return value.name();
            }

            @Override
            public ErrorType parseAsciiString(String serialized) {
                return ErrorType.valueOf(serialized);
            }
        };

        public static final Metadata.Key<ErrorType> KEY = Metadata.Key.of("ErrorType", ERROR_TYPE_ASCII_MARSHALLER);
    }

    public static TxRequest openRequest(Keyspace keyspace, GraknTxType txType) {
        Open.Builder open = Open.newBuilder().setKeyspace(convert(keyspace)).setTxType(convert(txType));
        return TxRequest.newBuilder().setOpen(open).build();
    }

    public static TxRequest commitRequest() {
        return TxRequest.newBuilder().setCommit(Commit.getDefaultInstance()).build();
    }

    public static TxRequest execQueryRequest(Query<?> query) {
        return execQueryRequest(query.toString(), query.inferring());
    }

    public static TxRequest execQueryRequest(String queryString, @Nullable Boolean infer) {
        GrpcGrakn.Query query = GrpcGrakn.Query.newBuilder().setValue(queryString).build();
        ExecQuery.Builder execQueryRequest = ExecQuery.newBuilder().setQuery(query);
        if (infer != null) {
            execQueryRequest.setInfer(Infer.newBuilder().setValue(infer));
        }
        return TxRequest.newBuilder().setExecQuery(execQueryRequest).build();
    }

    public static TxRequest nextRequest(IteratorId iteratorId) {
        return TxRequest.newBuilder().setNext(Next.newBuilder().setIteratorId(iteratorId).build()).build();
    }

    public static TxRequest stopRequest(IteratorId iteratorId) {
        return TxRequest.newBuilder().setStop(Stop.newBuilder().setIteratorId(iteratorId).build()).build();
    }

    public static TxRequest runConceptMethodRequest(ConceptId id, ConceptMethod<?> conceptMethod) {
        RunConceptMethod runConceptMethod = RunConceptMethod.newBuilder()
                .setId(convert(id))
                .setConceptMethod(conceptMethod.toGrpc())
                .build();
        return TxRequest.newBuilder().setRunConceptMethod(runConceptMethod).build();
    }

    public static TxRequest getConceptRequest(ConceptId id) {
        return TxRequest.newBuilder().setGetConcept(convert(id)).build();
    }

    public static TxRequest getSchemaConceptRequest(Label label) {
        return TxRequest.newBuilder().setGetSchemaConcept(convert(label)).build();
    }

    public static TxRequest getAttributesByValueRequest(Object value) {
        return TxRequest.newBuilder().setGetAttributesByValue(convertValue(value)).build();
    }

    public static TxRequest putEntityTypeRequest(Label label) {
        return TxRequest.newBuilder().setPutEntityType(convert(label)).build();
    }

    public static TxRequest putRelationshipTypeRequest(Label label) {
        return TxRequest.newBuilder().setPutRelationshipType(convert(label)).build();
    }

    public static TxRequest putAttributeTypeRequest(Label label, AttributeType.DataType<?> dataType) {
        PutAttributeType putAttributeType =
                PutAttributeType.newBuilder().setLabel(convert(label)).setDataType(convert(dataType)).build();

        return TxRequest.newBuilder().setPutAttributeType(putAttributeType).build();
    }

    public static TxRequest putRoleRequest(Label label) {
        return TxRequest.newBuilder().setPutRole(convert(label)).build();
    }

    public static TxRequest putRuleRequest(Label label, Pattern when, Pattern then) {
        PutRule putRule =
                PutRule.newBuilder().setLabel(convert(label)).setWhen(convert(when)).setThen(convert(then)).build();

        return TxRequest.newBuilder().setPutRule(putRule).build();
    }

    public static TxResponse doneResponse() {
        return TxResponse.newBuilder().setDone(Done.getDefaultInstance()).build();
    }

    public static TxResponse iteratorResponse(IteratorId iteratorId) {
        return TxResponse.newBuilder().setIteratorId(iteratorId).build();
    }

    public static TxResponse conceptResponse(Concept concept) {
        return TxResponse.newBuilder().setConcept(convert(concept)).build();
    }

    public static TxResponse optionalConceptResponse(Optional<Concept> concept) {
        return TxResponse.newBuilder().setOptionalConcept(convertOptionalConcept(concept)).build();
    }

    public static TxResponse rolePlayerResponse(RolePlayer rolePlayer) {
        return TxResponse.newBuilder().setRolePlayer(convert(rolePlayer)).build();
    }

    public static DeleteRequest deleteRequest(Open open) {
        return DeleteRequest.newBuilder().setOpen(open).build();
    }

    public static DeleteResponse deleteResponse() {
        return DeleteResponse.getDefaultInstance();
    }

    public static Keyspace getKeyspace(Open open) {
        return convert(open.getKeyspace());
    }

    public static GraknTxType getTxType(Open open) {
        return convert(open.getTxType());
    }

    public static ConceptId getConceptId(RunConceptMethod runConceptMethodRequest) {
        return convert(runConceptMethodRequest.getId());
    }

    public static GrpcConcept.Concept convert(Concept concept) {
        return GrpcConcept.Concept.newBuilder()
                .setId(GrpcConcept.ConceptId.newBuilder().setValue(concept.getId().getValue()).build())
                .setBaseType(getBaseType(concept))
                .build();
    }

    public static OptionalConcept convertOptionalConcept(Optional<Concept> concept) {
        OptionalConcept.Builder builder = OptionalConcept.newBuilder();
        return concept.map(GrpcUtil::convert).map(builder::setPresent).orElseGet(() -> builder.setAbsent(UNIT)).build();
    }

    public static Concepts convert(Stream<? extends Concept> concepts) {
        Concepts.Builder grpcConcepts = Concepts.newBuilder();
        grpcConcepts.addAllConcept(concepts.map(GrpcUtil::convert).collect(toList()));
        return grpcConcepts.build();
    }

    public static Stream<? extends Concept> convert(GrpcConceptConverter conceptConverter, Concepts concepts) {
        return concepts.getConceptList().stream().map(conceptConverter::convert);
    }

    public static GraknTxType convert(TxType txType) {
        switch (txType) {
            case Read:
                return GraknTxType.READ;
            case Write:
                return GraknTxType.WRITE;
            case Batch:
                return GraknTxType.BATCH;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + txType);
        }
    }

    private static TxType convert(GraknTxType txType) {
        switch (txType) {
            case READ:
                return TxType.Read;
            case WRITE:
                return TxType.Write;
            case BATCH:
                return TxType.Batch;
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + txType);
        }
    }

    private static Keyspace convert(GrpcGrakn.Keyspace keyspace) {
        return Keyspace.of(keyspace.getValue());
    }

    public static GrpcGrakn.Keyspace convert(Keyspace keyspace) {
        return GrpcGrakn.Keyspace.newBuilder().setValue(keyspace.getValue()).build();
    }

    private static GrpcConcept.ConceptId convert(ConceptId id) {
        return GrpcConcept.ConceptId.newBuilder().setValue(id.getValue()).build();
    }

    public static ConceptId convert(GrpcConcept.ConceptId id) {
        return ConceptId.of(id.getValue());
    }

    static GrpcConcept.Label convert(Label label) {
        return GrpcConcept.Label.newBuilder().setValue(label.getValue()).build();
    }

    public static Label convert(GrpcConcept.Label label) {
        return Label.of(label.getValue());
    }

    public static Object convert(AttributeValue value) {
        switch (value.getValueCase()) {
            case STRING:
                return value.getString();
            case BOOLEAN:
                return value.getBoolean();
            case INTEGER:
                return value.getInteger();
            case LONG:
                return value.getLong();
            case FLOAT:
                return value.getFloat();
            case DOUBLE:
                return value.getDouble();
            case DATE:
                return value.getDate();
            default:
            case VALUE_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + value);
        }
    }

    static AttributeValue convertValue(Object value) {
        AttributeValue.Builder builder = AttributeValue.newBuilder();
        if (value instanceof String) {
            builder.setString((String) value);
        } else if (value instanceof Boolean) {
            builder.setBoolean((boolean) value);
        } else if (value instanceof Integer) {
            builder.setInteger((int) value);
        } else if (value instanceof Long) {
            builder.setLong((long) value);
        } else if (value instanceof Float) {
            builder.setFloat((float) value);
        } else if (value instanceof Double) {
            builder.setDouble((double) value);
        } else if (value instanceof LocalDateTime) {
            builder.setDate(((LocalDateTime) value).atZone(ZoneId.of("Z")).toInstant().toEpochMilli());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + value);
        }

        return builder.build();
    }

    public static AttributeType.DataType<?> convert(GrpcConcept.DataType dataType) {
        switch (dataType) {
            case String:
                return AttributeType.DataType.STRING;
            case Boolean:
                return AttributeType.DataType.BOOLEAN;
            case Integer:
                return AttributeType.DataType.INTEGER;
            case Long:
                return AttributeType.DataType.LONG;
            case Float:
                return AttributeType.DataType.FLOAT;
            case Double:
                return AttributeType.DataType.DOUBLE;
            case Date:
                return AttributeType.DataType.DATE;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + dataType);
        }
    }

    public static GrpcConcept.DataType convert(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return GrpcConcept.DataType.String;
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return GrpcConcept.DataType.Boolean;
        } else if (dataType.equals(AttributeType.DataType.INTEGER)) {
            return GrpcConcept.DataType.Integer;
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return GrpcConcept.DataType.Long;
        } else if (dataType.equals(AttributeType.DataType.FLOAT)) {
            return GrpcConcept.DataType.Float;
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return GrpcConcept.DataType.Double;
        } else if (dataType.equals(AttributeType.DataType.DATE)) {
            return GrpcConcept.DataType.Date;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised " + dataType);
        }
    }

    public static OptionalDataType convertOptionalDataType(Optional<AttributeType.DataType<?>> dataType) {
        OptionalDataType.Builder builder = OptionalDataType.newBuilder();
        return dataType.map(GrpcUtil::convert).map(builder::setPresent).orElseGet(() -> builder.setAbsent(UNIT)).build();
    }

    public static Optional<AttributeType.DataType<?>> convert(OptionalDataType dataType) {
        switch (dataType.getValueCase()) {
            case PRESENT:
                return Optional.of(convert(dataType.getPresent()));
            case ABSENT:
            case VALUE_NOT_SET:
            default:
                return Optional.empty();
        }
    }

    public static GrpcConcept.BaseType getBaseType(Concept concept) {
        if (concept.isEntityType()) {
            return GrpcConcept.BaseType.EntityType;
        } else if (concept.isRelationshipType()) {
            return GrpcConcept.BaseType.RelationshipType;
        } else if (concept.isAttributeType()) {
            return GrpcConcept.BaseType.AttributeType;
        } else if (concept.isEntity()) {
            return GrpcConcept.BaseType.Entity;
        } else if (concept.isRelationship()) {
            return GrpcConcept.BaseType.Relationship;
        } else if (concept.isAttribute()) {
            return GrpcConcept.BaseType.Attribute;
        } else if (concept.isRole()) {
            return GrpcConcept.BaseType.Role;
        } else if (concept.isRule()) {
            return GrpcConcept.BaseType.Rule;
        } else if (concept.isType()) {
            return GrpcConcept.BaseType.MetaType;
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised concept " + concept);
        }
    }

    public static GrpcConcept.Pattern convert(Pattern pattern) {
        return GrpcConcept.Pattern.newBuilder().setValue(pattern.toString()).build();
    }

    public static Pattern convert(GrpcConcept.Pattern pattern ) {
        return Graql.parser().parsePattern(pattern.getValue());
    }

    public static OptionalPattern convert(Optional<Pattern> pattern) {
        OptionalPattern.Builder builder = OptionalPattern.newBuilder();
        return pattern.map(GrpcUtil::convert).map(builder::setPresent).orElseGet(() -> builder.setAbsent(UNIT)).build();
    }

    @Nullable
    public static Optional<Pattern> convert(OptionalPattern pattern) {
        switch (pattern.getValueCase()) {
            case PRESENT:
                return Optional.of(convert(pattern.getPresent()));
            case ABSENT:
                return Optional.empty();
            case VALUE_NOT_SET:
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + pattern);
        }
    }

    public static Map<Role, Set<Thing>> convert(GrpcConceptConverter converter, RolePlayers allRolePlayers) {
        ImmutableSetMultimap.Builder<Role, Thing> map = ImmutableSetMultimap.builder();

        for (GrpcConcept.RolePlayer rolePlayer : allRolePlayers.getRolePlayerList()) {
            Role role = converter.convert(rolePlayer.getRole()).asRole();
            Thing player = converter.convert(rolePlayer.getPlayer()).asThing();
            map.put(role, player);
        }

        return Multimaps.asMap(map.build());
    }

    public static RolePlayers convert(Map<Role, Set<Thing>> rolePlayers) {
        RolePlayers.Builder builder = RolePlayers.newBuilder();

        rolePlayers.forEach((role, players) -> {
            players.forEach(player -> {
                builder.addRolePlayer(convert(RolePlayer.create(role, player)));
            });
        });

        return builder.build();
    }

    public static GrpcConcept.RolePlayer convert(RolePlayer rolePlayer) {
        return GrpcConcept.RolePlayer.newBuilder()
                .setRole(convert(rolePlayer.role()))
                .setPlayer(convert(rolePlayer.player()))
                .build();
    }

    public static OptionalRegex convertRegex(Optional<String> regex) {
        OptionalRegex.Builder builder = OptionalRegex.newBuilder();
        return regex.map(builder::setPresent).orElseGet(() -> builder.setAbsent(UNIT)).build();
    }

    public static Optional<String> convert(OptionalRegex regex) {
        switch (regex.getValueCase()) {
            case PRESENT:
                return Optional.of(regex.getPresent());
            case ABSENT:
            case VALUE_NOT_SET:
            default:
                return Optional.empty();
        }
    }
}
