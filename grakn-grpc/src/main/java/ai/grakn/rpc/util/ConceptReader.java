package ai.grakn.rpc.util;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Label;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.rpc.GrpcConceptConverter;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.util.CommonUtil;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;

public class ConceptReader {
    public static Stream<? extends Concept> concepts(GrpcConceptConverter conceptConverter, GrpcConcept.Concepts concepts) {
        return concepts.getConceptList().stream().map(conceptConverter::convert);
    }

    public static Label label(GrpcConcept.Label label) {
        return Label.of(label.getValue());
    }

    public static Object attributeValue(GrpcConcept.AttributeValue value) {
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

    public static AttributeType.DataType<?> dataType(GrpcConcept.DataType dataType) {
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

    @Nullable
    public static Optional<Pattern> optionalPattern(GrpcConcept.OptionalPattern pattern) {
        switch (pattern.getValueCase()) {
            case PRESENT:
                return Optional.of(pattern(pattern.getPresent()));
            case ABSENT:
                return Optional.empty();
            case VALUE_NOT_SET:
            default:
                throw CommonUtil.unreachableStatement("Unrecognised " + pattern);
        }
    }

    public static Pattern pattern(GrpcConcept.Pattern pattern ) {
        return Graql.parser().parsePattern(pattern.getValue());
    }

    public static Optional<AttributeType.DataType<?>> optionalDataType(GrpcConcept.OptionalDataType dataType) {
        switch (dataType.getValueCase()) {
            case PRESENT:
                return Optional.of(dataType(dataType.getPresent()));
            case ABSENT:
            case VALUE_NOT_SET:
            default:
                return Optional.empty();
        }
    }

    public static Optional<String> optionalRegex(GrpcConcept.OptionalRegex regex) {
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
