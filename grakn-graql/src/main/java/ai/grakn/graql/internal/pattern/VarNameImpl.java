package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.VarName;
import org.apache.commons.lang.StringUtils;

import java.util.UUID;
import java.util.function.Function;

class VarNameImpl implements VarName {

    private final String value;

    VarNameImpl() {
        this.value = UUID.randomUUID().toString();
    }

    VarNameImpl(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public VarName map(Function<String, String> mapper) {
        return new VarNameImpl(mapper.apply(value));
    }

    @Override
    public String shortName() {
        return "$" + StringUtils.left(value, 3);
    }

    public String toString() {
        return "$" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VarNameImpl varName = (VarNameImpl) o;

        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
