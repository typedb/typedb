package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.admin.VarName;
import org.apache.commons.lang.StringUtils;

import java.util.UUID;
import java.util.function.Function;

class VarNameImpl implements VarName {

    private final boolean userDefined;
    private final String value;

    VarNameImpl() {
        this.value = UUID.randomUUID().toString();
        this.userDefined = false;
    }

    VarNameImpl(String value) {
        this.value = value;
        this.userDefined = true;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean isUserDefined() {
        return userDefined;
    }

    @Override
    public VarName rename(Function<String, String> mapper) {
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

        if (userDefined != varName.userDefined) return false;
        return value.equals(varName.value);
    }

    @Override
    public int hashCode() {
        int result = (userDefined ? 1 : 0);
        result = 31 * result + value.hashCode();
        return result;
    }
}
