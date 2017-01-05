package ai.grakn.graql.admin;

import java.util.function.Function;

public interface VarName {
    String getValue();

    boolean isUserDefined();

    VarName rename(Function<String, String> mapper);

    String shortName();
}
