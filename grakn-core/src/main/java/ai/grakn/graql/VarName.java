package ai.grakn.graql;

import java.util.function.Function;

public interface VarName {
    String getValue();

    VarName rename(Function<String, String> mapper);

    String shortName();
}
