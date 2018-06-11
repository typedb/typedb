package query;

import ai.grakn.GraknTx;
import ai.grakn.graql.Query;

import javax.annotation.Nullable;

public class ConnectiveQuery implements Query {
    public ConnectiveQuery() {
    }

    @Override
    public Query withTx(GraknTx tx) {
        return null;
    }

    @Override
    public Object execute() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Nullable
    @Override
    public GraknTx tx() {
        return null;
    }

    @Override
    public Boolean inferring() {
        return null;
    }
}
