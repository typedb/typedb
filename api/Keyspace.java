package grakn.core.api;

import java.util.regex.Pattern;

public interface Keyspace extends Comparable<Keyspace > {

    int MAX_LENGTH = 48;

    String name();

    static boolean isValidName(String name) {
        return Pattern.matches("[a-z_][a-z_0-9]*", name) && name.length() <= MAX_LENGTH;
    }

    @Override
    default int compareTo(Keyspace o) {
        if (equals(o)) return 0;
        return name().compareTo(o.name());
    }
}
