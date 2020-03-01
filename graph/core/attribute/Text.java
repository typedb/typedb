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

package grakn.core.graph.core.attribute;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Comparison relations for text objects. These comparisons are based on a tokenized representation
 * of the text, i.e. the text is considered as a set of word tokens.
 */
public enum Text implements JanusGraphPredicate {

    /**
     * Whether the text contains a given term as a token in the text (case insensitive)
     */
    CONTAINS {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String terms) {
            Set<String> tokens = Sets.newHashSet(tokenize(value.toLowerCase()));
            terms = terms.trim();
            List<String> tokenTerms = tokenize(terms.toLowerCase());
            if (!terms.isEmpty() && tokenTerms.isEmpty()) return false;
            for (String term : tokenTerms) {
                if (!tokens.contains(term)) return false;
            }
            return true;
        }


        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank((String) condition);
        }

        @Override
        public String toString() {
            return "textContains";
        }
    },

    /**
     * Whether the text contains a token that starts with a given term (case insensitive)
     */
    CONTAINS_PREFIX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            for (String token : tokenize(value.toLowerCase())) {
                if (PREFIX.evaluateRaw(token, prefix.toLowerCase())) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String;
        }

        @Override
        public String toString() {
            return "textContainsPrefix";
        }

    },

    /**
     * Whether the text contains a token that matches a regular expression
     */
    CONTAINS_REGEX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String regex) {
            for (String token : tokenize(value.toLowerCase())) {
                if (REGEX.evaluateRaw(token, regex)) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

        @Override
        public String toString() {
            return "textContainsRegex";
        }

    },

    /**
     * Whether the text starts with a given prefix (case sensitive)
     */
    PREFIX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String prefix) {
            return value.startsWith(prefix.trim());
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String;
        }

        @Override
        public String toString() {
            return "textPrefix";
        }

    },

    /**
     * Whether the text matches a regular expression (case sensitive)
     */
    REGEX {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        public boolean evaluateRaw(String value, String regex) {
            return value.matches(regex);
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

        @Override
        public String toString() {
            return "textRegex";
        }

    },

    /**
     * Whether the text is at X Lenvenstein of a token (case sensitive)
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     */
    FUZZY {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String term) {
            return isFuzzy(term.trim(), value.trim());
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

        @Override
        public String toString() {
            return "textFuzzy";
        }

    },

    /**
     * Whether the text contains a token is at X Lenvenstein of a token (case insensitive)
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     */
    CONTAINS_FUZZY {
        @Override
        public boolean test(Object value, Object condition) {
            this.preevaluate(value, condition);
            return value != null && evaluateRaw(value.toString(), (String) condition);
        }

        @Override
        public boolean evaluateRaw(String value, String term) {
            for (String token : tokenize(value.toLowerCase())) {
                if (isFuzzy(term.toLowerCase(), token)) return true;
            }
            return false;
        }

        @Override
        public boolean isValidCondition(Object condition) {
            return condition instanceof String && StringUtils.isNotBlank(condition.toString());
        }

        @Override
        public String toString() {
            return "textContainsFuzzy";
        }

    };

    /**
     * Whether {@code term} is at X Lenvenstein of a {@code value}
     * with X=:
     * - 0 for strings of one or two characters
     * - 1 for strings of three, four or five characters
     * - 2 for strings of more than five characters
     *
     * @return true if {@code term} is similar to {@code value}
     */
    private static boolean isFuzzy(String term, String value) {
        int distance;
        term = term.trim();
        if (term.length() < 3) {
            distance = 0;
        } else if (term.length() < 6) {
            distance = 1;
        } else {
            distance = 2;
        }
//        return LevenshteinDistance.getDefaultInstance().apply(value, term) <= distance;
        return false; //TODO: this is hard-coded so we can avoid an extra dependency, re-enable line above if we ever decide to actually use this mysterious class
    }

    private static final Logger log = LoggerFactory.getLogger(Text.class);

    public void preevaluate(Object value, Object condition) {
        Preconditions.checkArgument(this.isValidCondition(condition), "Invalid condition provided: %s", condition);
        if (!(value instanceof String)) log.debug("Value not a string: " + value);
    }

    abstract boolean evaluateRaw(String value, String condition);

    private static final int MIN_TOKEN_LENGTH = 1;

    public static List<String> tokenize(String str) {
        final ArrayList<String> tokens = new ArrayList<>();
        int previous = 0;
        for (int p = 0; p < str.length(); p++) {
            if (!Character.isLetterOrDigit(str.charAt(p))) {
                if (p > previous + MIN_TOKEN_LENGTH) tokens.add(str.substring(previous, p));
                previous = p + 1;
            }
        }
        if (previous + MIN_TOKEN_LENGTH < str.length()) tokens.add(str.substring(previous));
        return tokens;
    }

    @Override
    public boolean isValidValueType(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);
        return clazz.equals(String.class);
    }

    @Override
    public boolean hasNegation() {
        return false;
    }

    @Override
    public JanusGraphPredicate negate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isQNF() {
        return true;
    }

    //////////////// statics

    public final static Set<Text> HAS_CONTAINS = Collections
            .unmodifiableSet(EnumSet.of(CONTAINS, CONTAINS_PREFIX, CONTAINS_REGEX, CONTAINS_FUZZY));

    public static <V> P<V> textContains(V value) {
        return new P(Text.CONTAINS, value);
    }

    public static <V> P<V> textContainsPrefix(V value) {
        return new P(Text.CONTAINS_PREFIX, value);
    }

    public static <V> P<V> textContainsRegex(V value) {
        return new P(Text.CONTAINS_REGEX, value);
    }

    public static <V> P<V> textPrefix(V value) {
        return new P(Text.PREFIX, value);
    }

    public static <V> P<V> textRegex(V value) {
        return new P(Text.REGEX, value);
    }

    public static <V> P<V> textContainsFuzzy(V value) {
        return new P(Text.CONTAINS_FUZZY, value);
    }

    public static <V> P<V> textFuzzy(V value) {
        return new P(Text.FUZZY, value);
    }
}
