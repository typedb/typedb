/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.parameters;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

// TODO: Apply this class as a replacement of all "label" and "scope" Strings
//       used throughout the codebase. You should replace all occurrences of
//       String label, @Nullable String scope
public class Label {

    private final String name;
    private final String scope;
    private int hash = 0;

    /**
     * TODO:
     *
     * Note that in TypeQL, you declare "scoped labels/types" by writing the scope first: var("x").type("marriage", "husband") or var("x").plays("marriage", "husband").
     *
     * Meanwhile, in TypeDB, you declare "scoped labels/types" by writing the label first: graph.getType(label, scope), and Label.of(label, scope).
     *
     * This is is not good and needs to be fixed so we don't get tripped up (which we have).
     *
     * I think TypeQL wins, as it is closer to "native TypeQL", and therefore would be most intuitive for our users. E.g.
     * person sub entity, plays marriage:husband; is symmetrical to
     * type("person").sub("entity").plays("marriage", "husband");
     *
     * @param name
     * @param scope
     */
    private Label(String name, @Nullable String scope) {
        this.name = name;
        this.scope = scope;
    }

    public static Label of(String name) {
        return new Label(name, null);
    }

    public static Label of(String name, String scope) {
        return new Label(name, scope);
    }

    public String name() {
        return name;
    }

    public Optional<String> scope() {
        return Optional.ofNullable(scope);
    }

    public String scopedName() {
        if (scope == null) return name;
        else return scope + ":" + name;
    }

    @Override
    public String toString() {
        return scopedName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Label that = (Label) o;
        return this.name.equals(that.name) && Objects.equals(this.scope, that.scope);
    }

    @Override
    public int hashCode() {
        if (hash == 0) hash = Objects.hash(name, scope);
        return hash;
    }
}
