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
 *
 */

package grakn.core.query.pattern.constraint.thing;

import grakn.common.collection.Bytes;
import grakn.core.query.pattern.variable.ThingVariable;

import java.util.Arrays;
import java.util.Objects;

public class IIDConstraint extends ThingConstraint {

    private final byte[] iid;
    private final int hash;

    public IIDConstraint(final ThingVariable owner, final byte[] iid) {
        super(owner);
        this.iid = iid;
        this.hash = Objects.hash(IIDConstraint.class, this.owner, Arrays.hashCode(this.iid));
    }

    public static IIDConstraint of(final ThingVariable owner,
                                   final graql.lang.pattern.constraint.ThingConstraint.IID constraint) {
        return new IIDConstraint(owner, Bytes.hexStringToBytes(constraint.iid()));
    }

    public byte[] iid() {
        return iid;
    }

    @Override
    public boolean isIID() {
        return true;
    }

    @Override
    public IIDConstraint asIID() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final IIDConstraint that = (IIDConstraint) o;
        return (this.owner.equals(that.owner) && Arrays.equals(this.iid, that.iid));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
