/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.pattern.constraint.thing;

import grakn.common.collection.Bytes;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.variable.ThingVariable;
import grakn.core.traversal.Traversal;

import java.util.Arrays;
import java.util.Objects;

import static grakn.common.collection.Collections.set;
import static graql.lang.common.GraqlToken.Char.SPACE;
import static graql.lang.common.GraqlToken.Constraint.IID;

public class IIDConstraint extends ThingConstraint {

    private final byte[] iid;
    private final int hash;

    public IIDConstraint(ThingVariable owner, byte[] iid) {
        super(owner, set());
        this.iid = iid;
        this.hash = Objects.hash(IIDConstraint.class, this.owner, Arrays.hashCode(this.iid));
    }

    static IIDConstraint of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.IID constraint) {
        return new IIDConstraint(owner, Bytes.hexStringToBytes(constraint.iid()));
    }

    static IIDConstraint of(ThingVariable owner, IIDConstraint clone) {
        return new IIDConstraint(owner, clone.iid());
    }

    public byte[] iid() {
        return iid;
    }

    @Override
    public void addTo(Traversal traversal) {
        traversal.iid(owner.id(), iid);
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
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IIDConstraint that = (IIDConstraint) o;
        return (this.owner.equals(that.owner) && Arrays.equals(this.iid, that.iid));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + IID + SPACE + Bytes.bytesToHexString(iid);
    }

    @Override
    public IIDConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner).iid(iid);
    }
}
