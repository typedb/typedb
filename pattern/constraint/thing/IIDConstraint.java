/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.common.collection.Bytes;
import com.vaticle.typedb.core.common.util.ByteArray;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.traversal.Traversal;

import java.util.Arrays;
import java.util.Objects;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Constraint.IID;

public class IIDConstraint extends ThingConstraint {

    private final ByteArray iid;
    private final int hash;

    public IIDConstraint(ThingVariable owner, ByteArray iid) {
        super(owner, set());
        this.iid = iid;
        this.hash = Objects.hash(IIDConstraint.class, this.owner, this.iid);
    }

    static IIDConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.IID constraint) {
        return new IIDConstraint(owner, Bytes.hexStringToBytes(constraint.iid()));
    }

    static IIDConstraint of(ThingVariable owner, IIDConstraint clone) {
        return new IIDConstraint(owner, clone.iid());
    }

    public ByteArray iid() {
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
