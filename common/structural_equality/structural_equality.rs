/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    mem::Discriminant,
};

mod typeql_structural_equality;

pub fn is_structurally_equivalent<T: StructuralEquality>(first: &T, second: &T) -> bool {
    let first_hash = first.hash();
    let second_hash = second.hash();
    return first_hash == second_hash && first.equals(second);
}

pub fn ordered_hash_combine(a: u64, b: u64) -> u64 {
    a ^ (b.wrapping_add(0x9e3779b9).wrapping_add(a << 6).wrapping_add(a >> 2))
}

pub trait StructuralEquality {
    // following the java-style hashing
    fn hash(&self) -> u64;

    fn hash_into(&self, hasher: &mut impl Hasher) {
        hasher.write_u64(self.hash())
    }

    fn equals(&self, other: &Self) -> bool;
}

impl StructuralEquality for bool {
    fn hash(&self) -> u64 {
        *self as u64
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl StructuralEquality for u64 {
    fn hash(&self) -> u64 {
        *self
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl StructuralEquality for usize {
    fn hash(&self) -> u64 {
        *self as u64
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl<T: StructuralEquality> StructuralEquality for Vec<T> {
    fn hash(&self) -> u64 {
        (self as &[T]).hash()
    }

    fn equals(&self, other: &Self) -> bool {
        (self as &[T]).equals(other)
    }
}

impl<T: StructuralEquality> StructuralEquality for [T] {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.iter().for_each(|element| element.hash_into(&mut hasher));
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.iter().zip(other.iter()).all(|(a, b)| a.equals(b))
    }
}

impl<T: StructuralEquality + Hash> StructuralEquality for HashSet<T> {
    fn hash(&self) -> u64 {
        self.iter().fold(0, |acc, element| {
            // values may generally be in a small rang, so we run them through a hasher first to make the XOR more effective
            let mut hasher = DefaultHasher::new();
            element.hash_into(&mut hasher);
            // WARNING: must use XOR or other commutative operator!
            acc ^ hasher.finish()
        })
    }

    /// Note: this is a quadratic operation! Best to precede with a Hash check elsewhere.
    fn equals(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter().all(|element| other.iter().any(|other_element| element.equals(other_element)))
    }
}

impl<K: StructuralEquality + Ord, V: StructuralEquality> StructuralEquality for BTreeMap<K, V> {
    fn hash(&self) -> u64 {
        self.iter().fold(0, |acc, (key, value)| {
            let mut hasher = DefaultHasher::new();
            key.hash_into(&mut hasher);
            value.hash_into(&mut hasher);
            // WARNING: must use XOR or other commutative operator!
            acc ^ hasher.finish()
        })
    }

    fn equals(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        self.iter().all(|(key, value)| {
            // TODO: bit strange that we don't use structural equality here, but we do in the hash?
            other.get(key).is_some_and(|other_value| value.equals(other_value))
        })
    }
}

impl<K: StructuralEquality + Hash, V: StructuralEquality> StructuralEquality for HashMap<K, V> {
    fn hash(&self) -> u64 {
        self.iter().fold(0, |acc, (key, value)| {
            // values may generally be in a small rang, so we run them through a hasher first to make the XOR more effective
            let mut hasher = DefaultHasher::new();
            key.hash_into(&mut hasher);
            value.hash_into(&mut hasher);
            // WARNING: must use XOR or other commutative operator!
            acc ^ hasher.finish()
        })
    }

    fn equals(&self, other: &Self) -> bool {
        // Note: this is a quadratic operation! Best to precede with a Hash check elsewhere.
        if self.len() != other.len() {
            return false;
        }

        self.iter().all(|(key, value)| {
            other.iter().any(|(other_key, other_value)| key.equals(other_key) && value.equals(other_value))
        })
    }
}

impl<T: StructuralEquality> StructuralEquality for Option<T> {
    fn hash(&self) -> u64 {
        match self {
            None => 0,
            Some(inner) => inner.hash(),
        }
    }

    fn equals(&self, other: &Self) -> bool {
        match (self, other) {
            (None, None) => true,
            (Some(inner), Some(other_inner)) => inner.equals(other_inner),
            _ => false,
        }
    }
}

// NOTE: specifically not `AsRef<str>` since this may admit too many equalities by accident - we must get &str explicitly first
impl StructuralEquality for str {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        Hash::hash(self, &mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl<T> StructuralEquality for Discriminant<T> {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        Hash::hash(self, &mut hasher);
        hasher.finish()
    }

    fn equals(&self, other: &Self) -> bool {
        self == other
    }
}
