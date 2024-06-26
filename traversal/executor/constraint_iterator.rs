/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use crate::planner::pattern_plan::SortedIterateMode;
use ir::pattern::constraint::{Comparison, FunctionCallBinding, Has, RolePlayer};

enum ConstraintIteratorProvider {
    Has(HasProvider),
    HasReverse(HasReverseProvider),

    RolePlayer(RolePlayerProvider),
    RolePlayerReverse(RolePlayerReverseProvider),

    // RelationIndex(RelationIndexProvider)
    // RelationIndexReverse(RelationIndexReverseProvider)

    FunctionCallBinding(FunctionCallBindingProvider),

    Comparison(ComparisonProvider),
    ComparisonReverse(ComparisonReverseProvider),
}

impl ConstraintIteratorProvider {

    pub(crate) fn get_iterator(&self) -> ConstraintIterator {
        match self {
            ConstraintIteratorProvider::Has(provider) => provider.get_iterator(),
            ConstraintIteratorProvider::HasReverse(provider) => todo!(),
            ConstraintIteratorProvider::RolePlayer(provider) => todo!(),
            ConstraintIteratorProvider::RolePlayerReverse(provider) => todo!(),
            ConstraintIteratorProvider::FunctionCallBinding(provider) => todo!(),
            ConstraintIteratorProvider::Comparison(provider) => todo!(),
            ConstraintIteratorProvider::ComparisonReverse(provider) => todo!(),
        }
    }
}

struct HasProvider {
    has: Has,
    iterate_mode: SortedIterateMode,
}

impl HasProvider {
    pub(crate) fn get_iterator(&self, row: &Row) -> ConstraintIterator {
        todo!()
    }
}

struct HasReverseProvider {
    has: Has,
    iterate_mode: SortedIterateMode,
}

struct RolePlayerProvider {
    role_player: RolePlayer,
    iterate_mode: SortedIterateMode,
}

struct RolePlayerReverseProvider {
    role_player: RolePlayer,
    iterate_mode: SortedIterateMode,
}

struct FunctionCallBindingProvider {
    function_call_binding: FunctionCallBinding,
}

struct ComparisonProvider {
    comparison: Comparison
}

struct ComparisonReverseProvider {
    comparision: Comparison
}


pub(crate) enum ConstraintIterator {
    Has(),
}
