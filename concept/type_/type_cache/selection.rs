/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::type_::{
    type_cache::TypeCache,
    type_cache::kind_cache::{CommonTypeCache, OwnerPlayerCache},
    type_manager::KindAPI,
};

pub(super) trait HasCommonTypeCache<T: KindAPI<'static>> {
    fn common_type_cache(&self) -> &CommonTypeCache<T>;
}

pub trait HasOwnerPlayerCache {
    fn owner_player_cache(&self) -> &OwnerPlayerCache;
}

pub(crate) trait CacheGetter {
    type CacheType;
    fn get_cache<'cache>(type_cache: &'cache TypeCache, type_: Self) -> &'cache Self::CacheType;
}

macro_rules! impl_cache_getter {
    ($cache_type: ty, $inner_type: ident, $member_name: ident) => {
        impl<'a> CacheGetter for $inner_type<'a> {
            type CacheType = $cache_type;
            fn get_cache<'cache>(type_cache: &'cache TypeCache, type_: $inner_type<'a>) -> &'cache Self::CacheType {
                let as_u16 = type_.vertex().type_id_().as_u16();
                type_cache.$member_name[as_u16 as usize].as_ref().unwrap()
            }
        }
    };
}
pub(super) use impl_cache_getter;

macro_rules! impl_has_common_type_cache {
    ($cache_type: ty, $inner_type: ty) => {
        impl HasCommonTypeCache<$inner_type> for $cache_type {
            fn common_type_cache(&self) -> &CommonTypeCache<$inner_type> {
                &self.common_type_cache
            }
        }
    };
}
pub(super) use impl_has_common_type_cache;

macro_rules! impl_has_owner_player_cache {
    ($cache_type: ty, $inner_type: ty) => {
        impl HasOwnerPlayerCache for $cache_type {
            fn owner_player_cache(&self) -> &OwnerPlayerCache {
                &self.owner_player_cache
            }
        }
    };
}
pub(super) use impl_has_owner_player_cache;
