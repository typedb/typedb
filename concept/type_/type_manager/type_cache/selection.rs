/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::type_::{
    type_manager::type_cache::{
        kind_cache::{CommonTypeCache, ObjectCache},
        TypeCache,
    },
    KindAPI,
};

pub(crate) trait HasCommonTypeCache<T: KindAPI> {
    fn common_type_cache(&self) -> &CommonTypeCache<T>;
}

pub trait HasObjectCache {
    fn object_cache(&self) -> &ObjectCache;
}

pub(crate) trait CacheGetter {
    type CacheType;
    fn get_cache(type_cache: &TypeCache, type_: Self) -> &Self::CacheType;
}

macro_rules! impl_cache_getter {
    ($cache_type: ty, $inner_type: ident, $member_name: ident) => {
        impl CacheGetter for $inner_type {
            type CacheType = $cache_type;
            fn get_cache(type_cache: &TypeCache, type_: $inner_type) -> &Self::CacheType {
                use ::encoding::graph::Typed;
                use encoding::graph::type_::vertex::TypeVertexEncoding;
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

macro_rules! impl_has_object_cache {
    ($cache_type: ty, $inner_type: ty) => {
        impl HasObjectCache for $cache_type {
            fn object_cache(&self) -> &ObjectCache {
                &self.object_cache
            }
        }
    };
}
pub(super) use impl_has_object_cache;
