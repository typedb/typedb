use bytes::Bytes;
use encoding::graph::type_::edge::{EdgeOwnsEncoder, EdgeOwnsReverseEncoder, EdgePlaysEncoder, EdgePlaysReverseEncoder, EdgeRelatesEncoder, EdgeRelatesReverseEncoder, EncodableParametrisedTypeEdge, TypeEdge, TypeEdgeEncoder};
use encoding::graph::type_::property::TypeEdgeProperty;
use encoding::layout::prefix::Prefix;
use storage::key_value::StorageKey;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use crate::type_::{IntoCanonicalTypeEdge, TypeAPI};
use crate::type_::object_type::ObjectType;
use crate::type_::owns::Owns;
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::role_type::RoleType;


