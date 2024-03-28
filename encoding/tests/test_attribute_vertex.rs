/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#![deny(unused_must_use)]

use std::rc::Rc;

use durability::wal::WAL;
use encoding::{
    graph::{
        thing::vertex_generator::{StringAttributeID, ThingVertexGenerator},
        type_::vertex::TypeID,
    },
    value::string::StringBytes,
    AsBytes, EncodingKeyspace,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::MVCCStorage;
use test_utils::{create_tmp_dir, init_logging};

#[test]
fn generate_string_attribute_vertex() {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>(Rc::from("storage"), &storage_path).unwrap();

    let snapshot = storage.open_snapshot_write();
    let type_id = TypeID::build(0);

    let thing_vertex_generator = ThingVertexGenerator::new();

    // 1: vertex for short string that is stored inline
    {
        let short_string = "Hello";
        let short_string_bytes: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(short_string);
        let vertex =
            thing_vertex_generator.create_attribute_string(type_id, short_string_bytes.clone_as_ref(), &snapshot);
        let vertex_id = StringAttributeID::new(vertex.attribute_id().unwrap_bytes_17());
        assert!(vertex_id.is_inline());
        assert_eq!(vertex_id.get_inline_length() as usize, short_string_bytes.length());
        assert_eq!(vertex_id.get_inline_string_bytes().bytes(), short_string_bytes.bytes());
    }

    // 2: vertex for long string that does not exist beforehand with default hasher
    {
        let string = "Hello world, this is a long attribute string to be encoded.";
        let string_bytes: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(string);
        let vertex = thing_vertex_generator.create_attribute_string(type_id, string_bytes.clone_as_ref(), &snapshot);
        let vertex_id = StringAttributeID::new(vertex.attribute_id().unwrap_bytes_17());
        assert!(!vertex_id.is_inline());
        assert_eq!(
            vertex_id.get_hash_prefix(),
            string_bytes.bytes().bytes()[StringAttributeID::ENCODING_STRING_PREFIX_RANGE]
        );
        assert_eq!(
            vertex_id.get_hash_hash(),
            seahash::hash(string_bytes.bytes().bytes()).to_be_bytes()
                [0..StringAttributeID::ENCODING_STRING_HASH_LENGTH]
        );
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);
    }

    // 3. use a constant hasher to force collisions
    const CONSTANT_HASH: u64 = 0;
    let thing_vertex_generator = ThingVertexGenerator::new_with_hasher(|_bytes| CONSTANT_HASH);

    {
        let string = "Hello world, this is a long attribute string to be encoded with a constant hash.";
        let string_bytes: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(string);
        let vertex = thing_vertex_generator.create_attribute_string(type_id, string_bytes.clone_as_ref(), &snapshot);

        let vertex_id = StringAttributeID::new(vertex.attribute_id().unwrap_bytes_17());
        assert!(!vertex_id.is_inline());
        assert_eq!(
            vertex_id.get_hash_prefix(),
            string_bytes.bytes().bytes()[StringAttributeID::ENCODING_STRING_PREFIX_RANGE]
        );
        assert_eq!(
            vertex_id.get_hash_hash(),
            CONSTANT_HASH.to_be_bytes()[0..StringAttributeID::ENCODING_STRING_HASH_LENGTH]
        );
        assert_eq!(vertex_id.get_hash_disambiguator(), 0u8);

        let string_collide = "Hello world, this is using the same prefix and will collide.";
        let string_collide_bytes: StringBytes<'_, BUFFER_KEY_INLINE> = StringBytes::build_ref(string_collide);
        let collide_vertex =
            thing_vertex_generator.create_attribute_string(type_id, string_collide_bytes.clone_as_ref(), &snapshot);

        let collide_id = StringAttributeID::new(collide_vertex.attribute_id().unwrap_bytes_17());
        assert!(!collide_id.is_inline());
        assert_eq!(
            collide_id.get_hash_prefix(),
            string_collide_bytes.bytes().bytes()[StringAttributeID::ENCODING_STRING_PREFIX_RANGE]
        );
        assert_eq!(
            collide_id.get_hash_hash(),
            CONSTANT_HASH.to_be_bytes()[0..StringAttributeID::ENCODING_STRING_HASH_LENGTH]
        );
        assert_eq!(collide_id.get_hash_disambiguator(), 1u8);
    }
}
