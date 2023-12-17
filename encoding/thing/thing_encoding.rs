/*
 * Copyright (C) 2023 Vaticle
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

use storage::{Section, Storage};

pub struct ThingEncoder {}

impl ThingEncoder {
    pub fn new(storage: &mut Storage) -> ThingEncoder {
        let options = Section::new_options();
        let _ = storage.create_section("entity", 0, &options);
        let _ = storage.create_section("relation", 1, &options);
        let _ = storage.create_section("attribute_small", 10, &options);
        let _ = storage.create_section("has_forward", 100, &options);
        let _ = storage.create_section("has_backward", 101, &options);
        todo!()
    }

    pub fn load(storage: &mut Storage) -> ThingEncoder {
        todo!()
    }
}

pub mod concept {
    use wal::SequenceNumber;
    use crate::type_::type_encoding::concept::TypeID;

    const ID_8_SIZE: usize = 8;
    const ID_24_SIZE: usize = 24;

    pub enum ThingIID {
        Small(ThingIIDSmall),
        Large(ThingIIDLarge),
    }

    struct ThingIIDSmallSequenced {
        iid: ThingIIDSmall,
        sequence_number: SequenceNumber,
    }

    pub struct ThingIIDSmall {
        prefix: u8,
        type_id: TypeID,
        id: ThingID8,
    }

    struct ThingIIDLargeSequenced {
        iid: ThingIIDLarge,
        sequence_number: SequenceNumber,
    }

    pub struct ThingIIDLarge {
        prefix: u8,
        type_id: TypeID,
        id: ThingID24,
    }

    struct ThingID8 {
        bytes: [u8; ID_8_SIZE],
    }

    struct ThingID24 {
        bytes: [u8; ID_24_SIZE],
    }
}

mod connection {

    mod has_small_forward {
        use wal::SequenceNumber;
        use crate::thing::thing_encoding::concept::ThingIIDSmall;

        pub(crate) const NAME: &str = "has_small_forward";

        struct HasSmallForwardIIDSequenced {
            id: HasSmallForwardIID,
            sequence_number: SequenceNumber,
        }

        struct HasSmallForwardIID {
            owner: ThingIIDSmall,
            infix: u8,
            attribute: ThingIIDSmall,
        }
    }

    mod has_small_backward {
        use wal::SequenceNumber;
        use crate::thing::thing_encoding::concept::{ThingIIDSmall};

        pub(crate) const NAME: &str = "has_small_backward";

        struct HasSmallBackwardIIDSequenced {
            id: HasSmallBackwardIID,
            sequence_number: SequenceNumber,
        }

        struct HasSmallBackwardIID {
            owner: ThingIIDSmall,
            infix: u8,
            attribute: ThingIIDSmall,
        }
    }

    mod has_large_forward {
        use wal::SequenceNumber;
        use crate::thing::thing_encoding::concept::{ThingIIDSmall, ThingIIDLarge};

        pub(crate) const NAME: &str = "has_large_forward";

        struct HasLargeForwardIIDSequenced {
            id: HasLargeForwardIID,
            sequence_number: SequenceNumber,
        }

        struct HasLargeForwardIID {
            owner: ThingIIDSmall,
            infix: u8,
            attribute: ThingIIDLarge,
        }
    }

    mod has_large_backward {
        use wal::SequenceNumber;
        use crate::thing::thing_encoding::concept::{ThingIIDLarge, ThingIIDSmall};

        pub(crate) const NAME: &str = "has_large_backward";

        struct HasLargeBackwardIIDSequenced {
            id: HasLargeBackwardIID,
            sequence_number: SequenceNumber,
        }

        struct HasLargeBackwardIID {
            owner: ThingIIDLarge,
            infix: u8,
            attribute: ThingIIDSmall,
        }
    }
}