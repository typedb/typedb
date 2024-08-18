/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub trait TypeDBError {
    fn variant_name(&self) -> &'static str;

    fn domain(&self) -> &'static str;

    fn code(&self) -> &'static str;

    fn prefix(&self) -> &'static str;

    fn code_number(&self) -> usize;

    fn format_description(&self) -> String;

    fn source(&self) -> Option<&dyn Error>;

    fn source_typedb_error(&self) -> Option<&dyn TypeDBError>;

    fn root_source_typedb_error(&self) -> &dyn TypeDBError where Self: Sized {
        let mut error: &dyn TypeDBError = self;
        while let Some(source) = error.source_typedb_error() {
            error = source;
        }
        error
    }
}

impl Debug for dyn TypeDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for dyn TypeDBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.source().is_some() {
            write!(f, "[{}] {}. Cause: \n\t {:?}", self.code(), self.format_description(), self.source().unwrap())
        } else {
            write!(f, "[{}] {}", self.code(), self.format_description())
        }
    }
}

// ***USAGE WARNING***: We should not set both Source and TypeDBSource, TypeDBSource has precedence! This is only checked in runtime assertion
#[macro_export]
macro_rules! typedb_error {
    ( $vis: vis $name:ident(domain = $domain: literal, prefix = $prefix: literal) { $(
        $variant: ident (
            $number: literal,
            $description: literal
            $(, source = $source: ty )?
            $(, typedb_source = $typedb_source: ty )?
            $(, $payload_name: ident = $payload_type: ty )*
        ),
    )*}) => {
        $vis enum $name {
            $(
                $variant { $(source: $source, )? $(typedb_source: $typedb_source, )? $($payload_name: $payload_type, )* },
            )*
        }

        impl TypeDBError for $name {

            fn variant_name(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant { .. } => &stringify!($variant),
                    )*
                }
            }

            fn prefix(&self) -> &'static str {
                & $prefix
            }

            fn code_number(&self) -> usize {
                match self {
                    $(
                        Self::$variant { .. } => $number,
                    )*
                }
            }

            fn code(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant { .. } => & concat!($prefix, stringify!($number)),
                    )*
                }
            }

            fn format_description(&self) -> String {
                match self {
                    $(
                        Self::$variant { $( $payload_name, )* .. } => format!($description),
                    )*
                }
            }

            fn source(&self) -> Option<&dyn Error> {
                let error = match self {
                    $(
                        $( Self::$variant { source, .. } => Some(source as &$source), )?
                    )*,
                    _ => None
                };
                if ::core::cfg!( debug_assertions ) {
                    // if both are set, they must be equal
                    if error.is_some() && self.source_typedb_error().is_some() {
                        ::core::assert!(error == self.source_typedb_error())
                    }
                }
                error
            }

            fn source_typedb_error(&self) -> Option<&dyn TypeDBError> {
                let error = match self {
                    $(
                        $( Self::$variant { typedb_source, .. } => Some(typedb_source as &$typedb_source), )?
                    )*,
                    _ => None
                };
                if ::core::cfg!( debug_assertions ) {
                    // if both are set, they must be equal
                    if error.is_some() && self.source().is_some() {
                        ::core::assert!(error == self.source())
                    }
                }
                error
            }
        }

        // impl Debug for dyn $name {
        //    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        //        Display::fmt(self, f)
        //    }
        // }
        //
        // impl Display for dyn $name {
        //    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        //        if self.source_typedb_error().is_some() {
        //           write!(f, "[{}] {}. Cause: \n\t {}", self.code(), self.description(), self.source_typedb_error().unwrap())
        //        } else if self.source().is_some() {
        //           write!(f, "[{}] {}. Cause: \n\t {:?}", self.code(), self.description(), self.source().unwrap())
        //        } else {
        //           write!(f, "[{}] {}", self.code(), self.description())
        //        }
        //    }
        // }
    };
}
