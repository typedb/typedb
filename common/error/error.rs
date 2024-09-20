/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};

mod typeql;

pub trait TypeDBError {
    fn variant_name(&self) -> &'static str;

    fn component(&self) -> &'static str;

    fn code(&self) -> &'static str;

    fn code_prefix(&self) -> &'static str;

    fn code_number(&self) -> usize;

    fn format_description(&self) -> String;

    fn source(&self) -> Option<&(dyn Error + Sync)>;

    fn source_typedb_error(&self) -> Option<&(dyn TypeDBError + Sync)>;

    fn root_source_typedb_error(&self) -> &(dyn TypeDBError + Sync)
    where
        Self: Sized + Sync,
    {
        let mut error: &(dyn TypeDBError + Sync) = self;
        while let Some(source) = error.source_typedb_error() {
            error = source;
        }
        error
    }

    fn format_code_and_description(&self) -> String {
        format!("[{}] {}", self.code(), self.format_description())
    }
}

impl PartialEq for dyn TypeDBError {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
}

impl Eq for dyn TypeDBError {}

impl fmt::Debug for dyn TypeDBError + '_ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for dyn TypeDBError + '_ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = self.source_typedb_error() {
            write!(f, "[{}] {}\nCause: \n\t {:?}", self.code(), self.format_description(), source as &dyn TypeDBError)
        } else if let Some(source) = self.source() {
            write!(f, "[{}] {}\nCause: \n\t {:?}", self.code(), self.format_description(), source)
        } else {
            write!(f, "{}", self.format_code_and_description())
        }
    }
}

// ***USAGE WARNING***: We should not set both Source and TypeDBSource, TypeDBSource has precedence!
#[macro_export]
macro_rules! typedb_error {
    ( $vis: vis $name:ident(component = $component: literal, prefix = $prefix: literal) { $(
        $variant: ident (
            $number: literal,
            $description: literal
            $(, $payload_name: ident : $payload_type: ty )*
            $(, ( source : $source: ty ) )?
            $(, ( typedb_source : $typedb_source: ty ) )?
        ),
    )*}) => {
        #[derive(Clone)]
        $vis enum $name {
            $(
                $variant { $(source: $source, )? $(typedb_source: $typedb_source, )? $($payload_name: $payload_type, )* },
            )*

        }

        impl $name {
            const _VALIDATE_NUMBERS: () = {
                #[deny(unreachable_patterns)] // fail to compile if any Numbers are the same
                match 0 {
                    $(
                        $number => (),
                    )*
                    _ => (),
               }
           };
        }

        impl $crate::TypeDBError for $name {
            fn variant_name(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant { .. } => &stringify!($variant),
                    )*
                }
            }

            fn component(&self) -> &'static str {
                &$component
            }

            fn code(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant { .. } => & concat!($prefix, stringify!($number)),
                    )*
                }
            }

            fn code_prefix(&self) -> &'static str {
                $prefix
            }

            fn code_number(&self) -> usize {
                match self {
                    $(
                        Self::$variant { .. } => $number,
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

            fn source(&self) -> Option<&(dyn ::std::error::Error + Sync + 'static)> {
                let error = match self {
                    $(
                        $(Self::$variant { source, .. } => {
                            let source: &$source = source;
                            Some(source as &(dyn ::std::error::Error + Sync))
                        })?
                    )*
                    _ => None
                };
                error
            }

            fn source_typedb_error(&self) -> Option<&(dyn $crate::TypeDBError + Sync + 'static)> {
                let error = match self {
                    $(
                        $(Self::$variant { typedb_source, .. } => {
                            let typedb_source: &$typedb_source = typedb_source;
                            Some(typedb_source as &(dyn $crate::TypeDBError + Sync))
                        })?
                    )*
                    _ => None
                };
                error
            }
        }

        impl ::std::fmt::Debug for $name {
           fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Debug::fmt(self as &dyn $crate::TypeDBError, f)
            }
        }
    };
}
