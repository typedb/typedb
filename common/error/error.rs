/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};

pub trait TypeDBError {
    fn variant_name(&self) -> &'static str;

    fn domain(&self) -> &'static str;

    fn code(&self) -> &'static str;

    fn code_prefix(&self) -> &'static str;

    fn code_number(&self) -> usize;

    fn format_description(&self) -> String;

    fn source(&self) -> Option<&dyn Error>;

    fn source_typedb_error(&self) -> Option<&dyn TypeDBError>;

    fn root_source_typedb_error(&self) -> &dyn TypeDBError
    where
        Self: Sized,
    {
        let mut error: &dyn TypeDBError = self;
        while let Some(source) = error.source_typedb_error() {
            error = source;
        }
        error
    }
}

impl PartialEq for dyn TypeDBError {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
}

impl Eq for dyn TypeDBError {}

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

// ***USAGE WARNING***: We should not set both Source and TypeDBSource, TypeDBSource has precedence!
#[macro_export]
macro_rules! typedb_error {
    ( $vis: vis $name:ident(domain = $domain: literal, prefix = $prefix: literal) { $(
        $variant: ident (
            $number: literal,
            $description: literal
            $(, $payload_name: ident : $payload_type: ty )*
            $(, ( source : $source: ty ) )?
            $(, ( typedb_source : $typedb_source: ty ) )?
        ),
    )*}) => {
        $vis enum $name {
            $(
                $variant { $(source: $source, )? $(typedb_source: $typedb_source, )? $($payload_name: $payload_type )* },
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

        impl error::TypeDBError for $name {

            fn variant_name(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant { .. } => &stringify!($variant),
                    )*
                }
            }

            fn domain(&self) -> &'static str {
                &$domain
            }

            fn code(&self) -> &'static str {
                match self {
                    $(
                        Self::$variant { .. } => & concat!($prefix, stringify!($number)),
                    )*
                }
            }

            fn code_prefix(&self) -> &'static str {
                &$prefix
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

            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                let error = match self {
                    $(
                        $( Self::$variant { source, .. } => {
                            let source: &$source = source;
                            Some(source as &dyn std::error::Error)
                        } )?
                    )*
                    _ => None
                };
                error
            }

            fn source_typedb_error(&self) -> Option<&(dyn error::TypeDBError + 'static)> {
                let error = match self {
                    $(
                        $( Self::$variant { typedb_source, .. } => {
                            let typedb_source: &$typedb_source = typedb_source;
                            Some(typedb_source as &dyn error::TypeDBError),
                        } )?
                    )*
                    _ => None
                };
                error
            }
        }
    };
}
