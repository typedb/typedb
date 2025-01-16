/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, fmt::Formatter};

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

impl<T: TypeDBError> TypeDBError for Box<T> {
    fn variant_name(&self) -> &'static str {
        (**self).variant_name()
    }

    fn component(&self) -> &'static str {
        (**self).component()
    }

    fn code(&self) -> &'static str {
        (**self).code()
    }

    fn code_prefix(&self) -> &'static str {
        (**self).code_prefix()
    }

    fn code_number(&self) -> usize {
        (**self).code_number()
    }

    fn format_description(&self) -> String {
        (**self).format_description()
    }

    fn source(&self) -> Option<&(dyn Error + Sync)> {
        (**self).source()
    }

    fn source_typedb_error(&self) -> Option<&(dyn TypeDBError + Sync)> {
        (**self).source_typedb_error()
    }
}

// ***USAGE WARNING***: We should not set both Source and TypeDBSource, TypeDBSource has precedence!
#[macro_export]
macro_rules! typedb_error {
    ($vis:vis $name:ident(component = $component:literal, prefix = $prefix:literal) { $(
        $variant:ident($number:literal, $description:literal $(, $($arg:tt)*)?),
    )*}) => {
        #[derive(Clone)]
        $vis enum $name {
            $($variant { $($($arg)*)? }),*
        }

        const _: () = {
            // fail to compile if any Numbers are the same
            trait Assert {}
            $(impl Assert for [(); $number ] {})*
        };

        impl $crate::TypeDBError for $name {
            fn variant_name(&self) -> &'static str {
                match self {
                    $(Self::$variant { .. } => stringify!($variant),)*
                }
            }

            fn component(&self) -> &'static str {
                &$component
            }

            fn code(&self) -> &'static str {
                match self {
                    $(Self::$variant { .. } => concat!($prefix, stringify!($number)),)*
                }
            }

            fn code_prefix(&self) -> &'static str {
                $prefix
            }

            fn code_number(&self) -> usize {
                match self {
                    $(Self::$variant { .. } => $number,)*
                }
            }

            fn format_description(&self) -> String {
                match self {
                    $(typedb_error!(@args $variant { $($($arg)*)? }) => format!($description),)*
                }
            }

            fn source(&self) -> Option<&(dyn ::std::error::Error + Sync + 'static)> {
                match self {
                    $(typedb_error!(@source source from $variant { $($($arg)*)? })=> {
                        typedb_error!(@source source { $($($arg)*)? })
                    })*
                }
            }

            fn source_typedb_error(&self) -> Option<&(dyn $crate::TypeDBError + Sync + 'static)> {
                match self {
                    $(typedb_error!(@typedb_source typedb_source from $variant { $($($arg)*)? })=> {
                        typedb_error!(@typedb_source typedb_source { $($($arg)*)? })
                    })*
                }
            }
        }

        impl ::std::fmt::Debug for $name {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                ::std::fmt::Debug::fmt(self as &dyn $crate::TypeDBError, f)
            }
        }
    };

    (@args $variant:ident { $($arg:ident : $ty:ty),* $(,)? }) => {
        Self::$variant { $($arg),* }
    };

    (@source $ts:ident from $variant:ident { source : $argty:ty $(, $($rest:tt)*)? }) => {
        Self::$variant { source: $ts, .. }
    };
    (@source $ts:ident from $variant:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source $ts from $variant { $($($rest)*)? })
    };
    (@source $ts:ident from $variant:ident { $(,)? }) => {
        Self::$variant { .. }
    };

    (@source $ts:ident { source: $_:ty $(, $($rest:tt)*)? }) => {
        Some($ts as &(dyn ::std::error::Error + Sync + 'static))
    };
    (@source $ts:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@source $ts { $($($rest)*)? })
    };
    (@source $ts:ident { $(,)? }) => {
        None
    };

    (@typedb_source $ts:ident from $variant:ident { typedb_source : $argty:ty $(, $($rest:tt)*)? }) => {
        Self::$variant { typedb_source: $ts, .. }
    };
    (@typedb_source $ts:ident from $variant:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@typedb_source $ts from $variant { $($($rest)*)? })
    };
    (@typedb_source $ts:ident from $variant:ident { $(,)? }) => {
        Self::$variant { .. }
    };

    (@typedb_source $ts:ident { typedb_source: $_:ty $(, $($rest:tt)*)? }) => {
        Some($ts as &(dyn $crate::TypeDBError + Sync + 'static))
    };
    (@typedb_source $ts:ident { $arg:ident : $argty:ty $(, $($rest:tt)*)? }) => {
        typedb_error!(@typedb_source $ts { $($($rest)*)? })
    };
    (@typedb_source $ts:ident { $(,)? }) => {
        None
    };
}

// Check for usages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnimplementedFeature {
    Optionals,
    Lists,
    Structs,

    BuiltinFunction(String),
    LetInBuiltinCall,

    ComparatorContains,
    ComparatorLike,
    UnsortedJoin,

    PipelineStageInFunction(&'static str),

    IrrelevantUnboundInvertedMode(&'static str),
}
impl std::fmt::Display for UnimplementedFeature {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[macro_export]
macro_rules! unimplemented_feature {
    ($feature:ident) => {
        unreachable!("TODO: Implement feature: {:?}", error::UnimplementedFeature::$feature)
    };
    ($feature:ident, $msg:literal) => {
        unreachable!("TODO: Implement feature: {:?} {}", error::UnimplementedFeature::$feature, $msg)
    };
}

#[macro_export]
macro_rules! todo_must_implement {
    ($msg:literal) => {
        // todo!(concat!("TODO: Must implement: ", $msg)) // Ensure the below is enabled when checking in.
        compile_error!(concat!("TODO: Must implement: ", $msg)) // Ensure this is enabled when checking in.
    };
}

#[macro_export]
macro_rules! todo_display_for_error {
    ($f:ident) => {
        write!($f, "fmt::Display has not yet been implemented for {}.", std::any::type_name::<Self>())
    };
}

#[macro_export]
macro_rules! ensure_unimplemented_unused {
    () => {
        compile_error!("Implement this path before making the function usable")
    };
}
