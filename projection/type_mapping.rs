/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::value::value_type::ValueTypeCategory;

/// Postgres type OID — the 4-byte identifier Postgres uses in RowDescription messages.
pub type PgOid = u32;

// ── Postgres OID constants (from pg_type) ──────────────────────────

// Fundamental types
pub const PG_OID_BOOL: PgOid = 16;
pub const PG_OID_BYTEA: PgOid = 17;
pub const PG_OID_INT8: PgOid = 20;
pub const PG_OID_INT2: PgOid = 21;
pub const PG_OID_INT4: PgOid = 23;
pub const PG_OID_TEXT: PgOid = 25;
pub const PG_OID_OID: PgOid = 26;

// JSON / XML
pub const PG_OID_JSON: PgOid = 114;
pub const PG_OID_XML: PgOid = 142;

// Geometric types
pub const PG_OID_POINT: PgOid = 600;
pub const PG_OID_LSEG: PgOid = 601;
pub const PG_OID_PATH: PgOid = 602;
pub const PG_OID_BOX: PgOid = 603;
pub const PG_OID_POLYGON: PgOid = 604;
pub const PG_OID_LINE: PgOid = 628;

// Network types
pub const PG_OID_CIDR: PgOid = 650;
pub const PG_OID_FLOAT4: PgOid = 700;
pub const PG_OID_FLOAT8: PgOid = 701;
pub const PG_OID_CIRCLE: PgOid = 718;
pub const PG_OID_MACADDR8: PgOid = 774;
pub const PG_OID_MONEY: PgOid = 790;
pub const PG_OID_MACADDR: PgOid = 829;
pub const PG_OID_INET: PgOid = 869;

// Character types
pub const PG_OID_CHAR: PgOid = 1042;
pub const PG_OID_VARCHAR: PgOid = 1043;

// Date/time types
pub const PG_OID_DATE: PgOid = 1082;
pub const PG_OID_TIME: PgOid = 1083;
pub const PG_OID_TIMESTAMP: PgOid = 1114;
pub const PG_OID_TIMESTAMPTZ: PgOid = 1184;
pub const PG_OID_INTERVAL: PgOid = 1186;
pub const PG_OID_TIMETZ: PgOid = 1266;

// Bit string types
pub const PG_OID_BIT: PgOid = 1560;
pub const PG_OID_VARBIT: PgOid = 1562;

// Numeric
pub const PG_OID_NUMERIC: PgOid = 1700;

// UUID
pub const PG_OID_UUID: PgOid = 2950;

// Text search
pub const PG_OID_TSVECTOR: PgOid = 3614;
pub const PG_OID_TSQUERY: PgOid = 3615;

// JSONB
pub const PG_OID_JSONB: PgOid = 3802;

// Replication / snapshots
pub const PG_OID_PG_LSN: PgOid = 3220;
pub const PG_OID_PG_SNAPSHOT: PgOid = 5038;

// ── TypeDB → Postgres mapping ──────────────────────────────────────

/// Maps a TypeDB `ValueTypeCategory` to the corresponding Postgres type OID.
/// This covers the 10 value types that TypeDB can produce.
pub fn value_type_to_pg_oid(category: ValueTypeCategory) -> PgOid {
    match category {
        ValueTypeCategory::Boolean => PG_OID_BOOL,
        ValueTypeCategory::Integer => PG_OID_INT8,
        ValueTypeCategory::Double => PG_OID_FLOAT8,
        ValueTypeCategory::Decimal => PG_OID_NUMERIC,
        ValueTypeCategory::Date => PG_OID_DATE,
        ValueTypeCategory::DateTime => PG_OID_TIMESTAMP,
        ValueTypeCategory::DateTimeTZ => PG_OID_TIMESTAMPTZ,
        ValueTypeCategory::Duration => PG_OID_INTERVAL,
        ValueTypeCategory::String => PG_OID_TEXT,
        ValueTypeCategory::Struct => PG_OID_JSONB,
    }
}

// ── Full Postgres catalog lookups ──────────────────────────────────

/// Returns the Postgres type name string for a given OID.
/// Covers all standard Postgres data types from pg_type.
pub fn pg_oid_to_type_name(oid: PgOid) -> Option<&'static str> {
    match oid {
        PG_OID_BOOL => Some("bool"),
        PG_OID_BYTEA => Some("bytea"),
        PG_OID_INT8 => Some("int8"),
        PG_OID_INT2 => Some("int2"),
        PG_OID_INT4 => Some("int4"),
        PG_OID_TEXT => Some("text"),
        PG_OID_OID => Some("oid"),
        PG_OID_JSON => Some("json"),
        PG_OID_XML => Some("xml"),
        PG_OID_POINT => Some("point"),
        PG_OID_LSEG => Some("lseg"),
        PG_OID_PATH => Some("path"),
        PG_OID_BOX => Some("box"),
        PG_OID_POLYGON => Some("polygon"),
        PG_OID_LINE => Some("line"),
        PG_OID_CIDR => Some("cidr"),
        PG_OID_FLOAT4 => Some("float4"),
        PG_OID_FLOAT8 => Some("float8"),
        PG_OID_CIRCLE => Some("circle"),
        PG_OID_MACADDR8 => Some("macaddr8"),
        PG_OID_MONEY => Some("money"),
        PG_OID_MACADDR => Some("macaddr"),
        PG_OID_INET => Some("inet"),
        PG_OID_CHAR => Some("char"),
        PG_OID_VARCHAR => Some("varchar"),
        PG_OID_DATE => Some("date"),
        PG_OID_TIME => Some("time"),
        PG_OID_TIMESTAMP => Some("timestamp"),
        PG_OID_TIMESTAMPTZ => Some("timestamptz"),
        PG_OID_INTERVAL => Some("interval"),
        PG_OID_TIMETZ => Some("timetz"),
        PG_OID_BIT => Some("bit"),
        PG_OID_VARBIT => Some("varbit"),
        PG_OID_NUMERIC => Some("numeric"),
        PG_OID_UUID => Some("uuid"),
        PG_OID_TSVECTOR => Some("tsvector"),
        PG_OID_TSQUERY => Some("tsquery"),
        PG_OID_JSONB => Some("jsonb"),
        PG_OID_PG_LSN => Some("pg_lsn"),
        PG_OID_PG_SNAPSHOT => Some("pg_snapshot"),
        _ => None,
    }
}

/// Returns the Postgres type size (in bytes) for a given OID.
/// Returns -1 for variable-length types. Matches `pg_type.typlen`.
pub fn pg_oid_type_size(oid: PgOid) -> Option<i16> {
    match oid {
        // Fixed-size types
        PG_OID_BOOL => Some(1),
        PG_OID_INT2 => Some(2),
        PG_OID_INT4 | PG_OID_OID | PG_OID_DATE => Some(4),
        PG_OID_MACADDR => Some(6),
        PG_OID_INT8 | PG_OID_FLOAT8 | PG_OID_MONEY | PG_OID_TIME | PG_OID_TIMESTAMP | PG_OID_TIMESTAMPTZ
        | PG_OID_PG_LSN => Some(8),
        PG_OID_MACADDR8 | PG_OID_FLOAT4 => {
            // macaddr8 = 8 bytes (EUI-64), float4 = 4 bytes
            match oid {
                PG_OID_MACADDR8 => Some(8),
                PG_OID_FLOAT4 => Some(4),
                _ => unreachable!(),
            }
        }
        PG_OID_TIMETZ => Some(12),
        PG_OID_INTERVAL | PG_OID_POINT | PG_OID_UUID => Some(16),
        PG_OID_LINE | PG_OID_CIRCLE => Some(24),
        PG_OID_LSEG | PG_OID_BOX => Some(32),
        // Variable-length types
        PG_OID_BYTEA | PG_OID_TEXT | PG_OID_JSON | PG_OID_XML | PG_OID_PATH | PG_OID_POLYGON | PG_OID_CIDR
        | PG_OID_INET | PG_OID_CHAR | PG_OID_VARCHAR | PG_OID_BIT | PG_OID_VARBIT | PG_OID_NUMERIC
        | PG_OID_TSVECTOR | PG_OID_TSQUERY | PG_OID_JSONB | PG_OID_PG_SNAPSHOT => Some(-1),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use encoding::value::value_type::ValueTypeCategory;

    use super::*;

    // ── TypeDB → Postgres OID mapping ──────────────────────────

    #[test]
    fn boolean_maps_to_pg_bool() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Boolean), 16);
    }

    #[test]
    fn integer_maps_to_pg_int8() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Integer), 20);
    }

    #[test]
    fn double_maps_to_pg_float8() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Double), 701);
    }

    #[test]
    fn decimal_maps_to_pg_numeric() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Decimal), 1700);
    }

    #[test]
    fn date_maps_to_pg_date() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Date), 1082);
    }

    #[test]
    fn datetime_maps_to_pg_timestamp() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::DateTime), 1114);
    }

    #[test]
    fn datetime_tz_maps_to_pg_timestamptz() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::DateTimeTZ), 1184);
    }

    #[test]
    fn duration_maps_to_pg_interval() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Duration), 1186);
    }

    #[test]
    fn string_maps_to_pg_text() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::String), 25);
    }

    #[test]
    fn struct_maps_to_pg_jsonb() {
        assert_eq!(value_type_to_pg_oid(ValueTypeCategory::Struct), 3802);
    }

    // ── type names: TypeDB-mapped types ────────────────────────

    #[test]
    fn pg_bool_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(16), Some("bool"));
    }

    #[test]
    fn pg_int8_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(20), Some("int8"));
    }

    #[test]
    fn pg_float8_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(701), Some("float8"));
    }

    #[test]
    fn pg_numeric_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(1700), Some("numeric"));
    }

    #[test]
    fn pg_text_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(25), Some("text"));
    }

    #[test]
    fn pg_jsonb_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(3802), Some("jsonb"));
    }

    // ── type names: additional standard Postgres types ─────────

    #[test]
    fn pg_bytea_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_BYTEA), Some("bytea"));
    }

    #[test]
    fn pg_int2_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_INT2), Some("int2"));
    }

    #[test]
    fn pg_int4_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_INT4), Some("int4"));
    }

    #[test]
    fn pg_json_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_JSON), Some("json"));
    }

    #[test]
    fn pg_xml_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_XML), Some("xml"));
    }

    #[test]
    fn pg_float4_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_FLOAT4), Some("float4"));
    }

    #[test]
    fn pg_uuid_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_UUID), Some("uuid"));
    }

    #[test]
    fn pg_varchar_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_VARCHAR), Some("varchar"));
    }

    #[test]
    fn pg_char_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_CHAR), Some("char"));
    }

    #[test]
    fn pg_money_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_MONEY), Some("money"));
    }

    #[test]
    fn pg_inet_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_INET), Some("inet"));
    }

    #[test]
    fn pg_cidr_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_CIDR), Some("cidr"));
    }

    #[test]
    fn pg_macaddr_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_MACADDR), Some("macaddr"));
    }

    #[test]
    fn pg_macaddr8_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_MACADDR8), Some("macaddr8"));
    }

    #[test]
    fn pg_time_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_TIME), Some("time"));
    }

    #[test]
    fn pg_timetz_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_TIMETZ), Some("timetz"));
    }

    #[test]
    fn pg_bit_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_BIT), Some("bit"));
    }

    #[test]
    fn pg_varbit_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_VARBIT), Some("varbit"));
    }

    #[test]
    fn pg_point_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_POINT), Some("point"));
    }

    #[test]
    fn pg_line_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_LINE), Some("line"));
    }

    #[test]
    fn pg_lseg_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_LSEG), Some("lseg"));
    }

    #[test]
    fn pg_box_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_BOX), Some("box"));
    }

    #[test]
    fn pg_path_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_PATH), Some("path"));
    }

    #[test]
    fn pg_polygon_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_POLYGON), Some("polygon"));
    }

    #[test]
    fn pg_circle_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_CIRCLE), Some("circle"));
    }

    #[test]
    fn pg_tsvector_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_TSVECTOR), Some("tsvector"));
    }

    #[test]
    fn pg_tsquery_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_TSQUERY), Some("tsquery"));
    }

    #[test]
    fn pg_pg_lsn_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_PG_LSN), Some("pg_lsn"));
    }

    #[test]
    fn pg_pg_snapshot_oid_has_correct_name() {
        assert_eq!(pg_oid_to_type_name(PG_OID_PG_SNAPSHOT), Some("pg_snapshot"));
    }

    #[test]
    fn unknown_oid_returns_none() {
        assert_eq!(pg_oid_to_type_name(99999), None);
    }

    // ── type sizes ─────────────────────────────────────────────

    #[test]
    fn pg_bool_size_is_1() {
        assert_eq!(pg_oid_type_size(PG_OID_BOOL), Some(1));
    }

    #[test]
    fn pg_int2_size_is_2() {
        assert_eq!(pg_oid_type_size(PG_OID_INT2), Some(2));
    }

    #[test]
    fn pg_int4_size_is_4() {
        assert_eq!(pg_oid_type_size(PG_OID_INT4), Some(4));
    }

    #[test]
    fn pg_int8_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_INT8), Some(8));
    }

    #[test]
    fn pg_float4_size_is_4() {
        assert_eq!(pg_oid_type_size(PG_OID_FLOAT4), Some(4));
    }

    #[test]
    fn pg_float8_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_FLOAT8), Some(8));
    }

    #[test]
    fn pg_date_size_is_4() {
        assert_eq!(pg_oid_type_size(PG_OID_DATE), Some(4));
    }

    #[test]
    fn pg_time_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_TIME), Some(8));
    }

    #[test]
    fn pg_timestamp_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_TIMESTAMP), Some(8));
    }

    #[test]
    fn pg_timestamptz_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_TIMESTAMPTZ), Some(8));
    }

    #[test]
    fn pg_timetz_size_is_12() {
        assert_eq!(pg_oid_type_size(PG_OID_TIMETZ), Some(12));
    }

    #[test]
    fn pg_interval_size_is_16() {
        assert_eq!(pg_oid_type_size(PG_OID_INTERVAL), Some(16));
    }

    #[test]
    fn pg_uuid_size_is_16() {
        assert_eq!(pg_oid_type_size(PG_OID_UUID), Some(16));
    }

    #[test]
    fn pg_macaddr_size_is_6() {
        assert_eq!(pg_oid_type_size(PG_OID_MACADDR), Some(6));
    }

    #[test]
    fn pg_macaddr8_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_MACADDR8), Some(8));
    }

    #[test]
    fn pg_money_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_MONEY), Some(8));
    }

    #[test]
    fn pg_point_size_is_16() {
        assert_eq!(pg_oid_type_size(PG_OID_POINT), Some(16));
    }

    #[test]
    fn pg_line_size_is_24() {
        assert_eq!(pg_oid_type_size(PG_OID_LINE), Some(24));
    }

    #[test]
    fn pg_lseg_size_is_32() {
        assert_eq!(pg_oid_type_size(PG_OID_LSEG), Some(32));
    }

    #[test]
    fn pg_box_size_is_32() {
        assert_eq!(pg_oid_type_size(PG_OID_BOX), Some(32));
    }

    #[test]
    fn pg_circle_size_is_24() {
        assert_eq!(pg_oid_type_size(PG_OID_CIRCLE), Some(24));
    }

    #[test]
    fn pg_pg_lsn_size_is_8() {
        assert_eq!(pg_oid_type_size(PG_OID_PG_LSN), Some(8));
    }

    // Variable-length types
    #[test]
    fn pg_text_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_TEXT), Some(-1));
    }

    #[test]
    fn pg_numeric_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_NUMERIC), Some(-1));
    }

    #[test]
    fn pg_jsonb_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_JSONB), Some(-1));
    }

    #[test]
    fn pg_json_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_JSON), Some(-1));
    }

    #[test]
    fn pg_xml_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_XML), Some(-1));
    }

    #[test]
    fn pg_bytea_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_BYTEA), Some(-1));
    }

    #[test]
    fn pg_varchar_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_VARCHAR), Some(-1));
    }

    #[test]
    fn pg_inet_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_INET), Some(-1));
    }

    #[test]
    fn pg_path_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_PATH), Some(-1));
    }

    #[test]
    fn pg_polygon_size_is_variable() {
        assert_eq!(pg_oid_type_size(PG_OID_POLYGON), Some(-1));
    }

    #[test]
    fn unknown_oid_size_returns_none() {
        assert_eq!(pg_oid_type_size(99999), None);
    }

    // ── round-trip: every category maps to a valid OID with a name ─

    #[test]
    fn all_categories_have_valid_oid_and_name() {
        let categories = [
            ValueTypeCategory::Boolean,
            ValueTypeCategory::Integer,
            ValueTypeCategory::Double,
            ValueTypeCategory::Decimal,
            ValueTypeCategory::Date,
            ValueTypeCategory::DateTime,
            ValueTypeCategory::DateTimeTZ,
            ValueTypeCategory::Duration,
            ValueTypeCategory::String,
            ValueTypeCategory::Struct,
        ];
        for cat in categories {
            let oid = value_type_to_pg_oid(cat);
            assert!(oid > 0, "OID for {:?} should be positive", cat);
            let name = pg_oid_to_type_name(oid);
            assert!(name.is_some(), "Name for OID {} should exist", oid);
            assert!(!name.unwrap().is_empty(), "Name for OID {} should not be empty", oid);
            let size = pg_oid_type_size(oid);
            assert!(size.is_some(), "Size for OID {} should exist", oid);
        }
    }

    // ── every OID constant has consistent name and size ────────

    #[test]
    fn all_defined_oids_have_name_and_size() {
        let all_oids = [
            PG_OID_BOOL,
            PG_OID_BYTEA,
            PG_OID_INT8,
            PG_OID_INT2,
            PG_OID_INT4,
            PG_OID_TEXT,
            PG_OID_OID,
            PG_OID_JSON,
            PG_OID_XML,
            PG_OID_POINT,
            PG_OID_LSEG,
            PG_OID_PATH,
            PG_OID_BOX,
            PG_OID_POLYGON,
            PG_OID_LINE,
            PG_OID_CIDR,
            PG_OID_FLOAT4,
            PG_OID_FLOAT8,
            PG_OID_CIRCLE,
            PG_OID_MACADDR8,
            PG_OID_MONEY,
            PG_OID_MACADDR,
            PG_OID_INET,
            PG_OID_CHAR,
            PG_OID_VARCHAR,
            PG_OID_DATE,
            PG_OID_TIME,
            PG_OID_TIMESTAMP,
            PG_OID_TIMESTAMPTZ,
            PG_OID_INTERVAL,
            PG_OID_TIMETZ,
            PG_OID_BIT,
            PG_OID_VARBIT,
            PG_OID_NUMERIC,
            PG_OID_UUID,
            PG_OID_TSVECTOR,
            PG_OID_TSQUERY,
            PG_OID_JSONB,
            PG_OID_PG_LSN,
            PG_OID_PG_SNAPSHOT,
        ];
        for oid in all_oids {
            assert!(pg_oid_to_type_name(oid).is_some(), "OID {} should have a name", oid);
            assert!(pg_oid_type_size(oid).is_some(), "OID {} should have a size", oid);
        }
    }
}
