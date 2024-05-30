/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Tz;

use super::{
    date_time_bytes::DateTimeBytes,
    primitive_encoding::{decode_u16, encode_u16},
};
use crate::graph::thing::vertex_attribute::AttributeIDLength;

#[derive(Debug, Copy, Clone)]
pub struct DateTimeTZBytes {
    bytes: [u8; Self::LENGTH],
}

impl DateTimeTZBytes {
    const LENGTH: usize = AttributeIDLength::Long.length();

    const DATE_TIME_LENGTH: usize = (i64::BITS + u32::BITS) as usize / 8;
    const TZ_LENGTH: usize = u16::BITS as usize / 8;

    pub fn new(bytes: [u8; Self::LENGTH]) -> Self {
        Self { bytes }
    }

    pub fn build(date_time: DateTime<Tz>) -> Self {
        let mut bytes = DateTimeBytes::build(date_time.naive_utc()).bytes();
        bytes[Self::DATE_TIME_LENGTH..][..Self::TZ_LENGTH]
            .copy_from_slice(&encode_u16(encode_tz(date_time.timezone())));
        Self { bytes }
    }

    pub fn as_date_time(&self) -> DateTime<Tz> {
        let date_time = DateTimeBytes::new(self.bytes).as_naive_date_time();
        let tz = decode_tz(decode_u16(self.bytes[Self::DATE_TIME_LENGTH..][..Self::TZ_LENGTH].try_into().unwrap()));
        date_time.and_utc().with_timezone(&tz)
    }

    pub(crate) fn bytes(&self) -> [u8; Self::LENGTH] {
        self.bytes
    }
}

macro_rules! tz_to_number {
    ($($id:literal => $tz:path),+ $(,)?) => {
        const NUM_TZS: u16 = 596;
        const _: () = {
            // NOTE: if this breaks, that means a new timezone has been added to the IANA database.
            // The new timezone(s) MUST be added to the END of the list below in order to preserve the data integrity.
            const _: [u16; { NUM_TZS as usize }] = [$($id),+];
        };
        const fn encode_tz(tz: Tz) -> u16 {
            #[deny(unreachable_patterns)]
            match tz { $($tz => $id,)+ }
        }
        const fn decode_tz(id: u16) -> Tz {
            assert!(id  < NUM_TZS);
            #[deny(unreachable_patterns)]
            match id {
                $($id => $tz,)+
                NUM_TZS.. => unreachable!(),
            }
        }
    };
}

tz_to_number! {
    0 => Tz::Africa__Abidjan,
    1 => Tz::Africa__Accra,
    2 => Tz::Africa__Addis_Ababa,
    3 => Tz::Africa__Algiers,
    4 => Tz::Africa__Asmara,
    5 => Tz::Africa__Asmera,
    6 => Tz::Africa__Bamako,
    7 => Tz::Africa__Bangui,
    8 => Tz::Africa__Banjul,
    9 => Tz::Africa__Bissau,
    10 => Tz::Africa__Blantyre,
    11 => Tz::Africa__Brazzaville,
    12 => Tz::Africa__Bujumbura,
    13 => Tz::Africa__Cairo,
    14 => Tz::Africa__Casablanca,
    15 => Tz::Africa__Ceuta,
    16 => Tz::Africa__Conakry,
    17 => Tz::Africa__Dakar,
    18 => Tz::Africa__Dar_es_Salaam,
    19 => Tz::Africa__Djibouti,
    20 => Tz::Africa__Douala,
    21 => Tz::Africa__El_Aaiun,
    22 => Tz::Africa__Freetown,
    23 => Tz::Africa__Gaborone,
    24 => Tz::Africa__Harare,
    25 => Tz::Africa__Johannesburg,
    26 => Tz::Africa__Juba,
    27 => Tz::Africa__Kampala,
    28 => Tz::Africa__Khartoum,
    29 => Tz::Africa__Kigali,
    30 => Tz::Africa__Kinshasa,
    31 => Tz::Africa__Lagos,
    32 => Tz::Africa__Libreville,
    33 => Tz::Africa__Lome,
    34 => Tz::Africa__Luanda,
    35 => Tz::Africa__Lubumbashi,
    36 => Tz::Africa__Lusaka,
    37 => Tz::Africa__Malabo,
    38 => Tz::Africa__Maputo,
    39 => Tz::Africa__Maseru,
    40 => Tz::Africa__Mbabane,
    41 => Tz::Africa__Mogadishu,
    42 => Tz::Africa__Monrovia,
    43 => Tz::Africa__Nairobi,
    44 => Tz::Africa__Ndjamena,
    45 => Tz::Africa__Niamey,
    46 => Tz::Africa__Nouakchott,
    47 => Tz::Africa__Ouagadougou,
    48 => Tz::Africa__PortoNovo,
    49 => Tz::Africa__Sao_Tome,
    50 => Tz::Africa__Timbuktu,
    51 => Tz::Africa__Tripoli,
    52 => Tz::Africa__Tunis,
    53 => Tz::Africa__Windhoek,
    54 => Tz::America__Adak,
    55 => Tz::America__Anchorage,
    56 => Tz::America__Anguilla,
    57 => Tz::America__Antigua,
    58 => Tz::America__Araguaina,
    59 => Tz::America__Argentina__Buenos_Aires,
    60 => Tz::America__Argentina__Catamarca,
    61 => Tz::America__Argentina__ComodRivadavia,
    62 => Tz::America__Argentina__Cordoba,
    63 => Tz::America__Argentina__Jujuy,
    64 => Tz::America__Argentina__La_Rioja,
    65 => Tz::America__Argentina__Mendoza,
    66 => Tz::America__Argentina__Rio_Gallegos,
    67 => Tz::America__Argentina__Salta,
    68 => Tz::America__Argentina__San_Juan,
    69 => Tz::America__Argentina__San_Luis,
    70 => Tz::America__Argentina__Tucuman,
    71 => Tz::America__Argentina__Ushuaia,
    72 => Tz::America__Aruba,
    73 => Tz::America__Asuncion,
    74 => Tz::America__Atikokan,
    75 => Tz::America__Atka,
    76 => Tz::America__Bahia,
    77 => Tz::America__Bahia_Banderas,
    78 => Tz::America__Barbados,
    79 => Tz::America__Belem,
    80 => Tz::America__Belize,
    81 => Tz::America__BlancSablon,
    82 => Tz::America__Boa_Vista,
    83 => Tz::America__Bogota,
    84 => Tz::America__Boise,
    85 => Tz::America__Buenos_Aires,
    86 => Tz::America__Cambridge_Bay,
    87 => Tz::America__Campo_Grande,
    88 => Tz::America__Cancun,
    89 => Tz::America__Caracas,
    90 => Tz::America__Catamarca,
    91 => Tz::America__Cayenne,
    92 => Tz::America__Cayman,
    93 => Tz::America__Chicago,
    94 => Tz::America__Chihuahua,
    95 => Tz::America__Ciudad_Juarez,
    96 => Tz::America__Coral_Harbour,
    97 => Tz::America__Cordoba,
    98 => Tz::America__Costa_Rica,
    99 => Tz::America__Creston,
    100 => Tz::America__Cuiaba,
    101 => Tz::America__Curacao,
    102 => Tz::America__Danmarkshavn,
    103 => Tz::America__Dawson,
    104 => Tz::America__Dawson_Creek,
    105 => Tz::America__Denver,
    106 => Tz::America__Detroit,
    107 => Tz::America__Dominica,
    108 => Tz::America__Edmonton,
    109 => Tz::America__Eirunepe,
    110 => Tz::America__El_Salvador,
    111 => Tz::America__Ensenada,
    112 => Tz::America__Fort_Nelson,
    113 => Tz::America__Fort_Wayne,
    114 => Tz::America__Fortaleza,
    115 => Tz::America__Glace_Bay,
    116 => Tz::America__Godthab,
    117 => Tz::America__Goose_Bay,
    118 => Tz::America__Grand_Turk,
    119 => Tz::America__Grenada,
    120 => Tz::America__Guadeloupe,
    121 => Tz::America__Guatemala,
    122 => Tz::America__Guayaquil,
    123 => Tz::America__Guyana,
    124 => Tz::America__Halifax,
    125 => Tz::America__Havana,
    126 => Tz::America__Hermosillo,
    127 => Tz::America__Indiana__Indianapolis,
    128 => Tz::America__Indiana__Knox,
    129 => Tz::America__Indiana__Marengo,
    130 => Tz::America__Indiana__Petersburg,
    131 => Tz::America__Indiana__Tell_City,
    132 => Tz::America__Indiana__Vevay,
    133 => Tz::America__Indiana__Vincennes,
    134 => Tz::America__Indiana__Winamac,
    135 => Tz::America__Indianapolis,
    136 => Tz::America__Inuvik,
    137 => Tz::America__Iqaluit,
    138 => Tz::America__Jamaica,
    139 => Tz::America__Jujuy,
    140 => Tz::America__Juneau,
    141 => Tz::America__Kentucky__Louisville,
    142 => Tz::America__Kentucky__Monticello,
    143 => Tz::America__Knox_IN,
    144 => Tz::America__Kralendijk,
    145 => Tz::America__La_Paz,
    146 => Tz::America__Lima,
    147 => Tz::America__Los_Angeles,
    148 => Tz::America__Louisville,
    149 => Tz::America__Lower_Princes,
    150 => Tz::America__Maceio,
    151 => Tz::America__Managua,
    152 => Tz::America__Manaus,
    153 => Tz::America__Marigot,
    154 => Tz::America__Martinique,
    155 => Tz::America__Matamoros,
    156 => Tz::America__Mazatlan,
    157 => Tz::America__Mendoza,
    158 => Tz::America__Menominee,
    159 => Tz::America__Merida,
    160 => Tz::America__Metlakatla,
    161 => Tz::America__Mexico_City,
    162 => Tz::America__Miquelon,
    163 => Tz::America__Moncton,
    164 => Tz::America__Monterrey,
    165 => Tz::America__Montevideo,
    166 => Tz::America__Montreal,
    167 => Tz::America__Montserrat,
    168 => Tz::America__Nassau,
    169 => Tz::America__New_York,
    170 => Tz::America__Nipigon,
    171 => Tz::America__Nome,
    172 => Tz::America__Noronha,
    173 => Tz::America__North_Dakota__Beulah,
    174 => Tz::America__North_Dakota__Center,
    175 => Tz::America__North_Dakota__New_Salem,
    176 => Tz::America__Nuuk,
    177 => Tz::America__Ojinaga,
    178 => Tz::America__Panama,
    179 => Tz::America__Pangnirtung,
    180 => Tz::America__Paramaribo,
    181 => Tz::America__Phoenix,
    182 => Tz::America__PortauPrince,
    183 => Tz::America__Port_of_Spain,
    184 => Tz::America__Porto_Acre,
    185 => Tz::America__Porto_Velho,
    186 => Tz::America__Puerto_Rico,
    187 => Tz::America__Punta_Arenas,
    188 => Tz::America__Rainy_River,
    189 => Tz::America__Rankin_Inlet,
    190 => Tz::America__Recife,
    191 => Tz::America__Regina,
    192 => Tz::America__Resolute,
    193 => Tz::America__Rio_Branco,
    194 => Tz::America__Rosario,
    195 => Tz::America__Santa_Isabel,
    196 => Tz::America__Santarem,
    197 => Tz::America__Santiago,
    198 => Tz::America__Santo_Domingo,
    199 => Tz::America__Sao_Paulo,
    200 => Tz::America__Scoresbysund,
    201 => Tz::America__Shiprock,
    202 => Tz::America__Sitka,
    203 => Tz::America__St_Barthelemy,
    204 => Tz::America__St_Johns,
    205 => Tz::America__St_Kitts,
    206 => Tz::America__St_Lucia,
    207 => Tz::America__St_Thomas,
    208 => Tz::America__St_Vincent,
    209 => Tz::America__Swift_Current,
    210 => Tz::America__Tegucigalpa,
    211 => Tz::America__Thule,
    212 => Tz::America__Thunder_Bay,
    213 => Tz::America__Tijuana,
    214 => Tz::America__Toronto,
    215 => Tz::America__Tortola,
    216 => Tz::America__Vancouver,
    217 => Tz::America__Virgin,
    218 => Tz::America__Whitehorse,
    219 => Tz::America__Winnipeg,
    220 => Tz::America__Yakutat,
    221 => Tz::America__Yellowknife,
    222 => Tz::Antarctica__Casey,
    223 => Tz::Antarctica__Davis,
    224 => Tz::Antarctica__DumontDUrville,
    225 => Tz::Antarctica__Macquarie,
    226 => Tz::Antarctica__Mawson,
    227 => Tz::Antarctica__McMurdo,
    228 => Tz::Antarctica__Palmer,
    229 => Tz::Antarctica__Rothera,
    230 => Tz::Antarctica__South_Pole,
    231 => Tz::Antarctica__Syowa,
    232 => Tz::Antarctica__Troll,
    233 => Tz::Antarctica__Vostok,
    234 => Tz::Arctic__Longyearbyen,
    235 => Tz::Asia__Aden,
    236 => Tz::Asia__Almaty,
    237 => Tz::Asia__Amman,
    238 => Tz::Asia__Anadyr,
    239 => Tz::Asia__Aqtau,
    240 => Tz::Asia__Aqtobe,
    241 => Tz::Asia__Ashgabat,
    242 => Tz::Asia__Ashkhabad,
    243 => Tz::Asia__Atyrau,
    244 => Tz::Asia__Baghdad,
    245 => Tz::Asia__Bahrain,
    246 => Tz::Asia__Baku,
    247 => Tz::Asia__Bangkok,
    248 => Tz::Asia__Barnaul,
    249 => Tz::Asia__Beirut,
    250 => Tz::Asia__Bishkek,
    251 => Tz::Asia__Brunei,
    252 => Tz::Asia__Calcutta,
    253 => Tz::Asia__Chita,
    254 => Tz::Asia__Choibalsan,
    255 => Tz::Asia__Chongqing,
    256 => Tz::Asia__Chungking,
    257 => Tz::Asia__Colombo,
    258 => Tz::Asia__Dacca,
    259 => Tz::Asia__Damascus,
    260 => Tz::Asia__Dhaka,
    261 => Tz::Asia__Dili,
    262 => Tz::Asia__Dubai,
    263 => Tz::Asia__Dushanbe,
    264 => Tz::Asia__Famagusta,
    265 => Tz::Asia__Gaza,
    266 => Tz::Asia__Harbin,
    267 => Tz::Asia__Hebron,
    268 => Tz::Asia__Ho_Chi_Minh,
    269 => Tz::Asia__Hong_Kong,
    270 => Tz::Asia__Hovd,
    271 => Tz::Asia__Irkutsk,
    272 => Tz::Asia__Istanbul,
    273 => Tz::Asia__Jakarta,
    274 => Tz::Asia__Jayapura,
    275 => Tz::Asia__Jerusalem,
    276 => Tz::Asia__Kabul,
    277 => Tz::Asia__Kamchatka,
    278 => Tz::Asia__Karachi,
    279 => Tz::Asia__Kashgar,
    280 => Tz::Asia__Kathmandu,
    281 => Tz::Asia__Katmandu,
    282 => Tz::Asia__Khandyga,
    283 => Tz::Asia__Kolkata,
    284 => Tz::Asia__Krasnoyarsk,
    285 => Tz::Asia__Kuala_Lumpur,
    286 => Tz::Asia__Kuching,
    287 => Tz::Asia__Kuwait,
    288 => Tz::Asia__Macao,
    289 => Tz::Asia__Macau,
    290 => Tz::Asia__Magadan,
    291 => Tz::Asia__Makassar,
    292 => Tz::Asia__Manila,
    293 => Tz::Asia__Muscat,
    294 => Tz::Asia__Nicosia,
    295 => Tz::Asia__Novokuznetsk,
    296 => Tz::Asia__Novosibirsk,
    297 => Tz::Asia__Omsk,
    298 => Tz::Asia__Oral,
    299 => Tz::Asia__Phnom_Penh,
    300 => Tz::Asia__Pontianak,
    301 => Tz::Asia__Pyongyang,
    302 => Tz::Asia__Qatar,
    303 => Tz::Asia__Qostanay,
    304 => Tz::Asia__Qyzylorda,
    305 => Tz::Asia__Rangoon,
    306 => Tz::Asia__Riyadh,
    307 => Tz::Asia__Saigon,
    308 => Tz::Asia__Sakhalin,
    309 => Tz::Asia__Samarkand,
    310 => Tz::Asia__Seoul,
    311 => Tz::Asia__Shanghai,
    312 => Tz::Asia__Singapore,
    313 => Tz::Asia__Srednekolymsk,
    314 => Tz::Asia__Taipei,
    315 => Tz::Asia__Tashkent,
    316 => Tz::Asia__Tbilisi,
    317 => Tz::Asia__Tehran,
    318 => Tz::Asia__Tel_Aviv,
    319 => Tz::Asia__Thimbu,
    320 => Tz::Asia__Thimphu,
    321 => Tz::Asia__Tokyo,
    322 => Tz::Asia__Tomsk,
    323 => Tz::Asia__Ujung_Pandang,
    324 => Tz::Asia__Ulaanbaatar,
    325 => Tz::Asia__Ulan_Bator,
    326 => Tz::Asia__Urumqi,
    327 => Tz::Asia__UstNera,
    328 => Tz::Asia__Vientiane,
    329 => Tz::Asia__Vladivostok,
    330 => Tz::Asia__Yakutsk,
    331 => Tz::Asia__Yangon,
    332 => Tz::Asia__Yekaterinburg,
    333 => Tz::Asia__Yerevan,
    334 => Tz::Atlantic__Azores,
    335 => Tz::Atlantic__Bermuda,
    336 => Tz::Atlantic__Canary,
    337 => Tz::Atlantic__Cape_Verde,
    338 => Tz::Atlantic__Faeroe,
    339 => Tz::Atlantic__Faroe,
    340 => Tz::Atlantic__Jan_Mayen,
    341 => Tz::Atlantic__Madeira,
    342 => Tz::Atlantic__Reykjavik,
    343 => Tz::Atlantic__South_Georgia,
    344 => Tz::Atlantic__St_Helena,
    345 => Tz::Atlantic__Stanley,
    346 => Tz::Australia__ACT,
    347 => Tz::Australia__Adelaide,
    348 => Tz::Australia__Brisbane,
    349 => Tz::Australia__Broken_Hill,
    350 => Tz::Australia__Canberra,
    351 => Tz::Australia__Currie,
    352 => Tz::Australia__Darwin,
    353 => Tz::Australia__Eucla,
    354 => Tz::Australia__Hobart,
    355 => Tz::Australia__LHI,
    356 => Tz::Australia__Lindeman,
    357 => Tz::Australia__Lord_Howe,
    358 => Tz::Australia__Melbourne,
    359 => Tz::Australia__NSW,
    360 => Tz::Australia__North,
    361 => Tz::Australia__Perth,
    362 => Tz::Australia__Queensland,
    363 => Tz::Australia__South,
    364 => Tz::Australia__Sydney,
    365 => Tz::Australia__Tasmania,
    366 => Tz::Australia__Victoria,
    367 => Tz::Australia__West,
    368 => Tz::Australia__Yancowinna,
    369 => Tz::Brazil__Acre,
    370 => Tz::Brazil__DeNoronha,
    371 => Tz::Brazil__East,
    372 => Tz::Brazil__West,
    373 => Tz::CET,
    374 => Tz::CST6CDT,
    375 => Tz::Canada__Atlantic,
    376 => Tz::Canada__Central,
    377 => Tz::Canada__Eastern,
    378 => Tz::Canada__Mountain,
    379 => Tz::Canada__Newfoundland,
    380 => Tz::Canada__Pacific,
    381 => Tz::Canada__Saskatchewan,
    382 => Tz::Canada__Yukon,
    383 => Tz::Chile__Continental,
    384 => Tz::Chile__EasterIsland,
    385 => Tz::Cuba,
    386 => Tz::EET,
    387 => Tz::EST,
    388 => Tz::EST5EDT,
    389 => Tz::Egypt,
    390 => Tz::Eire,
    391 => Tz::Etc__GMT,
    392 => Tz::Etc__GMTPlus0,
    393 => Tz::Etc__GMTPlus1,
    394 => Tz::Etc__GMTPlus10,
    395 => Tz::Etc__GMTPlus11,
    396 => Tz::Etc__GMTPlus12,
    397 => Tz::Etc__GMTPlus2,
    398 => Tz::Etc__GMTPlus3,
    399 => Tz::Etc__GMTPlus4,
    400 => Tz::Etc__GMTPlus5,
    401 => Tz::Etc__GMTPlus6,
    402 => Tz::Etc__GMTPlus7,
    403 => Tz::Etc__GMTPlus8,
    404 => Tz::Etc__GMTPlus9,
    405 => Tz::Etc__GMTMinus0,
    406 => Tz::Etc__GMTMinus1,
    407 => Tz::Etc__GMTMinus10,
    408 => Tz::Etc__GMTMinus11,
    409 => Tz::Etc__GMTMinus12,
    410 => Tz::Etc__GMTMinus13,
    411 => Tz::Etc__GMTMinus14,
    412 => Tz::Etc__GMTMinus2,
    413 => Tz::Etc__GMTMinus3,
    414 => Tz::Etc__GMTMinus4,
    415 => Tz::Etc__GMTMinus5,
    416 => Tz::Etc__GMTMinus6,
    417 => Tz::Etc__GMTMinus7,
    418 => Tz::Etc__GMTMinus8,
    419 => Tz::Etc__GMTMinus9,
    420 => Tz::Etc__GMT0,
    421 => Tz::Etc__Greenwich,
    422 => Tz::Etc__UCT,
    423 => Tz::Etc__UTC,
    424 => Tz::Etc__Universal,
    425 => Tz::Etc__Zulu,
    426 => Tz::Europe__Amsterdam,
    427 => Tz::Europe__Andorra,
    428 => Tz::Europe__Astrakhan,
    429 => Tz::Europe__Athens,
    430 => Tz::Europe__Belfast,
    431 => Tz::Europe__Belgrade,
    432 => Tz::Europe__Berlin,
    433 => Tz::Europe__Bratislava,
    434 => Tz::Europe__Brussels,
    435 => Tz::Europe__Bucharest,
    436 => Tz::Europe__Budapest,
    437 => Tz::Europe__Busingen,
    438 => Tz::Europe__Chisinau,
    439 => Tz::Europe__Copenhagen,
    440 => Tz::Europe__Dublin,
    441 => Tz::Europe__Gibraltar,
    442 => Tz::Europe__Guernsey,
    443 => Tz::Europe__Helsinki,
    444 => Tz::Europe__Isle_of_Man,
    445 => Tz::Europe__Istanbul,
    446 => Tz::Europe__Jersey,
    447 => Tz::Europe__Kaliningrad,
    448 => Tz::Europe__Kiev,
    449 => Tz::Europe__Kirov,
    450 => Tz::Europe__Kyiv,
    451 => Tz::Europe__Lisbon,
    452 => Tz::Europe__Ljubljana,
    453 => Tz::Europe__London,
    454 => Tz::Europe__Luxembourg,
    455 => Tz::Europe__Madrid,
    456 => Tz::Europe__Malta,
    457 => Tz::Europe__Mariehamn,
    458 => Tz::Europe__Minsk,
    459 => Tz::Europe__Monaco,
    460 => Tz::Europe__Moscow,
    461 => Tz::Europe__Nicosia,
    462 => Tz::Europe__Oslo,
    463 => Tz::Europe__Paris,
    464 => Tz::Europe__Podgorica,
    465 => Tz::Europe__Prague,
    466 => Tz::Europe__Riga,
    467 => Tz::Europe__Rome,
    468 => Tz::Europe__Samara,
    469 => Tz::Europe__San_Marino,
    470 => Tz::Europe__Sarajevo,
    471 => Tz::Europe__Saratov,
    472 => Tz::Europe__Simferopol,
    473 => Tz::Europe__Skopje,
    474 => Tz::Europe__Sofia,
    475 => Tz::Europe__Stockholm,
    476 => Tz::Europe__Tallinn,
    477 => Tz::Europe__Tirane,
    478 => Tz::Europe__Tiraspol,
    479 => Tz::Europe__Ulyanovsk,
    480 => Tz::Europe__Uzhgorod,
    481 => Tz::Europe__Vaduz,
    482 => Tz::Europe__Vatican,
    483 => Tz::Europe__Vienna,
    484 => Tz::Europe__Vilnius,
    485 => Tz::Europe__Volgograd,
    486 => Tz::Europe__Warsaw,
    487 => Tz::Europe__Zagreb,
    488 => Tz::Europe__Zaporozhye,
    489 => Tz::Europe__Zurich,
    490 => Tz::GB,
    491 => Tz::GBEire,
    492 => Tz::GMT,
    493 => Tz::GMTPlus0,
    494 => Tz::GMTMinus0,
    495 => Tz::GMT0,
    496 => Tz::Greenwich,
    497 => Tz::HST,
    498 => Tz::Hongkong,
    499 => Tz::Iceland,
    500 => Tz::Indian__Antananarivo,
    501 => Tz::Indian__Chagos,
    502 => Tz::Indian__Christmas,
    503 => Tz::Indian__Cocos,
    504 => Tz::Indian__Comoro,
    505 => Tz::Indian__Kerguelen,
    506 => Tz::Indian__Mahe,
    507 => Tz::Indian__Maldives,
    508 => Tz::Indian__Mauritius,
    509 => Tz::Indian__Mayotte,
    510 => Tz::Indian__Reunion,
    511 => Tz::Iran,
    512 => Tz::Israel,
    513 => Tz::Jamaica,
    514 => Tz::Japan,
    515 => Tz::Kwajalein,
    516 => Tz::Libya,
    517 => Tz::MET,
    518 => Tz::MST,
    519 => Tz::MST7MDT,
    520 => Tz::Mexico__BajaNorte,
    521 => Tz::Mexico__BajaSur,
    522 => Tz::Mexico__General,
    523 => Tz::NZ,
    524 => Tz::NZCHAT,
    525 => Tz::Navajo,
    526 => Tz::PRC,
    527 => Tz::PST8PDT,
    528 => Tz::Pacific__Apia,
    529 => Tz::Pacific__Auckland,
    530 => Tz::Pacific__Bougainville,
    531 => Tz::Pacific__Chatham,
    532 => Tz::Pacific__Chuuk,
    533 => Tz::Pacific__Easter,
    534 => Tz::Pacific__Efate,
    535 => Tz::Pacific__Enderbury,
    536 => Tz::Pacific__Fakaofo,
    537 => Tz::Pacific__Fiji,
    538 => Tz::Pacific__Funafuti,
    539 => Tz::Pacific__Galapagos,
    540 => Tz::Pacific__Gambier,
    541 => Tz::Pacific__Guadalcanal,
    542 => Tz::Pacific__Guam,
    543 => Tz::Pacific__Honolulu,
    544 => Tz::Pacific__Johnston,
    545 => Tz::Pacific__Kanton,
    546 => Tz::Pacific__Kiritimati,
    547 => Tz::Pacific__Kosrae,
    548 => Tz::Pacific__Kwajalein,
    549 => Tz::Pacific__Majuro,
    550 => Tz::Pacific__Marquesas,
    551 => Tz::Pacific__Midway,
    552 => Tz::Pacific__Nauru,
    553 => Tz::Pacific__Niue,
    554 => Tz::Pacific__Norfolk,
    555 => Tz::Pacific__Noumea,
    556 => Tz::Pacific__Pago_Pago,
    557 => Tz::Pacific__Palau,
    558 => Tz::Pacific__Pitcairn,
    559 => Tz::Pacific__Pohnpei,
    560 => Tz::Pacific__Ponape,
    561 => Tz::Pacific__Port_Moresby,
    562 => Tz::Pacific__Rarotonga,
    563 => Tz::Pacific__Saipan,
    564 => Tz::Pacific__Samoa,
    565 => Tz::Pacific__Tahiti,
    566 => Tz::Pacific__Tarawa,
    567 => Tz::Pacific__Tongatapu,
    568 => Tz::Pacific__Truk,
    569 => Tz::Pacific__Wake,
    570 => Tz::Pacific__Wallis,
    571 => Tz::Pacific__Yap,
    572 => Tz::Poland,
    573 => Tz::Portugal,
    574 => Tz::ROC,
    575 => Tz::ROK,
    576 => Tz::Singapore,
    577 => Tz::Turkey,
    578 => Tz::UCT,
    579 => Tz::US__Alaska,
    580 => Tz::US__Aleutian,
    581 => Tz::US__Arizona,
    582 => Tz::US__Central,
    583 => Tz::US__EastIndiana,
    584 => Tz::US__Eastern,
    585 => Tz::US__Hawaii,
    586 => Tz::US__IndianaStarke,
    587 => Tz::US__Michigan,
    588 => Tz::US__Mountain,
    589 => Tz::US__Pacific,
    590 => Tz::US__Samoa,
    591 => Tz::UTC,
    592 => Tz::Universal,
    593 => Tz::WSU,
    594 => Tz::WET,
    595 => Tz::Zulu,
}
