
--
-- Name: az_bakuappealcourt_cases; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE az_bakuappealcourt_cases (
    id integer NOT NULL,
    date text,
    case_id text,
    details text,
    source_url text
);




--
-- Name: az_bakuappealcourt_cases_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE az_bakuappealcourt_cases_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: az_bakuappealcourt_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE az_bakuappealcourt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: az_taxcompanies; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE az_taxcompanies (
    id integer NOT NULL,
    tin text,
    nizamnama_kapitali text,
    huquqi_unvani text,
    qanuni_tamsilci text,
    voen text,
    dovlat_qeydiyyatina_alindigi_tarix text,
    taskilati_huquqi_formasi text,
    maliyya_ili text,
    kommersiya_qurumunun_adi text,
    register character varying(3),
    sequence integer,
    code integer
);




--
-- Name: az_tenders; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE az_tenders (
    id integer NOT NULL
);




--
-- Name: az_tenders_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE az_tenders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: az_voters; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE az_voters (
    id integer NOT NULL,
    constituency_id text,
    place_id text,
    table_index text,
    voter_name text,
    voter_address text,
    constituency_name text,
    source_url text,
    birth_year text
);




--
-- Name: az_voters_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE az_voters_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: azer_companies_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE azer_companies_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_katastar_building; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_katastar_building (
    id integer NOT NULL
);




--
-- Name: ba_katastar_building_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_katastar_building_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_katastar_deed_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_katastar_deed_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: ba_katastar_land; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_katastar_land (
    id integer NOT NULL,
    url text,
    owner text,
    share text,
    kat_id text,
    number integer,
    muni_name text,
    valid boolean,
    muni_id text,
    kat_name text,
    address text,
    area integer,
    owner_norm text
);




--
-- Name: ba_katastar_land_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_katastar_land_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_rgurs_owner; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_rgurs_owner (
    id integer NOT NULL,
    "katastarskaOpstinaId" text,
    "deedId" integer,
    "opstinaId" text,
    "registerId" text,
    "ownerId" integer,
    "nazivKatastarskeOpstine" text,
    adresa text,
    "imePrezime" text,
    jmbg text,
    udeo text,
    "vrstaPrava" text,
    "registerName" text,
    partytype text,
    "nazivOpstine" text
);




--
-- Name: ba_rgurs_owner_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_rgurs_owner_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_rgurs_parcel; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_rgurs_parcel (
    id integer NOT NULL,
    "opstinaId" text,
    "katastarskaOpstinaId" text,
    "deedId" integer,
    "parcelId" integer,
    "registerId" text,
    "nazivKatastarskeOpstine" text,
    napomena text,
    povrsina text,
    blok text,
    "nazivParcele" text,
    "nacinKoriscenja" text,
    "namenaKoriscenja" text,
    podbroj text,
    broj text,
    "registerName" text,
    "redniBroj" text,
    "nazivOpstine" text
);




--
-- Name: ba_rgurs_parcel_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_rgurs_parcel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_rgurs_real_estate_owner; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_rgurs_real_estate_owner (
    id integer NOT NULL,
    "katastarskaOpstinaId" text,
    "deedId" integer,
    "opstinaId" text,
    "registerId" text,
    "ownerId" integer,
    "nazivKatastarskeOpstine" text,
    adresa text,
    "imePrezime" text,
    jmbg text,
    udeo text,
    "vrstaPrava" text,
    "registerName" text,
    partytype text,
    "nazivOpstine" text
);




--
-- Name: ba_rgurs_real_estate_owner_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_rgurs_real_estate_owner_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_rgurs_real_estate_parcel; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_rgurs_real_estate_parcel (
    id integer NOT NULL,
    "opstinaId" text,
    "katastarskaOpstinaId" text,
    "deedId" integer,
    "parcelId" integer,
    "registerId" text,
    "nazivKatastarskeOpstine" text,
    napomena text,
    povrsina text,
    "nacinKoriscenja" text,
    osnov text,
    "namenaKoriscenja" text,
    potes text,
    adresa text,
    podbroj text,
    broj text,
    "registerName" text,
    "redniBroj" text,
    "nazivOpstine" text
);




--
-- Name: ba_rgurs_real_estate_parcel_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_rgurs_real_estate_parcel_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_rgurs_real_estate_record; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ba_rgurs_real_estate_record (
    id integer NOT NULL,
    "katastarskaOpstinaId" text,
    "deedId" integer,
    "opstinaId" text,
    "registerId" text,
    "nazivKatastarskeOpstine" text,
    json text,
    "registerName" text,
    "nazivOpstine" text
);




--
-- Name: ba_rgurs_real_estate_record_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_rgurs_real_estate_record_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ba_rgurs_record_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ba_rgurs_record_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: bw_boundary; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE bw_boundary (
    id integer NOT NULL,
    source_title text,
    layer_name text,
    "OBJECTID" integer,
    layer_id integer,
    source_name text,
    source_url text
);




--
-- Name: bw_boundary_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE bw_boundary_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: bw_districts; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE bw_districts (
    id integer NOT NULL,
    source_title text,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_url text,
    "FTYPE" integer,
    layer_name text,
    source_name text,
    "TYPE" text
);




--
-- Name: bw_districts_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE bw_districts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: bw_farms; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE bw_farms (
    id integer NOT NULL,
    source_title text,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_url text,
    layer_name text,
    "VILLAGENAM" text,
    "OWNER" text,
    source_name text
);




--
-- Name: bw_farms_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE bw_farms_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: bw_petroleum; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE bw_petroleum (
    id integer NOT NULL,
    "STATUS" text,
    "ISSUE_DATE" text,
    "SIGN_DATE" text,
    source_title text,
    "OBJECTID" integer,
    "COMMODITY" text,
    "PL_NO" text,
    layer_id integer,
    "COMPANY" text,
    "RECEVD_DAT" integer,
    source_url text,
    "MINERALRIG" text,
    "AREA_" text,
    "EXPIRY_DAT" text,
    source_name text,
    layer_name text,
    "AREA_UNIT" text
);




--
-- Name: bw_petroleum_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE bw_petroleum_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: bw_protected_areas; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE bw_protected_areas (
    id integer NOT NULL,
    source_title text,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_url text,
    layer_name text,
    source_name text,
    "TYPE" text
);




--
-- Name: bw_protected_areas_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE bw_protected_areas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: bw_townships; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE bw_townships (
    id integer NOT NULL,
    "OBJECTID_1" integer,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_url text,
    "FTYPE" integer,
    layer_name text,
    source_name text,
    "SHAPE_LENG" double precision,
    source_title text,
    "TYPE" text
);




--
-- Name: bw_townships_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE bw_townships_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: gb_companies_house_directorships; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_companies_house_directorships (
    version character varying(255),
    poscode character varying(1024),
    resdate_str character varying(1024) NOT NULL,
    func character varying(1024),
    pnr bigint NOT NULL,
    trp03 character varying(1024),
    org character varying(1024) NOT NULL,
    funcode character varying(1024),
    pos character varying(1024),
    sec character varying(1024) NOT NULL,
    active character varying(1024),
    resdate date,
    appdate date,
    id bigint NOT NULL
);




--
-- Name: cs_uk_directorships_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE cs_uk_directorships_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: gb_companies_house_trading_addresses; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_companies_house_trading_addresses (
    version character varying(255),
    _id bigint NOT NULL,
    tradpost character varying(1024),
    trad1 character varying(1024),
    trad4 character varying(1024),
    tel character varying(1024),
    td001 character varying(1024),
    org character varying(1024),
    tps character varying(1024),
    trad2 character varying(1024),
    trad3 character varying(1024)
);




--
-- Name: cs_uk_trading_addresses__id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE cs_uk_trading_addresses__id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bafin_directors_dealings; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bafin_directors_dealings (
    id integer NOT NULL,
    meldungsnr text,
    bafin_id text,
    emittent_name text,
    geschaftsart text,
    rolle_des_auslosers text,
    veroffentlichungsmedium text,
    wahrung text,
    handelsplatz text,
    basis_wkn_isin text,
    sitz text,
    bemerkung text,
    rolle_des_meldepflichtigen text,
    nominale text,
    falligkeit text,
    isin text,
    wertpapierbezeichnung text,
    meldepflichtiger_name text,
    veroffentlichungsdatum text,
    preismultiplikator text,
    kurs text,
    handelstag text
);




--
-- Name: de_bafin_directors_dealings_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bafin_directors_dealings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundeshaushalt; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundeshaushalt (
    id integer NOT NULL,
    address text,
    flow text,
    year integer,
    funktion_3_title text,
    funktion_3_address text,
    einzelplan_1_title text,
    gruppe_2_address text,
    gruppe_3_address text,
    gruppe_2_title text,
    gruppe_1_address text,
    einzelplan_1_address text,
    funktion_2_title text,
    account text,
    name text,
    title text,
    gruppe_1_title text,
    einzelplan_2_address text,
    value integer,
    funktion_1_title text,
    funktion_2_address text,
    gruppe_3_title text,
    einzelplan_2_title text,
    funktion_1_address text,
    flexible text
);




--
-- Name: de_bundeshaushalt_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundeshaushalt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_ablauf; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_ablauf (
    id integer NOT NULL,
    source_url text,
    signatur text,
    titel text,
    zustimmungsbeduerftig text,
    abgeschlossen boolean,
    wahlperiode integer,
    eu_dok_nr text,
    abstrakt text,
    initiative text,
    stand text,
    key integer,
    gesta_id text,
    typ text,
    sachgebiet text
);




--
-- Name: de_bundestag_ablauf_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_ablauf_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_beitrag; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_beitrag (
    id integer NOT NULL,
    source_url text,
    land text,
    art text,
    funktion text,
    seite text,
    ort text,
    ressort text,
    fundstelle text,
    nachname text,
    urheber text,
    vorname text,
    fraktion text
);




--
-- Name: de_bundestag_beitrag_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_beitrag_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_beschluss; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_beschluss (
    id integer NOT NULL,
    source_url text,
    grundlage text,
    seite text,
    fundstelle text,
    urheber text,
    dokument_text text,
    tenor text
);




--
-- Name: de_bundestag_beschluss_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_beschluss_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_person; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_person (
    id integer NOT NULL,
    ort text,
    nachname text,
    vorname text
);




--
-- Name: de_bundestag_person_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_person_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_plpr; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_plpr (
    id integer NOT NULL,
    sitzung integer,
    wahlperiode integer,
    in_writing boolean,
    sequence integer,
    text text,
    speaker_cleaned text,
    speaker_fp text,
    filename text,
    speaker text,
    speaker_party text,
    type text
);




--
-- Name: de_bundestag_plpr_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_plpr_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_position; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_position (
    id integer NOT NULL,
    source_url text
);




--
-- Name: de_bundestag_position_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_position_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_referenz; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_referenz (
    id integer NOT NULL,
    source_url text,
    fundstelle text,
    urheber text,
    nummer text,
    hrsg text,
    link text,
    typ text,
    seiten text,
    text text
);




--
-- Name: de_bundestag_referenz_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_referenz_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_schlagwort; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_schlagwort (
    id integer NOT NULL,
    wort text,
    source_url text
);




--
-- Name: de_bundestag_schlagwort_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_schlagwort_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_bundestag_zuweisung; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_bundestag_zuweisung (
    id integer NOT NULL,
    source_url text,
    gremium_key text,
    text text,
    fundstelle text,
    urheber text,
    federfuehrung boolean
);




--
-- Name: de_bundestag_zuweisung_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_bundestag_zuweisung_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_foerderkatalog; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE de_foerderkatalog (
    id integer NOT NULL,
    fkz text,
    profil text,
    summe text,
    art text,
    empfaenger_land text,
    empfaenger_staat text,
    laufzeit_bis text,
    url text,
    projekttraeger text,
    empfaenger_name text,
    lps text,
    ressort text,
    arbeitseinheit text,
    empfaenger_ort text,
    laufzeit_von text,
    stelle_ort text,
    thema text,
    stelle_name text,
    stelle_land text,
    referat text,
    stelle_staat text,
    bezeichnung text,
    summe_num text
);




--
-- Name: de_foerderkatalog_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_foerderkatalog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: de_handelsregister_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE de_handelsregister_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: eu_expert_data; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_data (
    id integer NOT NULL,
    "group" text,
    xml text,
    first_seen timestamp,
    name text,
    last_seen timestamp
);




--
-- Name: eu_expert_data_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group (
    id integer NOT NULL,
    group_id text,
    status text,
    last_updated text,
    name text,
    lead_dg text,
    mission text,
    abbreviation text,
    active_since text,
    scope text
);




--
-- Name: eu_expert_group_associated_dg; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group_associated_dg (
    id integer NOT NULL,
    dg text,
    group_id text
);




--
-- Name: eu_expert_group_associated_dg_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_associated_dg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group_member; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group_member (
    id integer NOT NULL,
    country text,
    subgroup_name text,
    group_id text,
    name text,
    member_type text,
    status text,
    public_authorities text,
    type text
);




--
-- Name: eu_expert_group_member_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_member_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group_note; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group_note (
    id integer NOT NULL,
    category text,
    info text,
    group_id text,
    link text
);




--
-- Name: eu_expert_group_note_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_note_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group_policy_area; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group_policy_area (
    id integer NOT NULL,
    group_id text,
    policy_area text
);




--
-- Name: eu_expert_group_policy_area_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_policy_area_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group_task; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group_task (
    id integer NOT NULL,
    task text,
    group_id text
);




--
-- Name: eu_expert_group_task_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_task_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_group_type; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_group_type (
    id integer NOT NULL,
    group_id text,
    type text
);




--
-- Name: eu_expert_group_type_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_group_type_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_expert_sub_group; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_expert_sub_group (
    id integer NOT NULL,
    group_id text,
    name text
);




--
-- Name: eu_expert_sub_group_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_expert_sub_group_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_fts; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_fts (
    id integer NOT NULL,
    source_id integer,
    source_file text,
    coordinator text,
    postcode text,
    budget_code text,
    vat_number text,
    total double precision,
    city text,
    source_contract_id integer,
    title text,
    beneficiary text,
    cofinancing_rate_pct double precision,
    grant_subject text,
    expensetype text,
    budget_item text,
    geozone text,
    cofinancing_rate text,
    source_line integer,
    source_url text,
    address text,
    date text,
    article text,
    chapter text,
    country text,
    position_key text,
    alias text,
    amount double precision,
    responsible_department text,
    action_type text,
    item text
);




--
-- Name: eu_fts_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_fts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_ted_contracts; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_ted_contracts (
    id integer NOT NULL,
    doc_no text,
    activity_contractor text,
    lot_number text,
    authority_country text,
    contract_award_month text,
    type_contract text,
    contract_award_title text,
    authority_postal_code text,
    location_nuts text,
    offers_received_meaning text,
    operator_slug text,
    contract_value_vat_included boolean,
    operator_town text,
    authority_address text,
    concessionaire_email text,
    appeal_procedure text,
    authority_official_name text,
    activity_type_other text,
    notice_dispatch_month text,
    contract_value_currency text,
    appeal_body_slug text,
    electronic_auction text,
    contract_value_cost text,
    operator_address text,
    notice_dispatch_year text,
    concessionaire_nationalid text,
    authority_fax text,
    contract_award_day text,
    operator_official_name text,
    concessionaire_contact text,
    operator_postal_code text,
    appeal_body_phone text,
    appeal_body_country text,
    appeal_body_fax text,
    authority_phone text,
    contract_number text,
    authority_attention text,
    contract_type_supply text,
    appeal_body_email text,
    operator_country text,
    contract_value_cost_eur double precision,
    index integer,
    relates_to_eu_project text,
    notice_dispatch_day text,
    authority_email text,
    authority_town text,
    additional_information text,
    appeal_body_postal_code text,
    authority_slug text,
    appeal_body_town text,
    contract_award_year text,
    gpa_covered text,
    offers_received_num text,
    contract_title text,
    location text,
    file_reference text,
    contract_description text,
    appeal_body_address text,
    cpv_code text,
    authority_url text,
    appeal_body_url text,
    appeal_body_official_name text,
    activity_type text,
    authority_url_buyer text,
    total_value_cost_eur double precision,
    total_value_currency text,
    total_value_vat_included boolean,
    total_value_cost text,
    initial_value_currency text,
    authority_url_info text,
    initial_value_cost text,
    initial_value_cost_eur double precision,
    operator_phone text,
    operator_fax text,
    initial_value_vat_included boolean,
    operator_email text,
    authority_url_participate text,
    operator_url text,
    contract_value_vat_rate text,
    initial_value_vat_rate text,
    total_value_vat_rate text,
    on_behalf_town text,
    on_behalf_country text,
    on_behalf_slug text,
    on_behalf_official_name text,
    on_behalf_address text,
    on_behalf_postal_code text,
    total_value_low text,
    total_value_high_eur double precision,
    total_value_low_eur double precision,
    total_value_high text,
    contract_value_high text,
    contract_value_low_eur double precision,
    contract_value_low text,
    contract_value_high_eur double precision
);




--
-- Name: eu_ted_contracts_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_ted_contracts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_ted_cpvs; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_ted_cpvs (
    id integer NOT NULL,
    doc_no text,
    text text,
    code text
);




--
-- Name: eu_ted_cpvs_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_ted_cpvs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_ted_documents; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_ted_documents (
    id integer NOT NULL,
    doc_no text,
    bid_type_code text,
    main_activities_code text,
    technical_deletion_date text,
    title_country text,
    award_criteria_code text,
    orig_nuts text,
    request_document_date text,
    regulation text,
    technical_form_lang text,
    oj_number text,
    info_url text,
    title_town text,
    title_text text,
    authority_name text,
    orig_nuts_code text,
    award_criteria text,
    document_type text,
    iso_country text,
    main_activities text,
    doc_url text,
    heading text,
    procedure text,
    document_type_code text,
    authority_type text,
    oj_date text,
    dispatch_date text,
    orig_language text,
    regulation_code text,
    procedure_code text,
    directive text,
    oj_collection text,
    technical_comments text,
    etendering_url text,
    contract_nature_code text,
    submission_date text,
    contract_nature text,
    reception_id text,
    technical_reception_id text,
    bid_type text,
    authority_type_code text
);




--
-- Name: eu_ted_documents_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_ted_documents_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_ted_references; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_ted_references (
    id integer NOT NULL,
    doc_no text,
    ref text
);




--
-- Name: eu_ted_references_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_ted_references_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_action_field; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_action_field (
    id integer NOT NULL,
    representative_etl_id text,
    action_field text,
    representative_update_date timestamp
);




--
-- Name: eu_tr_action_field_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_action_field_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_country_of_member; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_country_of_member (
    id integer NOT NULL,
    country text,
    representative_etl_id text,
    representative_update_date timestamp
);




--
-- Name: eu_tr_country_of_member_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_country_of_member_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_financial_data; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_financial_data (
    id integer NOT NULL,
    etl_id text,
    representative_etl_id text,
    other_sources_donation text,
    turnover_min text,
    eur_sources_procurement text,
    turnover_max text,
    public_financing_national text,
    cost_absolute text,
    direct_rep_costs_max text,
    turnover_absolute text,
    other_sources_contributions text,
    representative_update_date timestamp,
    public_financing_total text,
    type text,
    start_date timestamp,
    end_date timestamp,
    total_budget text,
    eur_sources_grants text,
    cost_min text,
    cost_max text,
    public_financing_infranational text,
    other_sources_total text,
    direct_rep_costs_min text
);




--
-- Name: eu_tr_financial_data_custom_source; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_financial_data_custom_source (
    id integer NOT NULL
);




--
-- Name: eu_tr_financial_data_custom_source_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_financial_data_custom_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_financial_data_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_financial_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_financial_data_turnover; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_financial_data_turnover (
    id integer NOT NULL,
    representative_etl_id text,
    financial_data_etl_id text,
    name text,
    min text,
    max text,
    representative_update_date timestamp
);




--
-- Name: eu_tr_financial_data_turnover_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_financial_data_turnover_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_interest; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_interest (
    id integer NOT NULL,
    representative_etl_id text,
    interest text,
    representative_update_date timestamp
);




--
-- Name: eu_tr_interest_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_interest_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_organisation; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_organisation (
    id integer NOT NULL
);




--
-- Name: eu_tr_organisation_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_organisation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_person; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_person (
    id integer NOT NULL,
    role text,
    representative_etl_id text,
    name text,
    first_name text,
    last_name text,
    title text,
    "position" text,
    representative_update_date timestamp,
    org_identification_code text
);




--
-- Name: eu_tr_person_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_person_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: eu_tr_representative; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE eu_tr_representative (
    id integer NOT NULL,
    identification_code text,
    etl_id text,
    members_100_percent text,
    sub_category text,
    contact_indic_phone text,
    activity_high_level_groups text,
    contact_more text,
    first_seen timestamp,
    contact_town text,
    contact_country text,
    contact_number text,
    contact_indic_fax text,
    networking text,
    main_category text,
    activity_expert_groups text,
    original_name text,
    members_info text,
    contact_street text,
    contact_phone text,
    activity_consult_committee text,
    status text,
    activity_inter_groups text,
    acronym text,
    activity_eu_legislative text,
    registration_date timestamp,
    activity_communication text,
    goals text,
    last_update_date timestamp,
    members_fte text,
    members_25_percent text,
    members_total text,
    web_site_url text,
    activity_other text,
    name text,
    number_of_natural_persons text,
    activity_industry_forums text,
    legal_status text,
    contact_post_code text,
    contact_fax text,
    number_of_organisations text,
    last_seen timestamp,
    code_of_conduct text
);




--
-- Name: eu_tr_representative_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE eu_tr_representative_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: flexi_changes_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE flexi_changes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: gb_companies_house_companies; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_companies_house_companies (
    version character varying(255),
    sic07desc character varying(1024),
    tradpost character varying(1024),
    sic07 character varying(1024),
    inc date,
    name character varying(1024),
    aorder character varying(1024),
    add2 character varying(1024),
    trad2 character varying(1024),
    add1 character varying(1024),
    trad1 character varying(1024),
    add4 character varying(1024),
    trad4 character varying(1024),
    trp01 character varying(1024),
    org character varying(1024) NOT NULL,
    add3 character varying(1024),
    trad3 character varying(1024),
    descr character varying(1024),
    rec_status character varying(1024),
    web character varying(1024),
    stat character varying(1024),
    type character varying(1024),
    volag character varying(1024),
    dissdate date,
    acc_type character varying(1024),
    post character varying(1024),
    admin character varying(1024),
    liq_status character varying(1024),
    charnum character varying(1024)
);




--
-- Name: gb_companies_house_company_accounts; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_companies_house_company_accounts (
    version character varying(255),
    tangas numeric,
    audqual character varying(1024),
    netwrth numeric,
    stock numeric,
    nomon numeric,
    taxation numeric,
    date date NOT NULL,
    incincsh numeric,
    sunrese numeric,
    placcres numeric,
    trdebt numeric,
    contliab character varying(1024),
    liabcurr numeric,
    asstotfix numeric,
    netass numeric,
    grprof numeric,
    ptaxpro numeric,
    ltloan numeric,
    diremol numeric,
    netcsfrfn numeric,
    trp04 character varying(1024),
    capemp numeric,
    accname character varying(1024),
    liabmisccur numeric,
    totliab numeric,
    asstotcur numeric,
    cstsales numeric,
    wrkcap numeric,
    liablt numeric,
    netcsbffn numeric,
    proaftax numeric,
    org character varying(1024) NOT NULL,
    intpay numeric,
    intass numeric,
    exports numeric,
    curr character varying(1024),
    bnkovr numeric,
    retpro numeric,
    audfee numeric,
    depre numeric,
    numemp numeric,
    audname character varying(1024),
    acctype character varying(1024),
    revres numeric,
    opprof numeric,
    bnkovltln numeric,
    shrttrmlns numeric,
    paidupeq numeric,
    asscur numeric,
    trover numeric,
    accformat integer NOT NULL,
    asstot numeric,
    acstat integer,
    wages numeric,
    trcred numeric,
    divpay numeric,
    opnetcashf numeric,
    consol character varying(1024),
    shrfun numeric,
    solname character varying(1024),
    assothcur numeric
);




--
-- Name: gb_companies_house_directors; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_companies_house_directors (
    version character varying(255),
    hns character varying(1024),
    suf character varying(1024),
    mname character varying(1024),
    dob date,
    trp02 character varying(1024),
    pnr bigint NOT NULL,
    title character varying(1024),
    ptitle character varying(1024),
    fname character varying(1024),
    sname character varying(1024)
);




--
-- Name: gb_companies_house_group_structure; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_companies_house_group_structure (
    version character varying(255),
    gs002 character varying(1024),
    regnum character varying(1024),
    safenum character varying(1024),
    name character varying(1024),
    holdnum character varying(1024),
    level numeric,
    org character varying(1024) NOT NULL,
    kfin date,
    ultholdnum character varying(1024)
);




--
-- Name: gb_land_registry; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_land_registry (
    id integer NOT NULL,
    price_num bigint,
    district text,
    date_of_registration text,
    proprietor_country text,
    proprietor_id text,
    proprietor_name text,
    title_number text,
    price_text_infill text,
    administrative_county text,
    year text,
    price_where_available text,
    proprietor_country_code text
);




--
-- Name: gb_land_registry_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE gb_land_registry_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: gb_land_registry_overseas; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_land_registry_overseas (
    "Title_Number" character varying(10) NOT NULL,
    "Tenure" character varying(9) NOT NULL,
    "Property_Address" character varying(995),
    "Price_Paid" character varying(8),
    "District" character varying(28),
    "County" character varying(28),
    "Region" character varying(16),
    "Postcode" character varying(8),
    "Proprietor_Name_1" character varying(177),
    "Company_Registration_No_1" character varying(29),
    "Proprietorship_1" character varying(41),
    "Country_Incorporated_1" character varying(45),
    "Proprietor_1_Address_1" character varying(321),
    "Proprietor_1_Address_2" character varying(177),
    "Proprietor_1_Address_3" character varying(162),
    "Proprietor_Name_2" character varying(79),
    "Company_Registration_No_2" character varying(12),
    "Proprietorship_2" character varying(41),
    "Country_Incorporated_2" character varying(25),
    "Proprietor_2_Address_1" character varying(255),
    "Proprietor_2_Address_2" character varying(177),
    "Proprietor_2_Address_3" character varying(108),
    "Proprietor_Name_3" character varying(38),
    "Company_Registration_No_3" character varying(7),
    "Proprietorship_3" character varying(41),
    "Country_Incorporated_3" character varying(22),
    "Proprietor_3_Address_1" character varying(120),
    "Proprietor_3_Address_2" character varying(78),
    "Proprietor_3_Address_3" character varying(4),
    "Proprietor_Name_4" character varying(36),
    "Company_Registration_No_4" character varying(4),
    "Proprietorship_4" character varying(41),
    "Country_Incorporated_4" character varying(22),
    "Proprietor_4_Address_1" character varying(85),
    "Proprietor_4_Address_2" character varying(78),
    "Proprietor_4_Address_3" character varying(4),
    "Date_Proprietor_Added" character varying(10),
    "Additional_Proprietor_Indicator" character varying(4),
    "Multiple_Address_Indicator" character varying(4)
);




--
-- Name: gb_land_registry_proprietor; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gb_land_registry_proprietor (
    id integer NOT NULL,
    "Title_Number" text,
    name_norm text,
    "Country_Incorporated" text,
    "Company_Registration_No" text,
    "Proprietor_Name" text,
    "Num" integer,
    "Address_1" text,
    "Address_2" text,
    "Address_3" text,
    "Proprietorship" text
);




--
-- Name: gb_land_registry_proprietor_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE gb_land_registry_proprietor_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: gg_greg; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE gg_greg (
    id integer NOT NULL,
    company_reg_number text,
    audit_exempt_indefinite text,
    alternative_name text,
    economic_activity_type text,
    registered_office_address text,
    audit_exempt_annual text,
    register text,
    company_type text,
    source_url text,
    company_status text,
    liability_type text,
    company_registered_date text,
    company_name text,
    company_classification text,
    waive_agms text,
    resident_agent_exempt text,
    registered_office_address_simple text,
    foundation_name text,
    foundation_registration_date text,
    foundation_status text,
    foundation_reg_number text,
    llp_registration_date text,
    principal_place_of_business text,
    llp_status text,
    llp_reg_number text,
    llp_name text,
    company_status_clean character varying,
    registered_date text,
    status text,
    name text,
    last_change text
);




--
-- Name: gg_greg_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE gg_greg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: int_interpol_wanted; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE int_interpol_wanted (
    id integer NOT NULL,
    url text,
    name text,
    colour_of_hair text,
    present_family_name text,
    sex text,
    charges text,
    reason text,
    date_of_birth text,
    colour_of_eyes text,
    forename text,
    language_spoken text,
    nationality text,
    place_of_birth text,
    height text,
    weight text
);




--
-- Name: int_interpol_wanted_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE int_interpol_wanted_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: kg_minjust; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE kg_minjust (
    id integer NOT NULL,
    public_id integer,
    html text,
    has_record boolean,
    source_url text,
    full_name_local_lang text,
    full_name_official_lang text,
    short_name_local_lang text,
    short_name_official_lang text,
    company_type text,
    foreign_investors text,
    registration_number text,
    industry_classifier text,
    vat_number text,
    area text,
    region text,
    city text,
    part_of_the_city text,
    street text,
    street_number text,
    apartment_number text,
    phone text,
    fax text,
    email text,
    state_registration text,
    registration_date text,
    initial_date text,
    creation_method text,
    ownership text,
    name_of_head text,
    main_activity text,
    activity_code text,
    number_founders_individual text,
    number_founders_legal_entities text,
    number_founders text
);




--
-- Name: kg_minjust_founder; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE kg_minjust_founder (
    id integer NOT NULL,
    source_url text,
    company text,
    public_id integer,
    founder text
);




--
-- Name: kg_minjust_founder_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE kg_minjust_founder_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: kg_minjust_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE kg_minjust_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: md_blacklist; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE md_blacklist (
    id integer NOT NULL,
    name text,
    name_norm text,
    date_end text,
    date_start text,
    comments text,
    address text,
    cause text
);




--
-- Name: md_blacklist_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE md_blacklist_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: md_company; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE md_company (
    id integer NOT NULL,
    idno_cod_fiscal text,
    name text,
    statutul text,
    forma_org_jurid text,
    company_id integer,
    adresa text,
    genuri_de_activitate_nelicentiate text,
    name_norm text,
    name_simple text,
    genuri_de_activitate_licentiate text,
    data_inregistrarii text
);




--
-- Name: md_company_director; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE md_company_director (
    id integer NOT NULL,
    company_id integer,
    name_simple text,
    name text,
    name_norm text
);




--
-- Name: md_company_director_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE md_company_director_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: md_company_founder; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE md_company_founder (
    id integer NOT NULL,
    company_id integer,
    name_simple text,
    name text,
    name_norm text
);




--
-- Name: md_company_founder_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE md_company_founder_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: md_company_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE md_company_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: md_contract_awards; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE md_contract_awards (
    id integer NOT NULL,
    cpv3 text,
    data_documentului text,
    name text,
    obiectul_achizitiei text,
    raion text,
    numarul_procedurii text,
    tipul_documentului text,
    autoritatea_contractanta text,
    suma text,
    name_norm text,
    tipul_contractului text,
    numar_de_intrare text,
    numarul_de_participanti text,
    data_intrarii text,
    numarul_documentului text
);




--
-- Name: md_contract_awards_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE md_contract_awards_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: md_court_cases; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE md_court_cases (
    id integer NOT NULL,
    record_id text,
    case_id text,
    topic text,
    url text,
    date text,
    parties text,
    court text,
    type text
);




--
-- Name: md_court_cases_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE md_court_cases_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pa_companies_company; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pa_companies_company (
    id integer NOT NULL,
    source_file text,
    datos_del_diario_asiento text,
    datos_de_microfilmacion_imagen text,
    status_de_la_prenda_text text,
    no_documento text,
    moneda text,
    agente_residente text,
    domicilio text,
    folio text,
    datos_del_diario_tomo text,
    fecha_de_pago text,
    status text,
    notaria text,
    datos_de_microfilmacion_rollo text,
    asiento text,
    monto_de_capital text,
    fecha_de_registro text,
    no_de_ficha text,
    nombre_de_la_sociedad text,
    tomo text,
    duracion text,
    fecha_de_escritura text,
    boleta text,
    representante_legal text,
    provincia text,
    fecha text,
    capital text,
    datos_de_la_escritura_notaria text,
    numero text,
    fecha_micro text,
    detalle text,
    tipo_acta text,
    disolucion_quiebra_o_fusion_imagen text,
    disolucion_quiebra_o_fusion_rollo text,
    no_de_escritura text,
    provincia_notaria_name text,
    notaria_name text,
    status_de_la_prenda text,
    datos_del_oficio text,
    name_norm text
);




--
-- Name: pa_companies_company_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pa_companies_company_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pa_companies_person; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pa_companies_person (
    id integer NOT NULL,
    nombre text,
    source_file text,
    role text,
    nombre_norm text
);




--
-- Name: pa_companies_person_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pa_companies_person_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pa_panama_companies_company; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pa_panama_companies_company (
    id integer NOT NULL,
    name_norm text
);




--
-- Name: pa_panama_companies_company_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pa_panama_companies_company_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pep_address; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pep_address (
    id integer NOT NULL,
    uid text,
    text text,
    region text,
    postal_code text,
    address1 text,
    country text,
    address2 text,
    name text,
    city text
);




--
-- Name: pep_address_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pep_address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pep_entity; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pep_entity (
    id integer NOT NULL,
    function text,
    publisher text,
    program text,
    uid text,
    publisher_url text,
    updated_at text,
    summary text,
    source text,
    json text,
    date_of_birth text,
    country_of_birth text,
    place_of_birth text,
    source_id text,
    source_url text,
    type text,
    name text,
    last_name text,
    first_name text,
    middle_name text,
    gender text,
    nationality text,
    second_name text
);




--
-- Name: pep_entity_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pep_entity_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pep_identity; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pep_identity (
    id integer NOT NULL,
    country text,
    type text,
    number text,
    name text,
    uid text,
    text text
);




--
-- Name: pep_identity_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pep_identity_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pep_other_name; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pep_other_name (
    id integer NOT NULL,
    first_name text,
    last_name text,
    uid text,
    other_name text,
    quality text,
    second_name text,
    name text,
    middle_name text,
    gender text,
    type text
);




--
-- Name: pep_other_name_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pep_other_name_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: pep_spindle_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pep_spindle_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: pep_xref; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE pep_xref (
    id integer NOT NULL,
    uid text,
    source_id text,
    alias text,
    alias_norm text,
    name text
);




--
-- Name: pep_xref_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE pep_xref_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ref_country_codes; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ref_country_codes (
    id integer NOT NULL,
    label text,
    source text,
    code text,
    label_orig text
);




--
-- Name: ref_country_codes_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ref_country_codes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ref_currency_rates_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ref_currency_rates_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: reg_person_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE reg_person_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: reg_representative_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE reg_representative_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--
-- Name: tj_andoz; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE tj_andoz (
    id integer NOT NULL,
    tin text,
    status_code integer,
    ajax text,
    has_record boolean,
    source_url text,
    name text,
    authority text,
    base_full_name text,
    base_short_name text,
    num_cross_unique text,
    num_tin text,
    num_statistical text,
    date_registration text,
    date_entry text,
    type_legal_entity text,
    addr_text text,
    addr_number text,
    addr_room text,
    founder_legal_entity text,
    founder_individual text,
    head_individual text,
    capital_amount text,
    activity_main text,
    status_current text
);




--
-- Name: tj_andoz_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE tj_andoz_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ua_entrepreneurs; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ua_entrepreneurs (
    id integer NOT NULL,
    source_file text,
    main_activity text,
    name text,
    source_row integer,
    short_name_norm text,
    head_name_norm text,
    state text,
    place_of_residence text,
    name_norm text,
    identifier text
);




--
-- Name: ua_entrepreneurs_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ua_entrepreneurs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ua_legal_entity; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ua_legal_entity (
    id integer NOT NULL,
    source_file text,
    main_activity text,
    head_name text,
    short_name text,
    source_row integer,
    short_name_norm text,
    edrpou text,
    state text,
    location text,
    name_norm text,
    head_name_norm text,
    identifier text,
    name text
);




--
-- Name: ua_legal_entity_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ua_legal_entity_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_alllicenses; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_alllicenses (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" text,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: ug_alllicenses_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_alllicenses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_applications; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_applications (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: ug_applications_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_applications_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_border; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_border (
    id integer NOT NULL,
    source_title text,
    "FC_ID" integer,
    "OBJECTID" integer,
    layer_id integer,
    source_name text,
    source_url text,
    layer_name text
);




--
-- Name: ug_border_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_border_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_exploration_licenses; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_exploration_licenses (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: ug_exploration_licenses_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_exploration_licenses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_location_licenses; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_location_licenses (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: ug_location_licenses_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_location_licenses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_mining_leases; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_mining_leases (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: ug_mining_leases_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_mining_leases_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_protected_areas; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_protected_areas (
    id integer NOT NULL,
    source_name text,
    "COUNTRY" text,
    "METADATAID" integer,
    "DESIG_TYPE" text,
    "REP_M_AREA" integer,
    "INT_CRIT" text,
    "MARINE" text,
    "MANG_PLAN" text,
    "WDPAID" integer,
    source_title text,
    "GIS_M_AREA" integer,
    "GOV_TYPE" text,
    "SUB_LOC" text,
    "GIS_AREA" double precision,
    "MANG_AUTH" text,
    "STATUS" text,
    "IUCN_CAT" text,
    "OBJECTID" integer,
    "REP_AREA" double precision,
    layer_id integer,
    "WDPA_PID" integer,
    source_url text,
    "ORIG_NAME" text,
    layer_name text,
    "STATUS_YR" integer,
    "NAME" text,
    "DESIG_ENG" text,
    "DESIG" text,
    "SHAPE_LENG" double precision
);




--
-- Name: ug_protected_areas_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_protected_areas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: ug_retention_licenses; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE ug_retention_licenses (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" integer,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: ug_retention_licenses_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE ug_retention_licenses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: us_corpwatch_companies; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_corpwatch_companies (
    row_id integer NOT NULL,
    cw_id integer NOT NULL,
    cik integer,
    company_name character varying(1480) NOT NULL,
    source_type character varying(130) NOT NULL,
    source_id integer NOT NULL
);




--
-- Name: us_corpwatch_company_info; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_corpwatch_company_info (
    row_id integer NOT NULL,
    cw_id integer NOT NULL,
    most_recent integer NOT NULL,
    year integer NOT NULL,
    cik integer,
    irs_number integer,
    best_location_id integer NOT NULL,
    sic_code integer,
    industry_name character varying(610),
    sic_sector integer,
    sector_name character varying(580),
    source_type character varying(130) NOT NULL,
    source_id integer NOT NULL,
    num_parents integer NOT NULL,
    num_children integer NOT NULL,
    top_parent_id integer NOT NULL,
    company_name character varying(1480) NOT NULL,
    max_year integer NOT NULL,
    min_year integer NOT NULL,
    no_sic integer NOT NULL
);




--
-- Name: us_corpwatch_company_locations; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_corpwatch_company_locations (
    location_id integer NOT NULL,
    cw_id integer,
    date date,
    type character varying(150) NOT NULL,
    raw_address character varying(1280),
    street_1 character varying(410),
    street_2 character varying(321000),
    city character varying(16573),
    state character varying(270),
    postal_code character varying(200),
    country character varying(70),
    country_code character varying(50),
    subdiv_code character varying(100),
    min_year character varying(40),
    max_year character varying(40),
    most_recent integer
);




--
-- Name: us_corpwatch_company_relations; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_corpwatch_company_relations (
    relation_id integer NOT NULL,
    source_cw_id integer NOT NULL,
    target_cw_id integer NOT NULL,
    relation_type character varying(320),
    relation_origin character varying(130) NOT NULL,
    origin_id integer NOT NULL,
    year integer NOT NULL
);




--
-- Name: us_corpwatch_relationships; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_corpwatch_relationships (
    relationship_id integer NOT NULL,
    company_name character varying(1000) NOT NULL,
    location character varying(1000),
    filing_id integer NOT NULL,
    country_code character varying(4),
    subdiv_code character varying(4),
    clean_company character varying(1000) NOT NULL,
    cik integer,
    ignore_record integer NOT NULL,
    parse_method character varying(1000) NOT NULL,
    hierarchy integer NOT NULL,
    percent character varying(320),
    parent_cw_id integer NOT NULL,
    cw_id integer NOT NULL,
    filer_cik integer NOT NULL,
    year integer NOT NULL,
    quarter integer NOT NULL
);




--
-- Name: us_fl_dor_nal; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_fl_dor_nal (
    id integer NOT NULL,
    source_file text,
    "EXMPT_33" text,
    "SPC_CIR_CD" text,
    "JV_CHNG_CD" text,
    "JV" text,
    "CONST_CLASS" text,
    "SPEC_FEAT_VAL" text,
    "ACT_YR_BLT" text,
    "SALE_YR2" text,
    "SALE_YR1" text,
    "TOT_LVG_AREA" text,
    "JV_CONSRV_LND" text,
    "EXMPT_19" text,
    "AV_HMSTD" text,
    "YR_VAL_TRNSF" text,
    "JV_NON_HMSTD_RESD" text,
    "PHY_CITY" text,
    "DOR_UC" text,
    "SPASS_CD" text,
    "PHY_ZIPCD" text,
    "DEL_VAL" text,
    "OR_PAGE1" text,
    "PHY_ADDR2" text,
    "OR_PAGE2" text,
    "PUBLIC_LND" text,
    "JV_CLASS_USE" text,
    own_name_norm text,
    "CO_APP_STAT" text,
    "PARCEL_ID_PRV_HMSTD" text,
    "ASS_TRNSFR_FG" text,
    "STATE_PAR_ID" text,
    "EXMPT_16" text,
    "VI_CD2" text,
    "VI_CD1" text,
    "QUAL_CD2" text,
    "QUAL_CD1" text,
    "PAR_SPLT" text,
    "OWN_ZIPCD" text,
    "MULTI_PAR_SAL1" text,
    "MULTI_PAR_SAL2" text,
    "AV_NON_HMSTD_RESD" text,
    "CO_NO" text,
    "FIDU_STATE" text,
    "PREV_HMSTD_OWN" text,
    "EXMPT_21" text,
    "FIDU_CD" text,
    "FIDU_ADDR2" text,
    "FIDU_ADDR1" text,
    "FILE_T" text,
    "FIDU_ZIPCD" text,
    "EXMPT_18" text,
    "ASS_DIF_TRNS" text,
    "EXMPT_22" text,
    "ALT_KEY" text,
    "EXMPT_10" text,
    "EXMPT_11" text,
    "EXMPT_12" text,
    "EXMPT_13" text,
    "EXMPT_14" text,
    "EXMPT_15" text,
    "TV_NSD" text,
    "EXMPT_17" text,
    "EFF_YR_BLT" text,
    "AV_NSD" text,
    "EXMPT_29" text,
    "PA_UC" text,
    "EXMPT_28" text,
    "EXMPT_81" text,
    "NO_BULDNG" text,
    "SALE_PRC2" text,
    "SALE_PRC1" text,
    "PARCEL_ID" text,
    "JV_RESD_NON_RESD" text,
    "PHY_ADDR1" text,
    "OWN_ADDR2" text,
    "OWN_ADDR1" text,
    "EXMPT_08" text,
    "OWN_CITY" text,
    "EXMPT_03" text,
    "ATV_STRT" text,
    "EXMPT_01" text,
    "EXMPT_07" text,
    "EXMPT_06" text,
    "EXMPT_05" text,
    "EXMPT_04" text,
    "NBRHD_CD" text,
    "BAS_STRT" text,
    "AV_RESD_NON_RESD" text,
    "MP_ID" text,
    "OWN_NAME" text,
    "FIDU_NAME" text,
    "SEQ_NO" text,
    "JV_HIST_SIGNF" text,
    "DISTR_CD" text,
    "SPC_CIR_TXT" text,
    "MKT_AR" text,
    "APP_STAT" text,
    "SEC" text,
    "EXMPT_38" text,
    "EXMPT_39" text,
    "EXMPT_36" text,
    "EXMPT_37" text,
    "EXMPT_34" text,
    "EXMPT_35" text,
    "EXMPT_32" text,
    "SALE_MO1" text,
    "SALE_MO2" text,
    "EXMPT_31" text,
    "JV_HMSTD" text,
    "S_LEGAL" text,
    "NO_LND_UNTS" text,
    "RS_ID" text,
    source_row integer,
    "CLERK_NO1" text,
    "CLERK_NO2" text,
    "OR_BOOK2" text,
    "OR_BOOK1" text,
    "EXMPT_80" text,
    "LND_UNTS_CD" text,
    "FIDU_CITY" text,
    "EXMPT_30" text,
    "CONO_PRV_HM" text,
    "JV_CHNG" text,
    "EXMPT_20" text,
    "EXMPT_23" text,
    "GRP_NO" text,
    "EXMPT_25" text,
    "EXMPT_24" text,
    "EXMPT_27" text,
    "EXMPT_26" text,
    "SAL_CHNG_CD1" text,
    "NCONST_VAL" text,
    "SAL_CHNG_CD2" text,
    "OWN_STATE" text,
    "IMP_QUAL" text,
    "RNG" text,
    "TWN" text,
    "TV_SD" text,
    "JV_WRKNG_WTRFNT" text,
    "AV_CLASS_USE" text,
    "LND_VAL" text,
    "ASMNT_YR" text,
    "EXMPT_09" text,
    "JV_HIST_COM_PROP" text,
    "CENSUS_BK" text,
    "NO_RES_UNTS" text,
    "AV_HIST_SIGNF" text,
    "AV_CONSRV_LND" text,
    "DT_LAST_INSPT" text,
    "AV_WRKNG_WTRFNT" text,
    "OWN_STATE_DOM" text,
    "EXMPT_02" text,
    "AV_SD" text,
    "TAX_AUTH_CD" text,
    "AV_H2O_RECHRGE" text,
    "LND_SQFOOT" text,
    "AV_HIST_COM_PROP" text,
    "SPC_CIR_YR" text,
    "JV_H2O_RECHRGE" text,
    "EXMPT_40" text,
    "DISTR_YR" text,
    fidu_name_norm text
);




--
-- Name: us_fl_dor_nal_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE us_fl_dor_nal_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: us_fl_dor_sdf; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_fl_dor_sdf (
    id integer NOT NULL,
    source_file text,
    "NBRHD_CD" text,
    "OR_PAGE" text,
    "ASMNT_YR" text,
    "VI_CD" text,
    "SALE_MO" text,
    "MKT_AR" text,
    "CENSUS_BK" text,
    "DOR_UC" text,
    "SALE_PRC" text,
    "RS_ID" text,
    "OR_BOOK" text,
    "SALE_ID_CD" text,
    "SALE_YR" text,
    source_row integer,
    "MULTI_PAR_SAL" text,
    "SAL_CHG_CD" text,
    "PARCEL_ID" text,
    "CLERK_NO" text,
    "CO_NO" text,
    "GRP_NO" text,
    "MP_ID" text,
    "STATE_PARCEL_ID" text,
    "ATV_STRT" text,
    "QUAL_CD" text,
    fidu_name_norm text,
    own_name_norm text
);




--
-- Name: us_fl_dor_sdf_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE us_fl_dor_sdf_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: us_fl_dor_tpp; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE us_fl_dor_tpp (
    id integer NOT NULL
);




--
-- Name: us_fl_dor_tpp_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE us_fl_dor_tpp_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: vg_icijoffshore_countries; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE vg_icijoffshore_countries (
    country_id integer NOT NULL,
    country_code character varying(2) NOT NULL,
    country_name character varying(48) NOT NULL
);




--
-- Name: vg_icijoffshore_edges; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE vg_icijoffshore_edges (
    "Unique_ID" integer NOT NULL,
    "Entity_ID1" integer NOT NULL,
    "Entity_ID2" integer NOT NULL,
    description_ character varying(29) NOT NULL,
    date_from date,
    date_to date,
    direction integer NOT NULL,
    "chinesePos" character varying(8) NOT NULL,
    "linkType" character varying(2) NOT NULL
);




--
-- Name: vg_icijoffshore_gen_address; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE vg_icijoffshore_gen_address (
    entity_id integer,
    address_id integer,
    address character varying(10000)
);




--
-- Name: vg_icijoffshore_gen_officers; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE vg_icijoffshore_gen_officers (
    officer_id integer,
    officer_name character varying(1000),
    officer_subtype character varying(1000),
    officer_status character varying(1000),
    officer_type character varying(1000),
    officer_tax_status character varying(1000),
    officer_inc_date date,
    officer_dorm_date date,
    officer_juris character varying(1000),
    officer_role_description character varying(1000),
    officer_role_type character varying(1000),
    officer_role_from date,
    officer_role_to date,
    entity_id integer,
    entity_name character varying(1000),
    entity_subtype character varying(1000),
    entity_status character varying(1000),
    entity_type character varying(1000),
    entity_tax_status character varying(1000),
    entity_inc_date date,
    entity_dorm_date date,
    entity_juris character varying(1000),
    officer_address character varying(1000),
    entity_address character varying(1000),
    entity_juris_code text,
    entity_name_norm text,
    officer_name_norm text
);




--
-- Name: vg_icijoffshore_node_countries; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE vg_icijoffshore_node_countries (
    "NODEID1" integer NOT NULL,
    country_code character varying(2) NOT NULL,
    country_name character varying(33) NOT NULL,
    country_id integer
);




--
-- Name: vg_icijoffshore_nodes; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE vg_icijoffshore_nodes (
    "Unique_ID" integer NOT NULL,
    subtypes_ character varying(23) NOT NULL,
    "Description_" character varying(221),
    "searchField_" character varying(305),
    status character varying(4),
    desc_status character varying(37),
    type character varying(5),
    desc_company_type character varying(46),
    inc_dat date,
    dorm_dat date,
    juris character varying(5),
    desc_jurisdiction character varying(24),
    "completeAddresses" character varying(269),
    "agencyID" integer,
    tax_stat character varying(5),
    tax_stat_description character varying(53)
);




--
-- Name: zm_alllicenses; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_alllicenses (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_alllicenses_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_alllicenses_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_amethyst_restricted_areas; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_amethyst_restricted_areas (
    id integer NOT NULL,
    source_title text,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_url text,
    layer_name text,
    source_name text,
    "RESTRICTEDAREATYPE" text
);




--
-- Name: zm_amethyst_restricted_areas_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_amethyst_restricted_areas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_applications; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_applications (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_applications_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_applications_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_artisinal_mining_rights; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_artisinal_mining_rights (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_artisinal_mining_rights_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_artisinal_mining_rights_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_boundary; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_boundary (
    id integer NOT NULL,
    source_title text,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_name text,
    source_url text,
    layer_name text
);




--
-- Name: zm_boundary_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_boundary_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_emerald_restricted_areas; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_emerald_restricted_areas (
    id integer NOT NULL,
    source_title text,
    "NAME" text,
    "OBJECTID" integer,
    layer_id integer,
    source_url text,
    layer_name text,
    source_name text,
    "RESTRICTEDAREATYPE" text
);




--
-- Name: zm_emerald_restricted_areas_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_emerald_restricted_areas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_flexicadastre; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_flexicadastre (
    id integer NOT NULL
);




--
-- Name: zm_flexicadastre_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_flexicadastre_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_large_scale_gemstone_licences; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_large_scale_gemstone_licences (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_large_scale_gemstone_licences_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_large_scale_gemstone_licences_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_large_scale_mining_licences; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_large_scale_mining_licences (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_large_scale_mining_licences_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_large_scale_mining_licences_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_mineral_processing_licences; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_mineral_processing_licences (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_mineral_processing_licences_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_mineral_processing_licences_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_national_parks; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_national_parks (
    id integer NOT NULL,
    source_title text,
    layer_name text,
    "OBJECTID" integer,
    layer_id integer,
    source_name text,
    "AREANAME" text,
    source_url text
);




--
-- Name: zm_national_parks_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_national_parks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_prospecting_licences; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_prospecting_licences (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_prospecting_licences_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_prospecting_licences_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_prospecting_permits; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_prospecting_permits (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_prospecting_permits_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_prospecting_permits_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_small_scale_gemstone_licences; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_small_scale_gemstone_licences (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_small_scale_gemstone_licences_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_small_scale_gemstone_licences_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zm_small_scale_mining_licences; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zm_small_scale_mining_licences (
    id integer NOT NULL,
    "DTEGRANTED" text,
    "CODE" text,
    source_name text,
    "TYPECODE" text,
    "APPLNO" text,
    "OLDCODE" text,
    "COMMODITIES" text,
    "PART" text,
    "LEN" integer,
    "STATUSGRP" text,
    "GROUP5" text,
    source_title text,
    "AREA" integer,
    "AREAUNIT" text,
    "RENEWAL" text,
    "DTEPEGGED" text,
    "INTEREST" text,
    "GROUP2" text,
    "COMMODITIESCD" text,
    "STATUS" text,
    "OBJECTID" integer,
    layer_id integer,
    "MAPREF" text,
    source_url text,
    layer_name text,
    "DTERENEWAL" text,
    "JURISDIC" text,
    "ARCHIVED" text,
    "RESPOFFICE" text,
    "NAME" text,
    "ACCCODE" text,
    "REGION" text,
    "COMMENTS" text,
    "TYPEGROUP" text,
    "GROUP4" text,
    "GROUP1" text,
    "GROUP3" text,
    "PARTIES" text,
    "DTEEXPIRES" text,
    "AREAVALUE" double precision,
    "TYPE" text,
    "DTEAPPLIED" text
);




--
-- Name: zm_small_scale_mining_licences_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zm_small_scale_mining_licences_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;




--

--




--
-- Name: zz_every_politician; Type: TABLE; Schema: public; Owner: alexandra; Tablespace: 
--

CREATE TABLE zz_every_politician (
    id integer NOT NULL,
    source_url text,
    period_name text,
    area_id text,
    twitter text,
    legislature_sources_directory text,
    country_code text,
    legislature_sha text,
    period_slug text,
    period_csv text,
    "group" text,
    area text,
    image text,
    period_end_date text,
    country_slug text,
    country_name text,
    legislature_name text,
    start_date text,
    end_date text,
    person_id text,
    facebook text,
    legislature_lastmod text,
    period_start_date text,
    term text,
    name text,
    legislature_popolo text,
    gender text,
    legislature_slug text,
    email text,
    chamber text,
    legislature_person_count integer,
    period_id text,
    country_country text,
    group_id text
);




--
-- Name: zz_every_politician_id_seq; Type: SEQUENCE; Schema: public; Owner: alexandra
--

CREATE SEQUENCE zz_every_politician_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- PostgreSQL database dump complete
--

