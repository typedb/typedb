/*
 * Copyright (C) 3030 Grakn Labs
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

package grakn.core.traversal;

import grakn.core.common.iterator.ResourceIterator;
import grakn.core.common.parameters.Arguments;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.rocks.RocksGrakn;
import grakn.core.rocks.RocksSession;
import grakn.core.rocks.RocksTransaction;
import grakn.core.test.integration.util.Util;
import graql.lang.query.GraqlDefine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static grakn.core.common.parameters.Arguments.Session.Type.DATA;
import static grakn.core.common.parameters.Arguments.Transaction.Type.READ;
import static grakn.core.common.parameters.Arguments.Transaction.Type.WRITE;
import static graql.lang.Graql.parseQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TraversalTest3 {
    private static Path directory = Paths.get(System.getProperty("user.dir")).resolve("traversal-test-3");
    private static String database = "traversal-test-3";

    private static RocksGrakn grakn;
    private static RocksSession session;

    @BeforeClass
    public static void before() throws IOException {
        Util.resetDirectory(directory);
        grakn = RocksGrakn.open(directory);
        grakn.databases().create(database);

        try (RocksSession session = grakn.session(database, Arguments.Session.Type.SCHEMA)) {
            try (RocksTransaction transaction = session.transaction(WRITE)) {
                final GraqlDefine query = parseQuery(schema());
                transaction.query().define(query);
                transaction.commit();
            }
        }

        session = grakn.session(database, DATA);
    }

    @AfterClass
    public static void after() {
        session.close();
        grakn.close();
    }

    @Test
    public void traversal_opencti() {
        try (RocksTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(parseQuery(
                    "insert $entity isa User, " +
                            "has internal_id 'ui1', " +
                            "has standard_id 'us1', " +
                            "has user_email 'admin@opencti.io';"
            ));
            transaction.query().insert(parseQuery(
                    "insert $entity isa Token, " +
                            "has internal_id 'ti1', " +
                            "has standard_id 'ts1', " +
                            "has uuid 'tu1', " +
                            "has name 'Default', " +
                            "has revoked false;"
            ));
            transaction.commit();
        }

        try (RocksTransaction transaction = session.transaction(WRITE)) {
            transaction.query().insert(
                    parseQuery(
                            "match " +
                                    "$from has internal_id 'ui1'; " +
                                    "$to has internal_id 'ti1'; " +
                                    "insert " +
                                    "$rel(authorized-by_from: $from, authorized-by_to: $to) isa authorized-by, " +
                                    "has internal_id 'ai1', " +
                                    "has standard_id 'as1', " +
                                    "has entity_type 'authorized-by';"
                    )
            ).toList();
            transaction.commit();
        }

        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<ConceptMap> answers = transaction.query().match(parseQuery(
                    "match $from has internal_id 'ui1'; "
            ).asMatch());
            assertTrue(answers.hasNext());

            answers = transaction.query().match(parseQuery(
                    "match $to has internal_id 'ti1';"
            ).asMatch());
            assertTrue(answers.hasNext());

            answers = transaction.query().match(parseQuery(
                    "match $from has internal_id 'ui1'; $to has internal_id 'ti1';"
            ).asMatch());
            assertTrue(answers.hasNext());
        }

        try (RocksTransaction transaction = session.transaction(READ)) {
            ResourceIterator<ConceptMap> tokens = transaction.query().match(parseQuery(
                    "match $entity isa Token; $entity has internal_id 'ti1';"
            ).asMatch());
            assertEquals(1, tokens.toList().size());

            ResourceIterator<ConceptMap> users = transaction.query().match(parseQuery(
                    "match $entity isa User; $entity has standard_id 'us1';"
            ).asMatch());
            assertEquals(1, users.toList().size());

            ResourceIterator<ConceptMap> authorisations = transaction.query().match(parseQuery(
                    "match $rel isa authorized-by;"
            ).asMatch());
            assertEquals(1, authorisations.toList().size());
        }
    }

    @Test
    public void test_planning_error() {
        String insert1 = "insert\n" +
                "   $entity isa Label, " +
                "       has value \"identity\", " +
                "       has color \"#be70e8\", " +
                "       has internal_id \"80f3ac8a-7860-4da7-a54c-72060797ea8e\", " +
                "       has standard_id \"label--355f76bb-be36-58dd-bdc9-90a75529df85\", " +
                "       has entity_type \"Label\", has spec_version \"2.1\", " +
                "       has created_at 2020-12-12T01:35:22.770, " +
                "       has i_created_at_day \"2020-12-12\", " +
                "       has i_created_at_month \"2020-12\", " +
                "       has i_created_at_year \"2020\", " +
                "       has updated_at 2020-12-12T01:35:22.770, " +
                "       has created 2020-12-12T01:35:22.770, " +
                "       has modified 2020-12-12T01:35:22.770, " +
                "       has i_created_at_day \"2020-12-12\", " +
                "       has i_created_at_month \"2020-12\", " +
                "       has i_created_at_year \"2020\";";

        String insert2 = "insert\n" +
                "   $entity isa Organization," +
                "       has name \"ANSSI\", " +
                "       has description \"\", " +
                "       has confidence 0, " +
                "       has revoked false, " +
                "       has lang \"en\", " +
                "       has x_opencti_organization_type \"CSIRT\", " +
                "       has created 2020-02-23T23:40:53.575, " +
                "       has modified 2020-02-27T08:45:39.351, " +
                "       has identity_class \"organization\", " +
                "       has internal_id \"0c5eee5b-0776-476c-a560-ba3ca0c68a9e\", " +
                "       has standard_id \"identity--3256d0a9-31a5-5de2-88df-04b76ca1a85d\", " +
                "       has entity_type \"Organization\", " +
                "       has x_opencti_stix_ids \"identity--7b82b010-b1c0-4dae-981f-7756374a17df\", " +
                "       has spec_version \"2.1\", " +
                "       has created_at 2020-12-12T01:35:22.879, " +
                "       has i_created_at_day \"2020-12-12\", " +
                "       has i_created_at_month \"2020-12\", " +
                "       has i_created_at_year \"2020\", " +
                "       has updated_at 2020-12-12T01:35:22.879, " +
                "       has i_aliases_ids \"aliases--e3a5d1ef-b3d3-5deb-bddb-1756a05daf81\", " +
                "       has i_created_at_day \"2020-12-12\", " +
                "       has i_created_at_month \"2020-12\", " +
                "       has i_created_at_year \"2020\";";
        String matchInsertStr = "match\n" +
                "   $from has internal_id \"0c5eee5b-0776-476c-a560-ba3ca0c68a9e\"; " +
                "   $to has internal_id \"80f3ac8a-7860-4da7-a54c-72060797ea8e\";\n" +
                "insert\n" +
                "   $rel(object-label_from: $from, object-label_to: $to) isa object-label, " +
                "       has internal_id \"7c177b09-7217-4214-a412-71be757090cd\", " +
                "       has standard_id \"relationship-meta--aba276f0-951e-47d5-9db7-d7ed3e2905b5\", " +
                "       has entity_type \"object-label\", " +
                "       has created_at 2020-12-12T01:35:22.881, " +
                "       has i_created_at_day \"2020-12-12\", " +
                "       has i_created_at_month \"2020-12\", " +
                "       has i_created_at_year \"2020\", " +
                "       has updated_at 2020-12-12T01:35:22.881, " +
                "       has i_created_at_day \"2020-12-12\"," +
                "       has i_created_at_month \"2020-12\", " +
                "       has i_created_at_year \"2020\"; ";
        System.out.println("about to start writing");
        try (RocksTransaction transaction = session.transaction(WRITE)) {
            System.out.println("start writing insert 1");
            transaction.query().insert(parseQuery(insert1));
            System.out.println("start committing insert 1");
            transaction.commit();
            System.out.println("committed insert 1");
        }

        System.out.println("start writing in second transaction");
        try (RocksTransaction transaction = session.transaction(WRITE)) {
            System.out.println("start writing insert 2");
            transaction.query().insert(parseQuery(insert2));
            System.out.println("start writing match insert");
            ResourceIterator<ConceptMap> answers = transaction.query().insert(parseQuery(matchInsertStr));
            System.out.println("start writing insert 1");
            assertTrue(answers.hasNext());
            System.out.println("got results from match insert");
            transaction.commit();
        }
    }

    private static String schema() {
        return "define\n" +
                "\n" +
                "  ## PROPERTIES\n" +
                "\n" +
                "  # Common\n" +
                "  internal_id sub attribute, value string;\n" +
                "  standard_id sub attribute, value string;\n" +
                "  entity_type sub attribute, value string;\n" +
                "  created_at sub attribute, value datetime;\n" +
                "  i_created_at_day sub attribute, value string;\n" +
                "  i_created_at_month sub attribute, value string;\n" +
                "  i_created_at_year sub attribute, value string;\n" +
                "  updated_at sub attribute, value datetime;\n" +
                "  name sub attribute, value string;\n" +
                "  description sub attribute, value string;\n" +
                "  x_opencti_graph_data sub attribute, value string;\n" +
                "  issuer sub attribute, value string;\n" +
                "  revoked sub attribute, value boolean;\n" +
                "\n" +
                "  # STIX Common\n" +
                "  spec_version sub attribute, value string;\n" +
                "  x_opencti_stix_ids sub attribute, value string;\n" +
                "  created sub attribute, value datetime;\n" +
                "  modified sub attribute, value datetime;\n" +
                "  confidence sub attribute, value long;\n" +
                "  lang sub attribute, value string;\n" +
                "\n" +
                "  # STIX General\n" +
                "  first_seen sub attribute, value datetime;\n" +
                "  i_first_seen_day sub attribute, value string;\n" +
                "  i_first_seen_month sub attribute, value string;\n" +
                "  i_first_seen_year sub attribute, value string;\n" +
                "  last_seen sub attribute, value datetime;\n" +
                "  i_last_seen_day sub attribute, value string;\n" +
                "  i_last_seen_month sub attribute, value string;\n" +
                "  i_last_seen_year sub attribute, value string;\n" +
                "\n" +
                "  # Internal Relationships\n" +
                "  grant sub attribute, value string;\n" +
                "\n" +
                "  # STIX Core Relationship\n" +
                "  relationship_type sub attribute, value string;\n" +
                "  start_time sub attribute, value datetime;\n" +
                "  i_start_time_day sub attribute, value string;\n" +
                "  i_start_time_month sub attribute, value string;\n" +
                "  i_start_time_year sub attribute, value string;\n" +
                "  stop_time sub attribute, value datetime;\n" +
                "  i_stop_time_day sub attribute, value string;\n" +
                "  i_stop_time_month sub attribute, value string;\n" +
                "  i_stop_time_year sub attribute, value string;\n" +
                "\n" +
                "  # STIX Sighting Relationship\n" +
                "  attribute_count sub attribute, value long;\n" +
                "  x_opencti_negative sub attribute, value boolean;\n" +
                "\n" +
                "  # Internal Entities\n" +
                "  platform_title sub attribute, value string;\n" +
                "  platform_language sub attribute, value string;\n" +
                "  platform_email sub attribute, value string;\n" +
                "  platform_url sub attribute, value string;\n" +
                "  title sub attribute, value string;\n" +
                "  timestamp sub attribute, value long;\n" +
                "  lastRun sub attribute, value string;\n" +
                "  uuid sub attribute, value string;\n" +
                "  duration sub attribute, value string;\n" +
                "  firstname sub attribute, value string;\n" +
                "  lastname sub attribute, value string;\n" +
                "  user_email sub attribute, value string;\n" +
                "  password sub attribute, value string;\n" +
                "  language sub attribute, value string;\n" +
                "  external sub attribute, value boolean;\n" +
                "  default_assignation sub attribute, value boolean;\n" +
                "  attribute_order sub attribute, value double;\n" +
                "  active sub attribute, value boolean;\n" +
                "  auto sub attribute, value boolean;\n" +
                "  connector_type sub attribute, value string;\n" +
                "  connector_scope sub attribute, value string;\n" +
                "  connector_state sub attribute, value string;\n" +
                "  connector_user_id sub attribute, value string;\n" +
                "  connector_state_reset sub attribute, value boolean;\n" +
                "  workspace_type sub attribute, value string;\n" +
                "  workspace_data sub attribute, value string;\n" +
                "\n" +
                "  # STIX Object\n" +
                "  value sub attribute, value string;\n" +
                "  url sub attribute, value string;\n" +
                "\n" +
                "  # STIX Meta Objects\n" +
                "  definition_type sub attribute, value string;\n" +
                "  definition sub attribute, value string;\n" +
                "  x_opencti_order  sub attribute, value long;\n" +
                "  x_opencti_color sub attribute, value string;\n" +
                "  color sub attribute, value string;\n" +
                "  source_name sub attribute, value string;\n" +
                "  hash sub attribute, value string;\n" +
                "  external_id sub attribute, value string;\n" +
                "  kill_chain_name sub attribute, value string;\n" +
                "  phase_name sub attribute, value string;\n" +
                "\n" +
                "  # STIX Core Objects\n" +
                "\n" +
                "  # STIX Domain Object\n" +
                "  aliases sub attribute, value string;\n" +
                "  i_aliases_ids sub attribute, value string;\n" +
                "  x_opencti_aliases sub attribute, value string;\n" +
                "  x_mitre_detection sub attribute, value string;\n" +
                "  x_mitre_platforms sub attribute, value string;\n" +
                "  x_mitre_permissions_required sub attribute, value string;\n" +
                "  x_mitre_id sub attribute, value string;\n" +
                "  contact_information sub attribute, value string;\n" +
                "  x_opencti_firstname sub attribute, value string;\n" +
                "  x_opencti_lastname sub attribute, value string;\n" +
                "  x_opencti_organization_type sub attribute, value string;\n" +
                "  x_opencti_reliability sub attribute, value string;\n" +
                "  latitude sub attribute, value double;\n" +
                "  longitude sub attribute, value double;\n" +
                "  precision sub attribute, value double;\n" +
                "  objective sub attribute, value string;\n" +
                "  hashes sub attribute, value string;\n" +
                "  pattern_type sub attribute, value string;\n" +
                "  pattern_version sub attribute, value string;\n" +
                "  pattern sub attribute, value string;\n" +
                "  indicator_types sub attribute, value string;\n" +
                "  valid_from sub attribute, value datetime;\n" +
                "  i_valid_from_day sub attribute, value string;\n" +
                "  i_valid_from_month sub attribute, value string;\n" +
                "  i_valid_from_year sub attribute, value string;\n" +
                "  valid_until sub attribute, value datetime;\n" +
                "  i_valid_until_day sub attribute, value string;\n" +
                "  i_valid_until_month sub attribute, value string;\n" +
                "  i_valid_until_year sub attribute, value string;\n" +
                "  x_opencti_score sub attribute, value long;\n" +
                "  x_opencti_detection sub attribute, value boolean;\n" +
                "  x_opencti_main_observable_type sub attribute, value string;\n" +
                "  infrastructure_types sub attribute, value string;\n" +
                "  goals sub attribute, value string;\n" +
                "  resource_level sub attribute, value string;\n" +
                "  primary_motivation sub attribute, value string;\n" +
                "  secondary_motivations sub attribute, value string;\n" +
                "  street_address sub attribute, value string;\n" +
                "  postal_code sub attribute, value string;\n" +
                "  malware_types sub attribute, value string;\n" +
                "  is_family sub attribute, value boolean;\n" +
                "  architecture_execution_envs sub attribute, value string;\n" +
                "  implementation_languages sub attribute, value string;\n" +
                "  capabilities sub attribute, value string;\n" +
                "  attribute_abstract  sub attribute, value string;\n" +
                "  content sub attribute, value string;\n" +
                "  authors sub attribute, value string;\n" +
                "  first_observed sub attribute, value datetime;\n" +
                "  last_observed sub attribute, value datetime;\n" +
                "  number_observed sub attribute, value long;\n" +
                "  explanation sub attribute, value string;\n" +
                "  opinion sub attribute, value string;\n" +
                "  report_types sub attribute, value string;\n" +
                "  x_opencti_report_status sub attribute, value long;\n" +
                "  published sub attribute, value datetime;\n" +
                "  i_published_day sub attribute, value string;\n" +
                "  i_published_month sub attribute, value string;\n" +
                "  i_published_year sub attribute, value string;\n" +
                "  threat_actor_types sub attribute, value string;\n" +
                "  sophistication sub attribute, value string;\n" +
                "  personal_motivations sub attribute, value string;\n" +
                "  roles sub attribute, value string;\n" +
                "  tool_types sub attribute, value string;\n" +
                "  tool_version sub attribute, value string;\n" +
                "  x_opencti_base_score sub attribute, value double;\n" +
                "  x_opencti_base_severity sub attribute, value string;\n" +
                "  x_opencti_attack_vector sub attribute, value string;\n" +
                "  x_opencti_integrity_impact sub attribute, value string;\n" +
                "  x_opencti_availability_impact sub attribute, value string;\n" +
                "  identity_class sub attribute, value string;\n" +
                "  x_opencti_location_type sub attribute, value string;\n" +
                "\n" +
                "  # STIX Cyber Observables\n" +
                "  x_opencti_description sub attribute, value string;\n" +
                "  mime_type sub attribute, value string;\n" +
                "  payload_bin sub attribute, value string;\n" +
                "  encryption_algorithm sub attribute, value string;\n" +
                "  decryption_key sub attribute, value string;\n" +
                "  number sub attribute, value long;\n" +
                "  rir sub attribute, value string;\n" +
                "  path sub attribute, value string;\n" +
                "  path_enc sub attribute, value string;\n" +
                "  ctime sub attribute, value datetime;\n" +
                "  mtime sub attribute, value datetime;\n" +
                "  atime sub attribute, value datetime;\n" +
                "  display_name sub attribute, value string;\n" +
                "  is_multipart sub attribute, value boolean;\n" +
                "  attribute_date sub attribute, value datetime;\n" +
                "  content_type sub attribute, value string;\n" +
                "  message_id sub attribute, value string;\n" +
                "  subject sub attribute, value string;\n" +
                "  received_lines sub attribute, value string;\n" +
                "  body sub attribute, value string;\n" +
                "  content_disposition sub attribute, value string;\n" +
                "  size sub attribute, value long;\n" +
                "  extensions sub attribute, value string;\n" +
                "  name_enc sub attribute, value string;\n" +
                "  magic_number_hex sub attribute, value string;\n" +
                "  start sub attribute, value datetime;\n" +
                "  end sub attribute, value datetime;\n" +
                "  is_active sub attribute, value boolean;\n" +
                "  protocols sub attribute, value string;\n" +
                "  src_port sub attribute, value long;\n" +
                "  dst_port sub attribute, value long;\n" +
                "  src_byte_count sub attribute, value long;\n" +
                "  dst_byte_count sub attribute, value long;\n" +
                "  src_packets sub attribute, value long;\n" +
                "  dst_packets sub attribute, value long;\n" +
                "  is_hidden sub attribute, value boolean;\n" +
                "  pid sub attribute, value long;\n" +
                "  created_time sub attribute, value datetime;\n" +
                "  cwd sub attribute, value string;\n" +
                "  command_line sub attribute, value string;\n" +
                "  environment_variables sub attribute, value string;\n" +
                "  cpe sub attribute, value string;\n" +
                "  swid sub attribute, value string;\n" +
                "  languages sub attribute, value string;\n" +
                "  vendor sub attribute, value string;\n" +
                "  version sub attribute, value string;\n" +
                "  user_id sub attribute, value string;\n" +
                "  credential sub attribute, value string;\n" +
                "  account_login sub attribute, value string;\n" +
                "  account_type sub attribute, value string;\n" +
                "  is_service_account sub attribute, value boolean;\n" +
                "  is_privileged sub attribute, value boolean;\n" +
                "  can_escalate_privs sub attribute, value boolean;\n" +
                "  is_disabled sub attribute, value boolean;\n" +
                "  account_created sub attribute, value datetime;\n" +
                "  account_expires sub attribute, value datetime;\n" +
                "  credential_last_changed sub attribute, value datetime;\n" +
                "  account_first_login sub attribute, value datetime;\n" +
                "  account_last_login sub attribute, value datetime;\n" +
                "  attribute_key sub attribute, value string;\n" +
                "  modified_time sub attribute, value datetime;\n" +
                "  number_of_subkeys sub attribute, value double;\n" +
                "  data sub attribute, value string;\n" +
                "  data_type sub attribute, value string;\n" +
                "  is_self_signed sub attribute, value boolean;\n" +
                "  serial_number sub attribute, value string;\n" +
                "  signature_algorithm sub attribute, value string;\n" +
                "  validity_not_before sub attribute, value datetime;\n" +
                "  validity_not_after sub attribute, value datetime;\n" +
                "  subject_public_key_algorithm sub attribute, value string;\n" +
                "  subject_public_key_modulus sub attribute, value string;\n" +
                "  subject_public_key_exponent sub attribute, value string;\n" +
                "  basic_constraints sub attribute, value string;\n" +
                "  name_constraints sub attribute, value string;\n" +
                "  policy_constraints sub attribute, value string;\n" +
                "  key_usage sub attribute, value string;\n" +
                "  extended_key_usage sub attribute, value string;\n" +
                "  subject_key_identifier sub attribute, value string;\n" +
                "  authority_key_identifier sub attribute, value string;\n" +
                "  subject_alternative_name sub attribute, value string;\n" +
                "  issuer_alternative_name sub attribute, value string;\n" +
                "  subject_directory_attributes sub attribute, value string;\n" +
                "  crl_distribution_points sub attribute, value string;\n" +
                "  inhibit_any_policy sub attribute, value string;\n" +
                "  private_key_usage_period_not_before sub attribute, value datetime;\n" +
                "  private_key_usage_period_not_after sub attribute, value datetime;\n" +
                "  certificate_policies sub attribute, value string;\n" +
                "  policy_mappings sub attribute, value string;\n" +
                "\n" +
                "  ## RELATIONS\n" +
                "\n" +
                "  basic-relationship sub relation,\n" +
                "    abstract,\n" +
                "    owns internal_id @key,\n" +
                "    owns standard_id @key,\n" +
                "    owns entity_type,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at;\n" +
                "\n" +
                "  # Internal relations\n" +
                "  internal-relationship sub basic-relationship,\n" +
                "    abstract;\n" +
                "\n" +
                "  authorized-by sub internal-relationship,\n" +
                "    relates authorized-by_from,\n" +
                "    relates authorized-by_to;\n" +
                "\n" +
                "  accesses-to sub internal-relationship,\n" +
                "    relates accesses-to_from,\n" +
                "    relates accesses-to_to;\n" +
                "\n" +
                "  migrates sub internal-relationship,\n" +
                "    relates migrates_from,\n" +
                "    relates migrates_to;\n" +
                "\n" +
                "  member-of sub internal-relationship,\n" +
                "    relates member-of_from,\n" +
                "    relates member-of_to;\n" +
                "\n" +
                "  allowed-by sub internal-relationship,\n" +
                "    owns grant,\n" +
                "    relates allowed-by_from,\n" +
                "    relates allowed-by_to;\n" +
                "\n" +
                "  has-role sub internal-relationship,\n" +
                "    relates has-role_from,\n" +
                "    relates has-role_to;\n" +
                "\n" +
                "  has-capability sub internal-relationship,\n" +
                "    relates has-capability_from,\n" +
                "    relates has-capability_to;\n" +
                "\n" +
                "  # STIX relations\n" +
                "  stix-relationship sub basic-relationship,\n" +
                "    abstract,\n" +
                "    owns x_opencti_stix_ids,\n" +
                "    owns spec_version,\n" +
                "    owns revoked,\n" +
                "    owns confidence,\n" +
                "    owns lang,\n" +
                "    owns created,\n" +
                "    owns modified,\n" +
                "    plays object:object_to;\n" +
                "\n" +
                "  # STIX Core Relationships\n" +
                "  stix-core-relationship sub stix-relationship,\n" +
                "    abstract,\n" +
                "    owns relationship_type,\n" +
                "    owns description,\n" +
                "    owns start_time,\n" +
                "    owns i_start_time_day,\n" +
                "    owns i_start_time_month,\n" +
                "    owns i_start_time_year,\n" +
                "    owns stop_time,\n" +
                "    owns i_stop_time_day,\n" +
                "    owns i_stop_time_month,\n" +
                "    owns i_stop_time_year,\n" +
                "    plays created-by:created-by_from,\n" +
                "    plays object-marking:object-marking_from,\n" +
                "    plays object-label:object-label_from,\n" +
                "    plays external-reference:external-reference_from,\n" +
                "    plays duplicate-of:duplicate-of_from,\n" +
                "    plays derived-from:derived-from_from,\n" +
                "    plays related-to:related-to_from,\n" +
                "    plays duplicate-of:duplicate-of_to,\n" +
                "    plays derived-from:derived-from_to,\n" +
                "    plays related-to:related-to_to;\n" +
                "\n" +
                "  delivers sub stix-core-relationship,\n" +
                "    relates delivers_from,\n" +
                "    relates delivers_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  targets sub stix-core-relationship,\n" +
                "    relates targets_from,\n" +
                "    relates targets_to,\n" +
                "    plays located-at:located-at_from;\n" +
                "\n" +
                "  uses sub stix-core-relationship,\n" +
                "    relates uses_from,\n" +
                "    relates uses_to,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  attributed-to sub stix-core-relationship,\n" +
                "    relates attributed-to_from,\n" +
                "    relates attributed-to_to;\n" +
                "\n" +
                "  compromises sub stix-core-relationship,\n" +
                "    relates compromises_from,\n" +
                "    relates compromises_to;\n" +
                "\n" +
                "  originates-from sub stix-core-relationship,\n" +
                "    relates originates-from_from,\n" +
                "    relates originates-from_to;\n" +
                "\n" +
                "  investigates sub stix-core-relationship,\n" +
                "    relates investigates_from,\n" +
                "    relates investigates_to;\n" +
                "\n" +
                "  mitigates sub stix-core-relationship,\n" +
                "    relates mitigates_from,\n" +
                "    relates mitigates_to;\n" +
                "\n" +
                "  located-at sub stix-core-relationship,\n" +
                "    relates located-at_from,\n" +
                "    relates located-at_to;\n" +
                "\n" +
                "  indicates sub stix-core-relationship,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    relates indicates_from,\n" +
                "    relates indicates_to;\n" +
                "\n" +
                "  based-on sub stix-core-relationship,\n" +
                "    relates based-on_from,\n" +
                "    relates based-on_to;\n" +
                "\n" +
                "  communicates-with sub stix-core-relationship,\n" +
                "    relates communicates-with_from,\n" +
                "    relates communicates-with_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  consists-of sub stix-core-relationship,\n" +
                "    relates consists-of_from,\n" +
                "    relates consists-of_to;\n" +
                "\n" +
                "  controls sub stix-core-relationship,\n" +
                "    relates controls_from,\n" +
                "    relates controls_to;\n" +
                "\n" +
                "  relation-has sub stix-core-relationship,\n" +
                "    relates relation-has_from,\n" +
                "    relates relation-has_to;\n" +
                "\n" +
                "  hosts sub stix-core-relationship,\n" +
                "    relates hosts_from,\n" +
                "    relates hosts_to;\n" +
                "\n" +
                "  relation-owns sub stix-core-relationship,\n" +
                "    relates owns_from,\n" +
                "    relates owns_to;\n" +
                "\n" +
                "  authored-by sub stix-core-relationship,\n" +
                "    relates authored-by_from,\n" +
                "    relates authored-by_to;\n" +
                "\n" +
                "  beacons-to sub stix-core-relationship,\n" +
                "    relates beacons-to_from,\n" +
                "    relates beacons-to_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  exfiltrate-to sub stix-core-relationship,\n" +
                "    relates exfiltrate-to_from,\n" +
                "    relates exfiltrate-to_to;\n" +
                "\n" +
                "  downloads sub stix-core-relationship,\n" +
                "    relates downloads_from,\n" +
                "    relates downloads_to;\n" +
                "\n" +
                "  drops sub stix-core-relationship,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    relates drops_from,\n" +
                "    relates drops_to;\n" +
                "\n" +
                "  exploits sub stix-core-relationship,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    relates exploits_from,\n" +
                "    relates exploits_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  variant-of sub stix-core-relationship,\n" +
                "    relates variant-of_from,\n" +
                "    relates variant-of_to;\n" +
                "\n" +
                "  characterizes sub stix-core-relationship,\n" +
                "    relates characterizes_from,\n" +
                "    relates characterizes_to;\n" +
                "\n" +
                "  analysis-of sub stix-core-relationship,\n" +
                "    relates analysis-of_from,\n" +
                "    relates analysis-of_to;\n" +
                "\n" +
                "  static-analysis-of sub stix-core-relationship,\n" +
                "    relates static-analysis-of_from,\n" +
                "    relates static-analysis-of_to;\n" +
                "\n" +
                "  dynamic-analysis-of sub stix-core-relationship,\n" +
                "    relates dynamic-analysis-of_from,\n" +
                "    relates dynamic-analysis-of_to;\n" +
                "\n" +
                "  impersonates sub stix-core-relationship,\n" +
                "    relates impersonates_from,\n" +
                "    relates impersonates_to;\n" +
                "\n" +
                "  remediates sub stix-core-relationship,\n" +
                "    relates remediates_from,\n" +
                "    relates remediates_to;\n" +
                "\n" +
                "  related-to sub stix-core-relationship,\n" +
                "    relates related-to_from,\n" +
                "    relates related-to_to;\n" +
                "\n" +
                "  derived-from sub stix-core-relationship,\n" +
                "    relates derived-from_from,\n" +
                "    relates derived-from_to;\n" +
                "\n" +
                "  duplicate-of sub stix-core-relationship,\n" +
                "    relates duplicate-of_from,\n" +
                "    relates duplicate-of_to;\n" +
                "\n" +
                "  part-of sub stix-core-relationship,\n" +
                "    relates part-of_from,\n" +
                "    relates part-of_to;\n" +
                "\n" +
                "  subtechnique-of sub stix-core-relationship,\n" +
                "    relates subtechnique-of_from,\n" +
                "    relates subtechnique-of_to;\n" +
                "\n" +
                "  revoked-by sub stix-core-relationship,\n" +
                "    relates revoked-by_from,\n" +
                "    relates revoked-by_to;\n" +
                "\n" +
                "  # STIX Sighting Relationships\n" +
                "  stix-sighting-relationship sub stix-relationship,\n" +
                "    owns description,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    owns attribute_count,\n" +
                "    owns x_opencti_negative,\n" +
                "    plays created-by:created-by_from,\n" +
                "    plays object-marking:object-marking_from,\n" +
                "    plays object-label:object-label_from,\n" +
                "    plays external-reference:external-reference_from,\n" +
                "    relates stix-sighting-relationship_from,\n" +
                "    relates stix-sighting-relationship_to;\n" +
                "\n" +
                "  # STIX Meta Relationships\n" +
                "  stix-meta-relationship sub stix-relationship,\n" +
                "    abstract;\n" +
                "\n" +
                "  created-by sub stix-meta-relationship,\n" +
                "    relates created-by_from,\n" +
                "    relates created-by_to;\n" +
                "\n" +
                "  object-marking sub stix-meta-relationship,\n" +
                "    relates object-marking_from,\n" +
                "    relates object-marking_to;\n" +
                "\n" +
                "  object sub stix-meta-relationship,\n" +
                "    relates object_from,\n" +
                "    relates object_to;\n" +
                "\n" +
                "  # STIX Internal Meta Relationships\n" +
                "  internal-meta-relationship sub stix-meta-relationship,\n" +
                "    abstract;\n" +
                "\n" +
                "  object-label sub stix-meta-relationship,\n" +
                "    relates object-label_from,\n" +
                "    relates object-label_to;\n" +
                "\n" +
                "  external-reference sub internal-meta-relationship,\n" +
                "    relates external-reference_from,\n" +
                "    relates external-reference_to;\n" +
                "\n" +
                "  kill-chain-phase sub internal-meta-relationship,\n" +
                "    relates kill-chain-phase_from,\n" +
                "    relates kill-chain-phase_to;\n" +
                "\n" +
                "  # STIX Cyber Observable Relationships\n" +
                "  stix-cyber-observable-relationship sub stix-relationship,\n" +
                "    abstract,\n" +
                "    owns relationship_type,\n" +
                "    owns start_time,\n" +
                "    owns i_start_time_day,\n" +
                "    owns i_start_time_month,\n" +
                "    owns i_start_time_year,\n" +
                "    owns stop_time,\n" +
                "    owns i_stop_time_day,\n" +
                "    owns i_stop_time_month,\n" +
                "    owns i_stop_time_year;\n" +
                "\n" +
                "  operating-system sub stix-cyber-observable-relationship,\n" +
                "    relates operating-system_from,\n" +
                "    relates operating-system_to;\n" +
                "\n" +
                "  sample sub stix-cyber-observable-relationship,\n" +
                "    relates sample_from,\n" +
                "    relates sample_to;\n" +
                "\n" +
                "  contains sub stix-cyber-observable-relationship,\n" +
                "    relates contains_from,\n" +
                "    relates contains_to;\n" +
                "\n" +
                "  resolves-to sub stix-cyber-observable-relationship,\n" +
                "    relates resolves-to_from,\n" +
                "    relates resolves-to_to;\n" +
                "\n" +
                "  belongs-to sub stix-cyber-observable-relationship,\n" +
                "    relates belongs-to_from,\n" +
                "    relates belongs-to_to;\n" +
                "\n" +
                "  from sub stix-cyber-observable-relationship,\n" +
                "    relates from_from,\n" +
                "    relates from_to;\n" +
                "\n" +
                "  sender sub stix-cyber-observable-relationship,\n" +
                "    relates sender_from,\n" +
                "    relates sender_to;\n" +
                "\n" +
                "  to sub stix-cyber-observable-relationship,\n" +
                "    relates to_from,\n" +
                "    relates to_to;\n" +
                "\n" +
                "  cc sub stix-cyber-observable-relationship,\n" +
                "    relates cc_from,\n" +
                "    relates cc_to;\n" +
                "\n" +
                "  bcc sub stix-cyber-observable-relationship,\n" +
                "    relates bcc_from,\n" +
                "    relates bcc_to;\n" +
                "\n" +
                "  raw-email sub stix-cyber-observable-relationship,\n" +
                "    relates raw-email_from,\n" +
                "    relates raw-email_to;\n" +
                "\n" +
                "  body-raw sub stix-cyber-observable-relationship,\n" +
                "    relates body-raw_from,\n" +
                "    relates body-raw_to;\n" +
                "\n" +
                "  parent-directory sub stix-cyber-observable-relationship,\n" +
                "    relates parent-directory_from,\n" +
                "    relates parent-directory_to;\n" +
                "\n" +
                "  relation-content sub stix-cyber-observable-relationship,\n" +
                "    relates relation-content_from,\n" +
                "    relates relation-content_to;\n" +
                "\n" +
                "  src sub stix-cyber-observable-relationship,\n" +
                "    relates src_from,\n" +
                "    relates src_to;\n" +
                "\n" +
                "  dst sub stix-cyber-observable-relationship,\n" +
                "    relates dst_from,\n" +
                "    relates dst_to;\n" +
                "\n" +
                "  src-payload sub stix-cyber-observable-relationship,\n" +
                "    relates src-payload_from,\n" +
                "    relates src-payload_to;\n" +
                "\n" +
                "  dst-payload sub stix-cyber-observable-relationship,\n" +
                "    relates dst-payload_from,\n" +
                "    relates dst-payload_to;\n" +
                "\n" +
                "  encapsulates sub stix-cyber-observable-relationship,\n" +
                "    relates encapsulates_from,\n" +
                "    relates encapsulates_to;\n" +
                "\n" +
                "  encapsulated-by sub stix-cyber-observable-relationship,\n" +
                "    relates encapsulated-by_from,\n" +
                "    relates encapsulated-by_to;\n" +
                "\n" +
                "  opened-connection sub stix-cyber-observable-relationship,\n" +
                "    relates opened-connection_from,\n" +
                "    relates opened-connection_to;\n" +
                "\n" +
                "  creator-user sub stix-cyber-observable-relationship,\n" +
                "    relates creator-user_from,\n" +
                "    relates creator-user_to;\n" +
                "\n" +
                "  image sub stix-cyber-observable-relationship,\n" +
                "    relates image_from,\n" +
                "    relates image_to;\n" +
                "\n" +
                "  parent sub stix-cyber-observable-relationship,\n" +
                "    relates parent_from,\n" +
                "    relates parent_to;\n" +
                "\n" +
                "  child sub stix-cyber-observable-relationship,\n" +
                "    relates child_from,\n" +
                "    relates child_to;\n" +
                "\n" +
                "  # Interrnal Cyber Observable Relationships\n" +
                "  internal-cyber-observable-relationship sub stix-cyber-observable-relationship,\n" +
                "      abstract;\n" +
                "\n" +
                "  body-multipart sub internal-cyber-observable-relationship,\n" +
                "    relates body-multipart_from,\n" +
                "    relates body-multipart_to;\n" +
                "\n" +
                "  values sub internal-cyber-observable-relationship,\n" +
                "    relates values_from,\n" +
                "    relates values_to;\n" +
                "\n" +
                "  x509-v3-extensions sub internal-cyber-observable-relationship,\n" +
                "    relates x509-v3-extensions_from,\n" +
                "    relates x509-v3-extensions_to;\n" +
                "\n" +
                "\n" +
                "  ## ENTITIES\n" +
                "\n" +
                "  Basic-Object sub entity,\n" +
                "    abstract,\n" +
                "    owns internal_id @key,\n" +
                "    owns standard_id @key,\n" +
                "    owns entity_type;\n" +
                "\n" +
                "  # Internal entities\n" +
                "  Internal-Object sub Basic-Object,\n" +
                "    abstract;\n" +
                "\n" +
                "  Settings sub Internal-Object,\n" +
                "    owns platform_title,\n" +
                "    owns platform_email,\n" +
                "    owns platform_url,\n" +
                "    owns platform_language,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at;\n" +
                "\n" +
                "  MigrationStatus sub Internal-Object,\n" +
                "    owns lastRun,\n" +
                "    plays migrates:migrates_from;\n" +
                "\n" +
                "  MigrationReference sub Internal-Object,\n" +
                "    owns title,\n" +
                "    owns timestamp,\n" +
                "    plays migrates:migrates_to;\n" +
                "\n" +
                "  Token sub Internal-Object,\n" +
                "    owns uuid @key,\n" +
                "    owns name,\n" +
                "    owns duration,\n" +
                "    owns issuer,\n" +
                "    owns revoked,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at,\n" +
                "    plays authorized-by:authorized-by_to;\n" +
                "\n" +
                "  Group sub Internal-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at,\n" +
                "    plays member-of:member-of_to,\n" +
                "    plays accesses-to:accesses-to_from;\n" +
                "\n" +
                "  User sub Internal-Object,\n" +
                "    owns user_email,\n" +
                "    owns password,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns firstname,\n" +
                "    owns lastname,\n" +
                "    owns language,\n" +
                "    owns external,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at,\n" +
                "    plays authorized-by:authorized-by_from,\n" +
                "    plays has-role:has-role_from,\n" +
                "    plays member-of:member-of_from,\n" +
                "    plays allowed-by:allowed-by_from;\n" +
                "\n" +
                "  Role sub Internal-Object,\n" +
                "    owns name @key,\n" +
                "    owns default_assignation,\n" +
                "    owns description,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at,\n" +
                "    plays allowed-by:allowed-by_to,\n" +
                "    plays has-capability:has-capability_from,\n" +
                "    plays has-role:has-role_to;\n" +
                "\n" +
                "  Capability sub Internal-Object,\n" +
                "    owns name @key,\n" +
                "    owns attribute_order,\n" +
                "    owns description,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at,\n" +
                "    plays has-capability:has-capability_to;\n" +
                "\n" +
                "  Connector sub Internal-Object,\n" +
                "    owns name,\n" +
                "    owns active,\n" +
                "    owns auto,\n" +
                "    owns connector_type,\n" +
                "    owns connector_scope,\n" +
                "    owns connector_state,\n" +
                "    owns connector_state_reset,\n" +
                "    owns connector_user_id,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at;\n" +
                "\n" +
                "  Workspace sub Internal-Object,\n" +
                "    owns workspace_type,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns x_opencti_graph_data,\n" +
                "    owns workspace_data,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at;\n" +
                "\n" +
                "  # STIX Object Entities\n" +
                "  Stix-Object sub Basic-Object,\n" +
                "    abstract,\n" +
                "    owns x_opencti_stix_ids,\n" +
                "    owns spec_version,\n" +
                "    owns created_at,\n" +
                "    owns i_created_at_day,\n" +
                "    owns i_created_at_month,\n" +
                "    owns i_created_at_year,\n" +
                "    owns updated_at;\n" +
                "\n" +
                "  # STIX Meta Object Entities\n" +
                "  Stix-Meta-Object sub Stix-Object,\n" +
                "    abstract,\n" +
                "    owns created,\n" +
                "    owns modified;\n" +
                "\n" +
                "  Marking-Definition sub Stix-Meta-Object,\n" +
                "    owns definition_type,\n" +
                "    owns definition,\n" +
                "    owns x_opencti_order,\n" +
                "    owns x_opencti_color,\n" +
                "    plays accesses-to:accesses-to_to,\n" +
                "    plays object-marking:object-marking_to;\n" +
                "\n" +
                "  Label sub Stix-Meta-Object,\n" +
                "    owns value @key,\n" +
                "    owns color,\n" +
                "    plays object-label:object-label_to;\n" +
                "\n" +
                "  External-Reference sub Stix-Meta-Object,\n" +
                "    owns source_name,\n" +
                "    owns description,\n" +
                "    owns url,\n" +
                "    owns hash,\n" +
                "    owns external_id,\n" +
                "    plays external-reference:external-reference_to;\n" +
                "\n" +
                "  Kill-Chain-Phase sub Stix-Meta-Object,\n" +
                "    owns kill_chain_name,\n" +
                "    owns phase_name,\n" +
                "    owns x_opencti_order,\n" +
                "    plays kill-chain-phase:kill-chain-phase_to;\n" +
                "\n" +
                "  Stix-Core-Object sub Stix-Object,\n" +
                "    abstract,\n" +
                "    plays created-by:created-by_from,\n" +
                "    plays object-marking:object-marking_from,\n" +
                "    plays object-label:object-label_from,\n" +
                "    plays external-reference:external-reference_from,\n" +
                "    plays object:object_to;\n" +
                "\n" +
                "  # STIX Domain Object Entities\n" +
                "  Stix-Domain-Object sub Stix-Core-Object,\n" +
                "    abstract,\n" +
                "    owns revoked,\n" +
                "    owns confidence,\n" +
                "    owns lang,\n" +
                "    owns created,\n" +
                "    owns modified,\n" +
                "    plays revoked-by:revoked-by_from,\n" +
                "    plays duplicate-of:duplicate-of_from,\n" +
                "    plays derived-from:derived-from_from,\n" +
                "    plays related-to:related-to_from,\n" +
                "    plays stix-sighting-relationship:stix-sighting-relationship_from,\n" +
                "    plays revoked-by:revoked-by_to,\n" +
                "    plays duplicate-of:duplicate-of_to,\n" +
                "    plays derived-from:derived-from_to,\n" +
                "    plays related-to:related-to_to;\n" +
                "\n" +
                "  Attack-Pattern sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns x_mitre_platforms,\n" +
                "    owns x_mitre_permissions_required,\n" +
                "    owns x_mitre_detection,\n" +
                "    owns x_mitre_id,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    plays subtechnique-of:subtechnique-of_from,\n" +
                "    plays delivers:delivers_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays subtechnique-of:subtechnique-of_to,\n" +
                "    plays indicates:indicates_to,\n" +
                "    plays mitigates:mitigates_to,\n" +
                "    plays uses:uses_to;\n" +
                "\n" +
                "  Campaign sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    owns objective,\n" +
                "    plays attributed-to:attributed-to_from,\n" +
                "    plays compromises:compromises_from,\n" +
                "    plays originates-from:originates-from_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays attributed-to:attributed-to_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  Container sub Stix-Domain-Object,\n" +
                "    abstract,\n" +
                "    plays object:object_from;\n" +
                "\n" +
                "  Note sub Container,\n" +
                "    owns attribute_abstract,\n" +
                "    owns content,\n" +
                "    owns authors;\n" +
                "\n" +
                "  Observed-Data sub Container,\n" +
                "    owns first_observed,\n" +
                "    owns last_observed,\n" +
                "    owns number_observed,\n" +
                "    plays based-on:based-on_to,\n" +
                "    plays consists-of:consists-of_to;\n" +
                "\n" +
                "  Opinion sub Container,\n" +
                "    owns explanation,\n" +
                "    owns authors,\n" +
                "    owns opinion;\n" +
                "\n" +
                "  Report sub Container,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns report_types,\n" +
                "    owns x_opencti_report_status,\n" +
                "    owns published,\n" +
                "    owns i_published_day,\n" +
                "    owns i_published_year,\n" +
                "    owns i_published_month,\n" +
                "    owns x_opencti_graph_data;\n" +
                "\n" +
                "  Course-Of-Action sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns x_opencti_aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns x_mitre_id,\n" +
                "    plays mitigates:mitigates_from,\n" +
                "    plays investigates:investigates_from,\n" +
                "    plays migrates:migrates_from,\n" +
                "    plays remediates:remediates_from;\n" +
                "\n" +
                "  Identity sub Stix-Domain-Object,\n" +
                "    abstract,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns contact_information,\n" +
                "    owns identity_class,\n" +
                "    owns roles,\n" +
                "    owns x_opencti_aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    plays located-at:located-at_from,\n" +
                "    plays part-of:part-of_from,\n" +
                "    plays stix-sighting-relationship:stix-sighting-relationship_to,\n" +
                "    plays targets:targets_to,\n" +
                "    plays attributed-to:attributed-to_to,\n" +
                "    plays impersonates:impersonates_to;\n" +
                "\n" +
                "  Individual sub Identity,\n" +
                "    owns x_opencti_firstname,\n" +
                "    owns x_opencti_lastname,\n" +
                "    plays created-by:created-by_to;\n" +
                "\n" +
                "  Organization sub Identity,\n" +
                "    owns x_opencti_organization_type,\n" +
                "    owns x_opencti_reliability,\n" +
                "    plays created-by:created-by_to,\n" +
                "    plays part-of:part-of_to;\n" +
                "\n" +
                "  Sector sub Identity,\n" +
                "    plays part-of:part-of_to;\n" +
                "\n" +
                "  Indicator sub Stix-Domain-Object,\n" +
                "    owns pattern_type,\n" +
                "    owns pattern_version,\n" +
                "    owns pattern @key,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns indicator_types,\n" +
                "    owns valid_from,\n" +
                "    owns i_valid_from_day,\n" +
                "    owns i_valid_from_month,\n" +
                "    owns i_valid_from_year,\n" +
                "    owns valid_until,\n" +
                "    owns i_valid_until_day,\n" +
                "    owns i_valid_until_month,\n" +
                "    owns i_valid_until_year,\n" +
                "    owns x_opencti_score,\n" +
                "    owns x_opencti_detection,\n" +
                "    owns x_opencti_main_observable_type,\n" +
                "    owns x_mitre_platforms,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    plays indicates:indicates_from,\n" +
                "    plays based-on:based-on_from,\n" +
                "    plays investigates:investigates_to,\n" +
                "    plays mitigates:mitigates_to;\n" +
                "\n" +
                "  Infrastructure sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns infrastructure_types,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    plays communicates-with:communicates-with_from,\n" +
                "    plays consists-of:consists-of_from,\n" +
                "    plays controls:controls_from,\n" +
                "    plays relation-has:relation-has_from,\n" +
                "    plays hosts:hosts_from,\n" +
                "    plays located-at:located-at_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays compromises:compromises_to,\n" +
                "    plays beacons-to:beacons-to_to,\n" +
                "    plays exfiltrate-to:exfiltrate-to_to,\n" +
                "    plays hosts:hosts_to,\n" +
                "    plays indicates:indicates_to,\n" +
                "    plays relation-owns:owns_to,\n" +
                "    plays targets:targets_to,\n" +
                "    plays uses:uses_to;\n" +
                "\n" +
                "  Intrusion-Set sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    owns goals,\n" +
                "    owns resource_level,\n" +
                "    owns primary_motivation,\n" +
                "    owns secondary_motivations,\n" +
                "    plays attributed-to:attributed-to_from,\n" +
                "    plays compromises:compromises_from,\n" +
                "    plays hosts:hosts_from,\n" +
                "    plays relation-owns:owns_from,\n" +
                "    plays originates-from:originates-from_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays attributed-to:attributed-to_to,\n" +
                "    plays authored-by:authored-by_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  Location sub Stix-Domain-Object,\n" +
                "    abstract,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns latitude,\n" +
                "    owns longitude,\n" +
                "    owns precision,\n" +
                "    owns x_opencti_aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns x_opencti_location_type,\n" +
                "    plays located-at:located-at_from,\n" +
                "    plays stix-sighting-relationship:stix-sighting-relationship_to,\n" +
                "    plays located-at:located-at_to,\n" +
                "    plays originates-from:originates-from_to,\n" +
                "    plays targets:targets_to;\n" +
                "\n" +
                "  City sub Location;\n" +
                "\n" +
                "  Country sub Location;\n" +
                "\n" +
                "  Region sub Location;\n" +
                "\n" +
                "  Position sub Location,\n" +
                "    owns street_address,\n" +
                "    owns postal_code;\n" +
                "\n" +
                "  Malware sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns malware_types,\n" +
                "    owns is_family,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    owns architecture_execution_envs,\n" +
                "    owns implementation_languages,\n" +
                "    owns capabilities,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    plays operating-system:operating-system_from,\n" +
                "    plays sample:sample_from,\n" +
                "    plays authorized-by:authorized-by_from,\n" +
                "    plays beacons-to:beacons-to_from,\n" +
                "    plays exfiltrate-to:exfiltrate-to_from,\n" +
                "    plays communicates-with:communicates-with_from,\n" +
                "    plays controls:controls_from,\n" +
                "    plays downloads:downloads_from,\n" +
                "    plays drops:drops_from,\n" +
                "    plays exploits:exploits_from,\n" +
                "    plays originates-from:originates-from_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays variant-of:variant-of_from,\n" +
                "    plays variant-of:variant-of_to,\n" +
                "    plays delivers:delivers_to,\n" +
                "    plays indicates:indicates_to,\n" +
                "    plays mitigates:mitigates_to,\n" +
                "    plays remediates:remediates_to,\n" +
                "    plays uses:uses_to,\n" +
                "    plays drops:drops_to,\n" +
                "    plays controls:controls_to,\n" +
                "    plays characterizes:characterizes_to,\n" +
                "    plays static-analysis-of:static-analysis-of_to,\n" +
                "    plays dynamic-analysis-of:dynamic-analysis-of_to;\n" +
                "\n" +
                "  Threat-Actor sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns threat_actor_types,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    owns goals,\n" +
                "    owns sophistication,\n" +
                "    owns resource_level,\n" +
                "    owns primary_motivation,\n" +
                "    owns secondary_motivations,\n" +
                "    owns personal_motivations,\n" +
                "    plays attributed-to:attributed-to_from,\n" +
                "    plays compromises:compromises_from,\n" +
                "    plays hosts:hosts_from,\n" +
                "    plays relation-owns:owns_from,\n" +
                "    plays impersonates:impersonates_from,\n" +
                "    plays located-at:located-at_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays attributed-to:attributed-to_to,\n" +
                "    plays authored-by:authored-by_to,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "  Tool sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns tool_types,\n" +
                "    owns tool_version,\n" +
                "    plays kill-chain-phase:kill-chain-phase_from,\n" +
                "    plays delivers:delivers_from,\n" +
                "    plays drops:drops_from,\n" +
                "    plays relation-has:relation-has_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays hosts:hosts_to,\n" +
                "    plays downloads:downloads_to,\n" +
                "    plays drops:drops_to,\n" +
                "    plays indicates:indicates_to,\n" +
                "    plays uses:uses_to;\n" +
                "\n" +
                "  Vulnerability sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns x_opencti_base_score,\n" +
                "    owns x_opencti_base_severity,\n" +
                "    owns x_opencti_attack_vector,\n" +
                "    owns x_opencti_integrity_impact,\n" +
                "    owns x_opencti_availability_impact,\n" +
                "    plays targets:targets_to,\n" +
                "    plays exploits:exploits_to,\n" +
                "    plays mitigates:mitigates_to,\n" +
                "    plays remediates:remediates_to,\n" +
                "    plays relation-has:relation-has_to;\n" +
                "\n" +
                "  X-OpenCTI-Incident sub Stix-Domain-Object,\n" +
                "    owns name,\n" +
                "    owns description,\n" +
                "    owns aliases,\n" +
                "    owns i_aliases_ids,\n" +
                "    owns first_seen,\n" +
                "    owns i_first_seen_day,\n" +
                "    owns i_first_seen_month,\n" +
                "    owns i_first_seen_year,\n" +
                "    owns last_seen,\n" +
                "    owns i_last_seen_day,\n" +
                "    owns i_last_seen_month,\n" +
                "    owns i_last_seen_year,\n" +
                "    owns objective,\n" +
                "    plays attributed-to:attributed-to_from,\n" +
                "    plays compromises:compromises_from,\n" +
                "    plays targets:targets_from,\n" +
                "    plays uses:uses_from,\n" +
                "    plays indicates:indicates_to;\n" +
                "\n" +
                "   # STIX Cyber Observables Entities\n" +
                "  Stix-Cyber-Observable sub Stix-Core-Object,\n" +
                "    abstract,\n" +
                "    owns x_opencti_description,\n" +
                "    owns x_opencti_score,\n" +
                "    plays stix-sighting-relationship:stix-sighting-relationship_from,\n" +
                "    plays related-to:related-to_from,\n" +
                "    plays consists-of:consists-of_to,\n" +
                "    plays based-on:based-on_to,\n" +
                "    plays related-to:related-to_to;\n" +
                "\n" +
                "  Autonomous-System sub Stix-Cyber-Observable,\n" +
                "    owns number,\n" +
                "    owns name,\n" +
                "    owns rir,\n" +
                "    plays belongs-to:belongs-to_to;\n" +
                "\n" +
                "  Directory sub Stix-Cyber-Observable,\n" +
                "    owns path,\n" +
                "    owns path_enc,\n" +
                "    owns ctime,\n" +
                "    owns mtime,\n" +
                "    owns atime,\n" +
                "    plays contains:contains_from;\n" +
                "\n" +
                "  Domain-Name sub Stix-Cyber-Observable,\n" +
                "    owns value,\n" +
                "    plays resolves-to:resolves-to_to,\n" +
                "    plays src:src_to,\n" +
                "    plays dst:dst_to;\n" +
                "\n" +
                "  Email-Addr sub Stix-Cyber-Observable,\n" +
                "    owns value,\n" +
                "    owns display_name,\n" +
                "    plays belongs-to:belongs-to_from,\n" +
                "    plays from:from_to,\n" +
                "    plays sender:sender_to,\n" +
                "    plays to:to_to,\n" +
                "    plays cc:cc_to,\n" +
                "    plays bcc:bcc_to;\n" +
                "\n" +
                "  Email-Message sub Stix-Cyber-Observable,\n" +
                "    owns is_multipart,\n" +
                "    owns attribute_date,\n" +
                "    owns content_type,\n" +
                "    owns message_id,\n" +
                "    owns subject,\n" +
                "    owns received_lines,\n" +
                "    owns body,\n" +
                "    plays from:from_from,\n" +
                "    plays sender:sender_from,\n" +
                "    plays to:to_from,\n" +
                "    plays cc:cc_from,\n" +
                "    plays bcc:bcc_from,\n" +
                "    plays body-multipart:body-multipart_from,\n" +
                "    plays raw-email:raw-email_from;\n" +
                "\n" +
                "  Email-Mime-Part-Type sub Stix-Cyber-Observable,\n" +
                "    owns body,\n" +
                "    owns content_type,\n" +
                "    owns content_disposition,\n" +
                "    plays body-raw:body-raw_from,\n" +
                "    plays body-multipart:body-multipart_to;\n" +
                "\n" +
                "  Hashed-Observable sub Stix-Cyber-Observable,\n" +
                "    abstract,\n" +
                "    owns hashes;\n" +
                "\n" +
                "  Artifact sub Hashed-Observable,\n" +
                "    owns mime_type,\n" +
                "    owns payload_bin,\n" +
                "    owns url,\n" +
                "    owns encryption_algorithm,\n" +
                "    owns decryption_key,\n" +
                "    plays raw-email:raw-email_to,\n" +
                "    plays body-raw:body-raw_to;\n" +
                "\n" +
                "  StixFile sub Hashed-Observable,\n" +
                "    owns extensions,\n" +
                "    owns size,\n" +
                "    owns name,\n" +
                "    owns name_enc,\n" +
                "    owns magic_number_hex,\n" +
                "    owns mime_type,\n" +
                "    owns ctime,\n" +
                "    owns mtime,\n" +
                "    owns atime,\n" +
                "    plays parent-directory:parent-directory_from,\n" +
                "    plays contains:contains_from,\n" +
                "    plays relation-content:relation-content_from,\n" +
                "    plays parent-directory:parent-directory_to,\n" +
                "    plays contains:contains_to,\n" +
                "    plays image:image_to;\n" +
                "\n" +
                "  X509-Certificate sub Hashed-Observable,\n" +
                "    owns is_self_signed,\n" +
                "    owns version,\n" +
                "    owns serial_number,\n" +
                "    owns signature_algorithm,\n" +
                "    owns issuer,\n" +
                "    owns validity_not_before,\n" +
                "    owns validity_not_after,\n" +
                "    owns subject,\n" +
                "    owns subject_public_key_algorithm,\n" +
                "    owns subject_public_key_modulus,\n" +
                "    owns subject_public_key_exponent,\n" +
                "    plays x509-v3-extensions:x509-v3-extensions_from;\n" +
                "\n" +
                "  IPv4-Addr sub Stix-Cyber-Observable,\n" +
                "    owns value,\n" +
                "    plays located-at:located-at_from,\n" +
                "    plays resolves-to:resolves-to_from,\n" +
                "    plays belongs-to:belongs-to_from,\n" +
                "    plays resolves-to:resolves-to_to,\n" +
                "    plays src:src_to,\n" +
                "    plays dst:dst_to;\n" +
                "\n" +
                "  IPv6-Addr sub Stix-Cyber-Observable,\n" +
                "    owns value,\n" +
                "    plays located-at:located-at_from,\n" +
                "    plays resolves-to:resolves-to_from,\n" +
                "    plays belongs-to:belongs-to_from,\n" +
                "    plays resolves-to:resolves-to_to,\n" +
                "    plays src:src_to,\n" +
                "    plays dst:dst_to;\n" +
                "\n" +
                "  Mac-Addr sub Stix-Cyber-Observable,\n" +
                "    owns value,\n" +
                "    plays resolves-to:resolves-to_to,\n" +
                "    plays src:src_to,\n" +
                "    plays dst:dst_to;\n" +
                "\n" +
                "  Mutex sub Stix-Cyber-Observable,\n" +
                "    owns name;\n" +
                "\n" +
                "  Network-Traffic sub Stix-Cyber-Observable,\n" +
                "    owns extensions,\n" +
                "    owns start,\n" +
                "    owns end,\n" +
                "    owns is_active,\n" +
                "    owns src_port,\n" +
                "    owns dst_port,\n" +
                "    owns protocols,\n" +
                "    owns src_byte_count,\n" +
                "    owns dst_byte_count,\n" +
                "    owns src_packets,\n" +
                "    owns dst_packets,\n" +
                "    plays src:src_from,\n" +
                "    plays dst:dst_from,\n" +
                "    plays src-payload:src-payload_from,\n" +
                "    plays dst-payload:dst-payload_from,\n" +
                "    plays encapsulates:encapsulates_from,\n" +
                "    plays encapsulated-by:encapsulated-by_from,\n" +
                "    plays encapsulates:encapsulates_to,\n" +
                "    plays encapsulated-by:encapsulated-by_to,\n" +
                "    plays opened-connection:opened-connection_to;\n" +
                "\n" +
                "  Process sub Stix-Cyber-Observable,\n" +
                "    owns extensions,\n" +
                "    owns is_hidden,\n" +
                "    owns pid,\n" +
                "    owns created_time,\n" +
                "    owns cwd,\n" +
                "    owns command_line,\n" +
                "    owns environment_variables,\n" +
                "    plays opened-connection:opened-connection_from,\n" +
                "    plays creator-user:creator-user_from,\n" +
                "    plays image:image_from,\n" +
                "    plays parent:parent_from,\n" +
                "    plays child:child_from,\n" +
                "    plays parent:parent_to,\n" +
                "    plays child:child_to;\n" +
                "\n" +
                "  Software sub Stix-Cyber-Observable,\n" +
                "    owns name,\n" +
                "    owns cpe,\n" +
                "    owns swid,\n" +
                "    owns languages,\n" +
                "    owns vendor,\n" +
                "    owns version;\n" +
                "\n" +
                "  Url sub Stix-Cyber-Observable,\n" +
                "    owns value;\n" +
                "\n" +
                "  User-Account sub Stix-Cyber-Observable,\n" +
                "    owns extensions,\n" +
                "    owns user_id,\n" +
                "    owns credential,\n" +
                "    owns account_login,\n" +
                "    owns account_type,\n" +
                "    owns display_name,\n" +
                "    owns is_service_account,\n" +
                "    owns is_privileged,\n" +
                "    owns can_escalate_privs,\n" +
                "    owns is_disabled,\n" +
                "    owns account_created,\n" +
                "    owns account_expires,\n" +
                "    owns credential_last_changed,\n" +
                "    owns account_first_login,\n" +
                "    owns account_last_login,\n" +
                "    plays creator-user:creator-user_to;\n" +
                "\n" +
                "  Windows-Registry-Key sub Stix-Cyber-Observable,\n" +
                "    owns attribute_key,\n" +
                "    owns modified_time,\n" +
                "    owns number_of_subkeys,\n" +
                "    plays values:values_from,\n" +
                "    plays creator-user:creator-user_from;\n" +
                "\n" +
                "  Windows-Registry-Value-Type sub Stix-Cyber-Observable,\n" +
                "    owns name,\n" +
                "    owns data,\n" +
                "    owns data_type,\n" +
                "    plays values:values_to;\n" +
                "\n" +
                "  X509-V3-Extensions-Type sub Stix-Cyber-Observable,\n" +
                "    owns basic_constraints,\n" +
                "    owns name_constraints,\n" +
                "    owns policy_constraints,\n" +
                "    owns key_usage,\n" +
                "    owns extended_key_usage,\n" +
                "    owns subject_key_identifier,\n" +
                "    owns authority_key_identifier,\n" +
                "    owns subject_alternative_name,\n" +
                "    owns issuer_alternative_name,\n" +
                "    owns subject_directory_attributes,\n" +
                "    owns crl_distribution_points,\n" +
                "    owns inhibit_any_policy,\n" +
                "    owns private_key_usage_period_not_before,\n" +
                "    owns private_key_usage_period_not_after,\n" +
                "    owns certificate_policies,\n" +
                "    owns policy_mappings,\n" +
                "    plays x509-v3-extensions:x509-v3-extensions_to;\n" +
                "\n" +
                "  X-OpenCTI-Cryptographic-Key sub Stix-Cyber-Observable,\n" +
                "    owns value;\n" +
                "\n" +
                "  X-OpenCTI-Cryptocurrency-Wallet sub Stix-Cyber-Observable,\n" +
                "    owns value;\n" +
                "\n" +
                "  X-OpenCTI-Hostname sub Stix-Cyber-Observable,\n" +
                "    owns value;\n" +
                "\n" +
                "  X-OpenCTI-Text sub Stix-Cyber-Observable,\n" +
                "    owns value;\n" +
                "\n" +
                "  X-OpenCTI-User-Agent sub Stix-Cyber-Observable,\n" +
                "    owns value;\n";
    }
}
