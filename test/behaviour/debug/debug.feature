##
## Copyright (C) 2022 Vaticle
##
## This program is free software: you can redistribute it and/or modify
## it under the terms of the GNU Affero General Public License as
## published by the Free Software Foundation, either version 3 of the
## License, or (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Affero General Public License for more details.
##
## You should have received a copy of the GNU Affero General Public License
## along with this program.  If not, see <https://www.gnu.org/licenses/>.
##
#
Feature: Debugging Space
Feature: TypeQL Rule Validation

  Background: Initialise a session and transaction for each scenario
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: typedb
    Given connection open schema session for database: typedb
    Given session opens transaction of type: write
    Given typeql define
      """
      define
stix-object sub entity,
	# Required
	owns stix-type,
	owns stix-id,
	owns custom-attribute,
	plays stix-core-relationship:active-role,
	plays stix-core-relationship:passive-role;

	stix-core-object sub stix-object,
		# Required for SDO but Optional for SCO
		owns spec-version,

		# Optional
		owns extensions,

		plays object-marking:marked,
		plays creation:created,
		plays sighting:sighting-of,

		# Common relations for SDOs and SCOs. Defined in 3.7.
		plays derivation:derived-from,
		plays derivation:deriving,
		plays duplicate:duplicated-object,
		plays relatedness:related-to;

			stix-domain-object sub stix-core-object,
				# Required
				owns created,
				owns modified,

				# Optional
				owns revoked,
				owns labels,
				owns confidence,
				owns langs,
				plays external-referencing:referencing,

				plays indication:indicated;

			stix-cyber-observable-object sub stix-core-object,
				owns defanged,
				plays reference:referred;

	stix-meta-object sub stix-object,
		abstract,
		owns created,
		owns modified;

##### 2 Common Data Types #####

# 2.5 External Reference
external-reference sub stix-meta-object,
	# In addition to source-name, at least one of description, url or external-id must be present§
	# Required
	owns source-name,

	# Optional
	owns description,
	owns url,
	owns external-id,
	owns hash-general,
	plays external-referencing:referenced;

external-referencing sub relation,
	relates referenced,
	relates referencing;

# 2.7 Hash
hash sub stix-attribute-string, abstract;
	hash-general sub hash;
	md5 sub hash;
	sha-1 sub hash;

# 2.11 Kill Chane Phase
kill-chain-usage sub stix-core-relationship,
	relates kill-chain-used,
	relates kill-chain-using;

kill-chain-phase sub stix-meta-object,
	owns kill-chain-name,
	owns phase-name,
	plays kill-chain-usage:kill-chain-using,

	# inferred role player
	plays kill-chain:participating-kill-chain-phase;

##### 4 STIX™ Domain Objects #####

# Custom object
custom-object sub stix-domain-object,
	owns name,
	owns description,
	owns alias,
	owns first-seen,
	owns last-seen,
	owns objective,
	plays kill-chain-usage:kill-chain-used,
	plays delivery:delivering,
	plays target:targetting,
	plays use:used-by,
	plays mitigation:mitigated,
	plays use:used;


# 4.1
attack-pattern sub stix-domain-object,
	# Attack Pattern Specific Properties
	owns name,
	owns description,
	owns alias,
	plays kill-chain-usage:kill-chain-used,

	# Relations
	plays delivery:delivering,
	plays target:targetting,
	plays use:used-by,

	# Reverse
	plays mitigation:mitigated,
	plays use:used;

# 4.2 Campaign
campaign sub stix-domain-object,
	owns name,
	owns description,
	owns alias,
	owns first-seen,
	owns last-seen,
	owns objective,


	# Relations
	plays attribution:attributing,
	# plays compromise:comrpomising, -> relation not defined yet
	plays origination:originating,
	plays target:targetting,
	plays use:used-by;

	# Reverse
	# plays indication:indicated; -> Defined in SDO


# 4.3 Course of Action
course-of-action sub stix-domain-object,
	owns name,
	owns description,
	owns action,
	plays mitigation:mitigating;

# 4.4 Grouping
grouping sub stix-domain-object; #TODO

# 4.5 Identity
identity sub stix-domain-object,
	owns name,
	owns description,
	owns stix-role,
	owns sector,  # should this perhaps be a relation?
	owns contact-information,
	plays located-at:locating,
	plays target:targetted,
	plays creation:creator,
	plays attribution:attributed,
	plays sighting:saw,
	plays target:targetted;

	# 10.7 Identity Class Vocabulary
	individual sub identity;
	group sub identity;
	system sub identity;
	organization sub identity;
	class sub identity;
	id-unknown sub identity;

# 4.6 Incident
incident sub stix-domain-object,
	owns name,
	owns description;

# 4.7 Indicator
indicator sub stix-domain-object,
	owns name,
	owns description,
	owns indicator-type, # This could be a relation to the open vocabulary
						 # anomalous-activity, anonymization, benign, compromised, malicious-activity, attribution, unknown
	owns pattern,
	owns pattern-type,   # should this open vocabulary be a relation? (stix, pcre, sigma, snort, suricata, yara)
	owns pattern-version,
	owns valid-from,
	owns valid-until,
	plays indication:indicating,
	plays kill-chain-usage:kill-chain-used;

# 4.8 Infrastructure TODO
infrastructure sub stix-domain-object,
	plays host:hosted,
	plays ownership:owned,
	plays compromise:compromised,
	plays target:targetted,
	plays kill-chain-usage:kill-chain-used,
	plays use:used-by,
	plays use:used,
	plays located-at:locating;

# 4.9 Intrusion Set
intrusion-set sub stix-domain-object, # TODO
	owns name,
	owns description,
	owns alias,
	owns first-seen,
	owns last-seen,
	owns goals,
	owns resource-level,
	owns primary-motivation,
	owns secondary-motivations,

	plays attribution:attributing,
	plays compromise:compromising,
	plays host:hosting,
	plays ownership:owning,
	plays origination:originating,
	plays use:used-by,
	plays attribution:attributed,
	plays target:targetting,
	plays authorship:authored,
	# plays indication:indicated; Already defined in SDO

	# Inferred role player
	plays inferred-mitigation:mitigated;

# 4.10 Location
location sub stix-domain-object,
	owns name,
	owns description,
	owns latitude,
	owns longitude,
	owns precision,
	owns region,
	owns country,
	owns administrative-area,
	owns city,
	owns street-address,
	owns postal-code,
	plays located-at:located,
	plays origination:originated-from,
	plays target:targetted;

# 4.11 Malware
malware sub stix-domain-object,
	owns name,
	owns description,
	owns malware-type,
	owns is-family,
    owns alias,
    owns first-seen,
    owns last-seen,
    owns operating-system-refs,
    owns architecture-execution-envs,
    owns implementation-languages,
    owns capability,
    owns sample-refs,
    plays download:downloaded,
    plays download:downloading,
    plays exploit:exploiting,
    plays origination:originating,
    plays target:targetting,
    plays use:used-by,
    plays use:used,
	plays kill-chain-usage:kill-chain-used,
	plays delivery:delivered,
	plays authorship:authoring;

# 4.12 Malware Analysis
malware-analysis sub stix-domain-object; # TODO

# 4.13 Note
note sub stix-domain-object; #TODO

# 4.14
observed-data sub stix-domain-object,
	owns first-observed,
	owns last-observed,
	owns number-observed,
	owns object,
	plays sighting:observed,
	plays reference:referring;

# 4.15 Opinion
opinion sub stix-domain-object; #TODO

# 4.16 Report
report sub stix-domain-object; #TODO

# 4.17
threat-actor sub stix-domain-object,
	owns name,
	owns description,
	owns threat-actor-type,
	owns alias,
	owns first-seen,
	owns last-seen,
	owns stix-role,
	owns goals,
	owns sophistication,
	owns resource-level,
	owns primary-motivation,
	owns secondary-motivations,
	owns personal-motivations,
	plays compromise:compromising,
	plays target:targetting,
	plays attribution:attributing,
	plays attribution:attributed,
	plays use:used-by,
	plays located-at:locating;

# 4.18 Tool
tool sub stix-domain-object,
	owns name,
	owns description,
	owns tool-type,
	owns alias,
	owns tool-version,

	plays delivery:delivering,
	plays drop:dropping,
	plays have:having,
	plays target:targetting,
	plays use:used-by,
	plays use:used,
	plays host:hosted,
	plays download:downloaded,
	plays drop:dropped,
	# plays indication:indicated, Already defined in SDO
	plays mitigation:mitigated,
	plays kill-chain-usage:kill-chain-used;

# 4.19 Vulnerability TODO
vulnerability sub stix-domain-object,
	plays have:had,
 	plays target:targetted,
 	plays exploit:exploited;

##### 5 STIX™ Relationship Objects #####

# 5.2 Sighting
sighting sub stix-core-relationship,
	owns first-seen,
	owns last-seen,
	owns count,
	owns summary,
	relates saw,
	relates sighting-of,
	relates observed;

##### 6 STIX™ Cyber-observable Objects #####

# 6.1 Artifact Object

# 6.2 Autonomous System Object

# 6.3 Directory Object

# 6.4 Domain Name Object

# 6.5 Email Address Object

# 6.6 Email Message

# 6.7 File
file sub stix-cyber-observable-object,
	owns size,
	owns name,
	owns hash-general,
	owns md5,
	owns sha-1,

	plays download:downloaded;

# 6.8 IPv4 Address

# 6.9 IPv6 Address

# 6.10 MAC Address

# 6.11 Mutex

# 6.12 Network Traffice

# 6.13 Process

# 6.14 Software

# 6.15 URL

# 6.16 User Account

# 6.17
windows-registry-key sub stix-cyber-observable-object,
	owns attribute-key,
	owns values,
	owns modified-time,
	owns number-subkeys,
	owns values;

# 6.18 X.509 Certificate

##### 7 STIX™ Meta Objects #####

# 7.1 Language Content

# owns spec-version

# 7.2 Data markings - this describes how data can be used/shared

marking-definition sub stix-meta-object,
	owns name,
	owns spec-version,
	plays creation:created,
	plays data-marking:marking;

	statement-marking sub marking-definition,
		owns statement;

	tlp-marking sub marking-definition;
		tlp-white sub tlp-marking;
		tlp-green sub tlp-marking;
		tlp-amber sub tlp-marking;
		tlp-red sub tlp-marking;


data-marking sub relation,
	relates marking,
	relates marked;

	object-marking sub data-marking;

	granular-marking sub data-marking;

# 7.3 Extension Definition

##### RELATIONSHIPS - APENDIX B #####

stix-core-relationship sub relation, # in conceptMapper
	# Required
	owns spec-version,
	owns stix-id,
	owns created,
	owns modified,
	owns stix-type,

	# Optional
	owns description,
	owns revoked,
	owns labels,
	owns confidence,
	owns langs,
	owns extensions,

	owns custom-attribute,

	relates active-role,
	relates passive-role,
	plays object-marking:marked,
	plays external-referencing:referencing;


	delivery sub stix-core-relationship,
		relates delivering,
		relates delivered;

	target sub stix-core-relationship,
		relates targetting,
		relates targetted;

	attribution sub stix-core-relationship,
		relates attributing,
		relates attributed;

	mitigation sub stix-core-relationship,
		relates mitigated,
		relates mitigating;

	indication sub stix-core-relationship,
		relates indicating,
		relates indicated;

	creation sub stix-core-relationship,
		relates created,
		relates creator;

	reference sub stix-core-relationship,
		relates referred,
		relates referring;

	use sub stix-core-relationship,
		relates used-by,
		relates used;

	located-at sub stix-core-relationship,
		relates located,
		relates locating;

	origination sub stix-core-relationship,
		relates originated-from,
		relates originating;

	have sub stix-core-relationship, # Could come up with a better name?
		relates having,
		relates had;

	host sub stix-core-relationship,
		relates hosting,
		relates hosted;

	ownership sub stix-core-relationship,
		relates owned,
		relates owning;

	compromise sub stix-core-relationship,
		relates compromising,
		relates compromised;

	authorship sub stix-core-relationship,
		relates authoring,
		relates authored;

	drop sub stix-core-relationship,
		relates dropped,
		relates dropping;

	download sub stix-core-relationship,
		relates downloaded,
		relates downloading;

	exploit sub stix-core-relationship,
		relates exploiting,
		relates exploited;

# 3.7 Common Relationships
# These are relation types that are shared by SDOs and SCOs

derivation sub relation,
	relates derived-from,
	relates deriving;

duplicate sub relation,
	relates duplicated-object;

relatedness sub relation,
	relates related-to;

## INFERRED RELATIONS ##

kill-chain sub relation,
	owns kill-chain-name,
	relates participating-kill-chain-phase;

inferred-mitigation sub mitigation;

##### ATTRIBUTES #####

stix-attribute-string sub attribute, value string, abstract,
	plays granular-marking:marked;

	stix-type sub stix-attribute-string;
	stix-id sub stix-attribute-string;
	object-marking-refs sub stix-attribute-string;
	created sub stix-attribute-string;
	modified sub stix-attribute-string;
	revoked sub stix-attribute-string;
	labels sub stix-attribute-string;
	confidence sub stix-attribute-string;
	langs sub stix-attribute-string;
	defanged sub stix-attribute-string;
	extensions sub stix-attribute-string;
	source-name sub stix-attribute-string;
	description sub stix-attribute-string;
	url sub stix-attribute-string;
	external-id sub stix-attribute-string;

	kill-chain-name sub stix-attribute-string;
	phase-name sub stix-attribute-string;
	name sub stix-attribute-string;
	sector sub stix-attribute-string;
	contact-information sub stix-attribute-string;
	indicator-type sub stix-attribute-string;
	pattern sub stix-attribute-string;
	pattern-type sub stix-attribute-string;
	pattern-version sub stix-attribute-string;
	valid-from sub stix-attribute-string;
	valid-until sub stix-attribute-string;

	malware-type sub stix-attribute-string;
	first-seen sub stix-attribute-string;
	last-seen sub stix-attribute-string;
	operating-system-refs sub stix-attribute-string;
	architecture-execution-envs sub stix-attribute-string;
	implementation-languages sub stix-attribute-string;
	capability sub stix-attribute-string;
	sample-refs sub stix-attribute-string;

	first-observed sub stix-attribute-string;
	last-observed sub stix-attribute-string;
	object sub stix-attribute-string;

	threat-actor-type sub stix-attribute-string;
	alias sub stix-attribute-string;
	first-seen sub stix-attribute-string;
	last-seen sub stix-attribute-string;
	stix-role sub stix-attribute-string;
	goals sub stix-attribute-string;
	sophistication sub stix-attribute-string;
	resource-level sub stix-attribute-string;
	primary-motivation sub stix-attribute-string;
	secondary-motivations sub stix-attribute-string;
	personal-motivations sub stix-attribute-string;

	first-seen sub stix-attribute-string;
	last-seen sub stix-attribute-string;
	summary sub stix-attribute-string;

	spec-version sub stix-attribute-string;

	attribute-key sub stix-attribute-string;
	values sub stix-attribute-string;
	modified-time sub stix-attribute-string;
	number-subkeys sub stix-attribute-string;
	values sub stix-attribute-string;
	objective sub stix-attribute-string;
	statement sub stix-attribute-string;
	action sub stix-attribute-string;
	tool-type sub stix-attribute-string; # OPEN VOCAB?
	tool-version sub stix-attribute-string;

	region sub stix-attribute-string; # open vocab TODO (10.21 Region Vocabulary)
	country sub stix-attribute-string;
	administrative-area sub stix-attribute-string;
	city sub stix-attribute-string;
	street-address sub stix-attribute-string;
	postal-code sub stix-attribute-string;

stix-attribute-long sub attribute, value long, abstract,
	plays granular-marking:marked;

	size  sub stix-attribute-long;
	count sub stix-attribute-long;
	number-observed sub stix-attribute-long;

stix-attribute-double sub attribute, value double, abstract,
	plays granular-marking:marked;

	latitude sub stix-attribute-double;
	longitude sub stix-attribute-double;
	precision sub stix-attribute-double;

stix-attribute-boolean sub attribute, value boolean, abstract,
	plays granular-marking:marked;

	is-family sub stix-attribute-boolean;

custom-attribute sub attribute, value string,
	owns attribute-type;
attribute-type sub attribute, value string;

##### Rules #####

# Description
# These two rules create a relation of type kill-chain between all the kill-chain-phase entities
# that have the same name. The first rule infers the relation, and the second rule infers the attribute
# kill-chain-name. We need to split this up into two rules as we can only infer one relation or one attribute
# per rule.
# Example:
# If we have many different kill-chain-phase entites that are called "Mitre Attack", but with different phases,
# we can infer a kill-chain relation between all the phases in that kill chain. This makes it easy for us to
# query across all the phases in that kill chain.
rule part-of-one-kill-chain-relation:
when {
	$kill-chain-1 isa kill-chain-phase, has kill-chain-name $x;
	$kill-chain-2 isa kill-chain-phase, has kill-chain-name $x;
	not {$kill-chain-1 is $kill-chain-2;};
} then {
	(participating-kill-chain-phase: $kill-chain-1, participating-kill-chain-phase: $kill-chain-2) isa kill-chain;
};
rule part-of-one-kill-chain-attribute-name:
when {
	$kill-chain-1 isa kill-chain-phase, has kill-chain-name $x;
	$kill-chain-2 isa kill-chain-phase, has kill-chain-name $x;
	not {$kill-chain-1 is $kill-chain-2;};
	$kill-chain (participating-kill-chain-phase: $kill-chain-1, participating-kill-chain-phase: $kill-chain-2) isa kill-chain;
} then {
	$kill-chain has $x;
};

# Description
# This rule infers that if X is using Y, and if Y is using Z, then that means that X is also using Z
# Example:
# If an intrusion set is using a malware, and if that malware is using a specific attack pattern,
# then we want to infer that that intrusion set is using that specific attack pattern
rule transitive-use:
when {
	$x isa stix-domain-object, has name $name1;
	$y isa stix-domain-object, has name $name2;
	$z isa stix-domain-object, has name $name3;
	$use1 (used-by: $x, used: $y) isa use;
	$use2 (used-by: $y, used: $z) isa use;
} then {
	(used-by: $x, used: $z) isa use;
};

# Description:
# If y has been attributed to x, and if y is using z, then we infer that x is using z also
# Example:
# If an identity has been attributed to a threat actor,
# and if that identity is using a malware, then we want to create a "use" relation
# between the threat actor and the malware; the threat actor is using the malware
# through the identiy
rule attribution-when-using:
when {
	(attributing: $x, attributed: $y) isa attribution;
	(used-by: $y, used: $z) isa use;
} then {
	(used-by: $x, used: $z) isa use;
};

# Description:
# If y has been attributed to x, and if y is targetting z, then we can infer that x is targetting z
# Example:
# If an attack pattern, for example "spear fishing", has been attributed to a specific identity named "Bravo",
# and if that attack pattern is targetting a location, for example New York,
# we can infer that the identiy "Bravo" is targeting the location New York
rule attribution-when-targetting:
when {
	(attributing: $x, attributed: $y) isa attribution;
	(targetting: $y, targetted: $z) isa target;
} then {
	(targetting: $x, targetted: $z) isa target;
};

# Description
# This rule infers a relation of type inferred-mitigation between a course of action and an intursion set, if the
# course of action mitigates an SDO which is used by an intrusion set. Note: as the relation "use" is transitive (see rule above),
# the SDO and the intrusion set may not be directly connected.
# Example:
# The entity type "course-of-action" with name "Restrict File and Directory Permissions" is mitigating against an entity "attack-pattern"
# with name "indicator Blocking". If that "attack-pattern" is being used by an entiy of type "intrusion-set" with name "BlackTech", then an i
# nferred relation of type "inferred-mitigation" will be created between the "course-of-action" and "the intrusion-set"
rule mitigating-course-of-action-with-intrusion-set:
when {
	$course-of-action isa course-of-action, has name $name1;
	$sdo isa stix-domain-object, has name $name2;
	$intrusion-set isa intrusion-set, has name $name3;
	$mitigation (mitigating: $course-of-action, mitigated: $sdo) isa mitigation;
	$use (used: $sdo, used-by: $intrusion-set) isa use;

} then {
	(mitigating: $course-of-action, mitigated: $intrusion-set) isa inferred-mitigation;
};

      """
  Given transaction commits

  # Note: These tests verify only the ability to create rules, and are not concerned with their application.

  Scenario: when a rule attaches an attribute to a type that can't have that attribute, an error is thrown
    When session opens transaction of type: write
    When  typeql undefine
      """
undefine rule mitigating-course-of-action-with-intrusion-set;
      """
    Then transaction commits

#
#  Background: Initialise a session and transaction for each scenario
#    Given connection has been opened
#    Given connection does not have any database
#    Given connection create database: typedb
#    Given connection open schema session for database: typedb
#    Given session opens transaction of type: write
#    Given typeql define
#      """
##############################################################################
##                                                                           #
##         CD-ROM: cORPORATE dESIGNED - rESOURCE oNTOLOGY mODEL                 #
##                                                                           #
##############################################################################
#define
#
#################
### Attributes ##
#################
#
#description sub attribute, value string;
#
#health_state sub attribute, value boolean;
#
#property sub attribute, abstract, value double,
#    plays featuring:feature;
#
#    rate sub property, abstract;
#        fps sub rate;
#    capacity sub property;
#    format sub property, abstract;
#        ROS_message sub format;
#    throughput sub property, abstract;
#        power sub throughput;
#    unit sub property, abstract;
#        wavelength sub unit;
#        intensity sub unit;
#        voltage sub unit;
#        m_s sub unit;
#    location sub property, abstract;
#        ROS_topic sub location;
#    quantifier sub property, abstract;
#        minimal sub quantifier;
#        exact sub quantifier;
#        maximal sub quantifier;
#
#sentence sub attribute, value string;
#leak_detected sub attribute, value double;
#
##properties that have no value double.
#performance sub attribute, abstract, value double, #specify any type of performance
#    plays featuring:feature;
#
#quality sub performance;
#    average_quality sub attribute, abstract, value double;
#        average_lumen sub average_quality;
#        average_decibel sub average_quality;
#        average_contrast sub average_quality;
#        average_chance_correct_detection sub average_quality;
#        average_size sub average_quality;
#        average_frequency sub average_quality;
#        average_molar_volume sub average_quality;
#        # average_signal_to_noise sub average_quality;
#        average_words sub average_quality;
#        average_beaufort sub average_quality;
#
#    minimal_quality sub attribute, abstract, value double;
#        min_lumen sub minimal_quality,
#            plays featuring:feature;
#        min_decibel sub minimal_quality,
#            plays featuring:feature;
#        min_contrast sub minimal_quality,
#            plays featuring:feature;
#        min_chance_correct_detection sub minimal_quality,
#            plays featuring:feature;
#        min_size sub minimal_quality,
#            plays featuring:feature;
#        min_frequency sub minimal_quality,
#            plays featuring:feature;
#        min_molar_volume sub minimal_quality,
#            plays featuring:feature;
#        # min_signal_to_noise sub minimal_quality,
#        #     plays featuring:feature;
#        min_words sub minimal_quality,
#            plays featuring:feature;
#        min_beaufort sub minimal_quality,
#            plays featuring:feature;
#
#    maximal_quality sub attribute, abstract, value double;
#        max_lumen sub maximal_quality,
#            plays featuring:feature;
#        max_decibel sub maximal_quality,
#            plays featuring:feature;
#        max_contrast sub maximal_quality,
#            plays featuring:feature;
#        max_chance_correct_detection sub maximal_quality,
#            plays featuring:feature;
#        max_size sub maximal_quality,
#            plays featuring:feature;
#        max_frequency sub maximal_quality,
#            plays featuring:feature;
#        max_molar_volume sub maximal_quality,
#            plays featuring:feature;
#        # max_signal_to_noise sub maximal_quality,
#        #     plays featuring:feature;
#        max_words sub maximal_quality,
#            plays featuring:feature;
#        max_beaufort sub maximal_quality,
#            plays featuring:feature;
#
#
#
#######################
### ROLES & RELATION ##
#######################
#processing sub relation,
#    relates input, # any creation
#    relates executor, # component in question
#    relates output, # any creation
#    relates context, #sensing_context
#    relates why, #in TO
#    plays processing_requirement:input;
#    #relates requested_behaviour, #in TO
#    #owns planned_status; # in TO
#
#requirement sub relation,
#    relates call,
#    relates petitioner,
#    relates product;
#
#    functional_requirement sub requirement,
#        relates input as call, # data
#        # relates petitioner, # component in question
#        relates output as product;  # data
#
#    non_functional_requirement sub requirement,
#        relates service as call, # resource
#        # relates petitioner, # component in question
#        relates outcome as product; # data
#
#    processing_requirement sub requirement,
#        relates input as call, # resource
#        # relates petitioner, # component in question
#        relates output as product; # data
#
#    environmental_requirement sub requirement,
#        relates state as call, # physical quantity
#        # relates petitioner, # component in question
#        relates outcome as product; # data
#        # plays state;
#
#realizing sub relation,
#    relates provider, # any creation
#    relates request;  # any creation
#
#featuring sub relation,
#    relates subject,
#    relates feature,
#    plays requirement:call,
#    plays environmental_requirement:state,
#    plays non_functional_requirement:service,
#    plays functional_requirement:input;
#
#sensing_context sub relation,
#    owns description,
#    relates physical_context,
#    plays processing:context;
#
###############
### ENTITIES ##
###############
#
#Creation sub entity, abstract,
#    owns description,
#    # owns property, #dit mag niet omdat anders alle subs deze abstract properties inheriten. TODO: check of dit oke is.
#    # owns performance, #dit mag niet omdat anders alle subs deze abstract properties inheriten. TODO: check of dit oke is.
#    plays featuring:subject,
#    plays requirement:call,
#    plays requirement:product,
#    plays realizing:provider,
#    plays realizing:request,
#    plays processing:input,
#    plays processing:output;
#
#    Data sub Creation,
#        plays functional_requirement:output,
#        plays non_functional_requirement:outcome,
#        plays environmental_requirement:outcome;
#
#        Signal sub Data,
#            owns wavelength, #TODO: check of all deze properties kloppen (bij alle niet abstract creations)
#            owns intensity,
#            owns voltage,
#            owns fps;
#        Information sub Data,
#            owns ROS_topic,
#            owns ROS_message;
#        Knowledge sub Data,
#            owns ROS_topic,
#            owns ROS_message;
#
#        # added for 2022 use-case
#        Image sub Data,
#            owns min_contrast,
#            owns max_contrast,
#            owns average_contrast,
#            plays functional_requirement:input,
#            # plays environmental_requirement:outcome, # BUG: already included in abstract definition of Creation
#            # plays featuring:subject, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        Spectrogram sub Data,
#            owns min_frequency,
#            owns max_frequency,
#            owns average_frequency,
#            plays functional_requirement:input,
#            # plays environmental_requirement:outcome, # BUG: already included in abstract definition of Creation
#            # plays featuring:subject, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        LeakDetection sub Data,
#            owns min_chance_correct_detection,
#            owns max_chance_correct_detection,
#            owns average_chance_correct_detection,
#            plays functional_requirement:input,
#            # plays functional_requirement:output, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        GasDetection sub Data,
#            owns min_chance_correct_detection,
#            owns max_chance_correct_detection,
#            owns average_chance_correct_detection,
#            plays functional_requirement:input,
#            # plays functional_requirement:output, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        ObjectDetection sub Data,
#            owns min_chance_correct_detection,
#            owns max_chance_correct_detection,
#            owns average_chance_correct_detection,
#            plays functional_requirement:input,
#            # plays functional_requirement:output, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        HumanDetection sub Data,
#            owns min_chance_correct_detection,
#            owns max_chance_correct_detection,
#            owns average_chance_correct_detection,
#            plays functional_requirement:input,
#            # plays functional_requirement:output, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        ManometerValueDetection sub Data,
#            owns min_chance_correct_detection,
#            owns max_chance_correct_detection,
#            owns average_chance_correct_detection,
#            plays functional_requirement:input,
#            # plays functional_requirement:output, # BUG: already included in abstract definition of Creation
#            plays processing_requirement:output;
#
#        Sentence_msg sub Data,
#            owns min_words,
#            owns max_words,
#            owns average_words,
#            owns sentence,
#            plays functional_requirement:input,
#            plays processing_requirement:output;
#
#        Spoken_msg sub Data,
#            owns average_decibel,
#            plays functional_requirement:input,
#            plays processing_requirement:output;
#
#        IncidentReport sub Data,
#            owns leak_detected,
#            plays functional_requirement:input,
#            plays processing_requirement:output;
#
#    Resource sub Creation;
#
#        ElectricPower sub Resource,
#            owns power,
#            owns voltage;
#        ComputingPower sub Resource,
#            owns power,
#            owns voltage;
#
#    PhysicalQuantity sub Creation,
#        plays functional_requirement:input;
#        # plays functional_requirement:output; # BUG: already included in abstract definition of Creation
#
#        Light sub PhysicalQuantity,
#            owns intensity,
#            owns average_lumen;
#        Speed sub PhysicalQuantity,
#            owns m_s;
#        Sound sub PhysicalQuantity,
#            owns average_decibel;
#        Gas sub PhysicalQuantity,
#            owns average_molar_volume;
#        Wind sub PhysicalQuantity,
#            owns average_beaufort;
#
#    PhysicalState sub Creation,
#         plays processing_requirement:output;
#
#PhyscalConcepts sub Creation, abstract,
#    # owns description,
#    plays environmental_requirement:state;
#
#        LightIntensity sub PhyscalConcepts,
#            owns min_lumen,
#            owns max_lumen,
#            owns average_lumen,
#            plays sensing_context:physical_context;
#        AmbientNoise sub PhyscalConcepts,
#            owns min_decibel,
#            owns max_decibel,
#            owns average_decibel,
#            plays sensing_context:physical_context;
#        RoomSize sub PhyscalConcepts,
#            owns min_size,
#            owns max_size,
#            owns average_size,
#            plays sensing_context:physical_context;
#        AmbientGas sub PhyscalConcepts,
#            owns min_molar_volume, #please check de eenheid
#            owns max_molar_volume,
#            owns average_molar_volume,
#            plays sensing_context:physical_context;
#        AmbientWind sub PhyscalConcepts,
#            owns min_beaufort, #please check de eenheid
#            owns max_beaufort,
#            owns average_beaufort,
#            plays sensing_context:physical_context;
#
#
#Component sub entity,
#    owns description,
#    owns health_state,
#    plays requirement:petitioner,
#    plays processing:executor;
#
#    Sensor sub Component;
#       AcousticCamera sub Sensor;
#       Camera sub Sensor;
#       GasSensor sub Sensor;
#       Speaker sub Sensor;
#    Actuator sub Component;
#    FunctionalComponent sub Component;
#        GasLeakDetector sub FunctionalComponent;
#        ObjectDetector sub FunctionalComponent;
#        ManometerDetector sub FunctionalComponent;
#        HumanDetector sub FunctionalComponent;
#        IncidentGenerator sub FunctionalComponent;
#    ApplianceComponent sub Component;
#
#      """
#
#
#  # Note: These tests verify only the ability to create rules, and are not concerned with their application.
#
#  Scenario: a rule can infer both an attribute and its ownership
#    Given typeql define
#      """
#      define
#      rule processing_is_transitive2:
#    when {
#      $data_in isa Data; $data_out isa Data; $data_out2 isa Data;
#        (input: $data_in, executor: $comp1, output:$data_out) isa processing;
#        (input: $data_out, executor: $comp2, output:$data_out2) isa processing;
#    } then {
#        (input: $data_in, executor: $comp1, executor: $comp2, output:$data_out2) isa processing;
#    };
#      """
#    Then transaction commits
#
