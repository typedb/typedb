# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

Feature: Debugging Space

  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

  Background: Open connection and create a simple extensible schema
    Given typedb starts
    Given connection opens with default authentication
    Given connection is open: true
    Given connection reset database: typedb
    Given connection open schema transaction for database: typedb

  Scenario: hanging forever
    Given typeql schema query
      """
      define
        entity person, owns name, owns age;
        attribute name, value string;
        attribute age, value long;
      """
    Given transaction commits
    Given connection open write transaction for database: typedb

    When typeql write query
      """
      insert
        $p0 isa person, has name "John", has age 0;
        $p1 isa person, has name "John", has age 1;
        $p2 isa person, has name "John", has age 2;
        $p3 isa person, has name "John", has age 3;
        $p4 isa person, has name "John", has age 4;
        $p5 isa person, has name "John", has age 5;
        $p6 isa person, has name "John", has age 6;
        $p7 isa person, has name "John", has age 7;
        $p8 isa person, has name "John", has age 8;
        $p9 isa person, has name "John", has age 9;
        $p10 isa person, has name "John", has age 10;
        $p11 isa person, has name "John", has age 11;
        $p12 isa person, has name "John", has age 12;
        $p13 isa person, has name "John", has age 13;
        $p14 isa person, has name "John", has age 14;
        $p15 isa person, has name "John", has age 15;
        $p16 isa person, has name "John", has age 16;
        $p17 isa person, has name "John", has age 17;
        $p18 isa person, has name "John", has age 18;
        $p19 isa person, has name "John", has age 19;
        $p20 isa person, has name "John", has age 20;
        $p21 isa person, has name "John", has age 21;
        $p22 isa person, has name "John", has age 22;
        $p23 isa person, has name "John", has age 23;
        $p24 isa person, has name "John", has age 24;
        $p25 isa person, has name "John", has age 25;
        $p26 isa person, has name "John", has age 26;
        $p27 isa person, has name "John", has age 27;
        $p28 isa person, has name "John", has age 28;
        $p29 isa person, has name "John", has age 29;
        $p30 isa person, has name "John", has age 30;
        $p31 isa person, has name "John", has age 31;
        $p32 isa person, has name "John", has age 32;
        $p33 isa person, has name "John", has age 33;
        $p34 isa person, has name "John", has age 34;
        $p35 isa person, has name "John", has age 35;
        $p36 isa person, has name "John", has age 36;
        $p37 isa person, has name "John", has age 37;
        $p38 isa person, has name "John", has age 38;
        $p39 isa person, has name "John", has age 39;
        $p40 isa person, has name "John", has age 40;
        $p41 isa person, has name "John", has age 41;
        $p42 isa person, has name "John", has age 42;
        $p43 isa person, has name "John", has age 43;
        $p44 isa person, has name "John", has age 44;
        $p45 isa person, has name "John", has age 45;
        $p46 isa person, has name "John", has age 46;
        $p47 isa person, has name "John", has age 47;
        $p48 isa person, has name "John", has age 48;
        $p49 isa person, has name "John", has age 49;
      """

    When get answers of typeql read query
      """
      match
        $p0 isa person, has name "John", has age 0;
        $p1 isa person, has name "John", has age 1;
        $p2 isa person, has name "John", has age 2;
        $p3 isa person, has name "John", has age 3;
        $p4 isa person, has name "John", has age 4;
        $p5 isa person, has name "John", has age 5;
        $p6 isa person, has name "John", has age 6;
        $p7 isa person, has name "John", has age 7;
        $p8 isa person, has name "John", has age 8;
        $p9 isa person, has name "John", has age 9;
        $p10 isa person, has name "John", has age 10;
        $p11 isa person, has name "John", has age 11;
        $p12 isa person, has name "John", has age 12;
        $p13 isa person, has name "John", has age 13;
        $p14 isa person, has name "John", has age 14;
        $p15 isa person, has name "John", has age 15;
        $p16 isa person, has name "John", has age 16;
        $p17 isa person, has name "John", has age 17;
        $p18 isa person, has name "John", has age 18;
        $p19 isa person, has name "John", has age 19;
        $p20 isa person, has name "John", has age 20;
        $p21 isa person, has name "John", has age 21;
        $p22 isa person, has name "John", has age 22;
        $p23 isa person, has name "John", has age 23;
        $p24 isa person, has name "John", has age 24;
        $p25 isa person, has name "John", has age 25;
        $p26 isa person, has name "John", has age 26;
        $p27 isa person, has name "John", has age 27;
        $p28 isa person, has name "John", has age 28;
        $p29 isa person, has name "John", has age 29;
        $p30 isa person, has name "John", has age 30;
        $p31 isa person, has name "John", has age 31;
        $p32 isa person, has name "John", has age 32;
        $p33 isa person, has name "John", has age 33;
        $p34 isa person, has name "John", has age 34;
        $p35 isa person, has name "John", has age 35;
        $p36 isa person, has name "John", has age 36;
        $p37 isa person, has name "John", has age 37;
        $p38 isa person, has name "John", has age 38;
        $p39 isa person, has name "John", has age 39;
        $p40 isa person, has name "John", has age 40;
        $p41 isa person, has name "John", has age 41;
        $p42 isa person, has name "John", has age 42;
        $p43 isa person, has name "John", has age 43;
        $p44 isa person, has name "John", has age 44;
        $p45 isa person, has name "John", has age 45;
        $p46 isa person, has name "John", has age 46;
        $p47 isa person, has name "John", has age 47;
        $p48 isa person, has name "John", has age 48;
        $p49 isa person, has name "John", has age 49;
      """
#    Then answer contains document:
#    """
#    {
#        "0": 0,
#        "1": 1,
#        "2": 2,
#        "3": 3,
#        "4": 4,
#        "5": 5
#    }
#    """
