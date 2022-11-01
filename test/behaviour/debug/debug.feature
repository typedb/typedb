#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

Feature: Debugging Space

  Background: Open connection and create a simple extensible schema
    Given connection has been opened
    Given connection does not have any database
    Given connection create database: typedb
    Given connection open schema session for database: typedb
    Given session opens transaction of type: write

    Given typeql define
      """
      define
      person sub entity,
        plays friendship:friend,
        plays employment:employee,
        owns name,
        owns age,
        owns ref @key;
      company sub entity,
        plays employment:employer,
        owns name,
        owns ref @key;
      friendship sub relation,
        relates friend,
        owns ref @key;
      employment sub relation,
        relates employee,
        relates employer,
        owns ref @key;
      name sub attribute, value string;
      age sub attribute, value long;
      ref sub attribute, value long;
      """
    Given transaction commits

    Given connection close all sessions
    Given connection open data session for database: typedb
    Given session opens transaction of type: write


  Scenario Outline: sorting and query predicates produce order ignoring types
    Given connection close all sessions
    Given connection open schema session for database: typedb
    Given session opens transaction of type: write
    Given typeql define
      """
      define
      <firstAttr> sub attribute, value <firstType>, owns ref @key;
      <secondAttr> sub attribute, value <secondType>, owns ref @key;
      <thirdAttr> sub attribute, value <thirdType>, owns ref @key;
      <fourthAttr> sub attribute, value <fourthType>, owns ref @key;
      """
    Given transaction commits

    Given connection close all sessions
    Given connection open data session for database: typedb
    Given session opens transaction of type: write
    Given typeql insert
      """
      insert
      $first1 <firstValue1> isa <firstAttr>, has ref 0;
      $first2 <firstValue2> isa <firstAttr>, has ref 1;
      $second <secondValue> isa <secondAttr>, has ref 2;
      $third <thirdValue> isa <thirdAttr>, has ref 3;
      $fourth <fourthValuePivot> isa <fourthAttr>, has ref 4;
      """
    Given transaction commits

    Given session opens transaction of type: read

    # ascending
    When get answers of typeql match
      """
      match $x isa $t; $t owns ref;
      get $x;
      sort $x asc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:2 |
      | key:ref:1 |
      | key:ref:4 |
      | key:ref:0 |
      | key:ref:3 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x < <fourthValuePivot>;
      get $x;
      sort $x asc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:2 |
      | key:ref:1 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x <= <fourthValuePivot>;
      get $x;
      sort $x asc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:2 |
      | key:ref:1 |
      | key:ref:4 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x > <fourthValuePivot>;
      get $x;
      sort $x asc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:0 |
      | key:ref:3 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x >= <fourthValuePivot>;
      get $x;
      sort $x asc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:4 |
      | key:ref:0 |
      | key:ref:3 |

    # descending
    When get answers of typeql match
      """
      match $x isa $t; $t owns ref;
      get $x;
      sort $x desc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:3 |
      | key:ref:0 |
      | key:ref:4 |
      | key:ref:1 |
      | key:ref:2 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x < <fourthValuePivot>;
      get $x;
      sort $x desc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:1 |
      | key:ref:2 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x <= <fourthValuePivot>;
      get $x;
      sort $x desc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:4 |
      | key:ref:1 |
      | key:ref:2 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x > <fourthValuePivot>;
      get $x;
      sort $x desc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:3 |
      | key:ref:0 |

    When get answers of typeql match
      """
      match $x isa $t; $t owns ref; $x >= <fourthValuePivot>;
      get $x;
      sort $x desc;
      """
    Then order of answer concepts is
      | x         |
      | key:ref:3 |
      | key:ref:0 |
      | key:ref:4 |

    Examples:
      # NOTE: fourthValuePivot is expected to be the middle of the sort order (pivot)
      | firstAttr   | firstType | firstValue1 | firstValue2 | secondAttr | secondType | secondValue | thirdAttr | thirdType | thirdValue | fourthAttr | fourthType | fourthValuePivot |
      | score       | long      | 4           | -38         | quantity   | long       | -50         | area      | long      | 100        | length     | long       | 0                |
#      | correlation | double    | 4.1         | -38.999     | quantity   | double     | -101.4      | area      | double    | 110.0555   | length     | double     | 0.5              |
#      # mixed double-long data
#      | score       | long      | 4           | -38         | quantity   | double     | -55.123     | area      | long      | 100        | length     | double     | 0.5              |
#      | dob         | datetime  | 2970-01-01   | 1970-02-01 | start-date | datetime   | 1970-01-01  | end-date  | datetime  | 3100-11-20 | last-date  | datetime   | 2000-08-03       |
