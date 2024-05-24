/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use ir::conjunction::Conjunction;

#[test]
fn build_conjunction_constraints() {
    let mut conjunction = Conjunction::new_root();

    let var_person = conjunction.get_or_declare_variable(&"person").unwrap();
    let var_name = conjunction.get_or_declare_variable(&"name").unwrap();
    let var_person_type = conjunction.get_or_declare_variable(&"person_type").unwrap();
    let var_name_type = conjunction.get_or_declare_variable(&"name_type").unwrap();

    conjunction.constraints().add_isa(var_person, var_person_type);
    conjunction.constraints().add_has(var_person, var_name);
    conjunction.constraints().add_isa(var_name, var_name_type);
    conjunction.constraints().add_type(var_person_type, "person");
    conjunction.constraints().add_type(var_name_type, "name");

    dbg!(&conjunction);
}