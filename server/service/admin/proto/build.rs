/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

fn main() -> std::io::Result<()> {
    let protos = vec!["admin_service.proto"];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    tonic_build::configure()
        .server_mod_attribute(".", "#[allow(non_camel_case_types)]")
        .client_mod_attribute(".", "#[allow(non_camel_case_types)]")
        .compile_protos(&protos, &["."])
}
