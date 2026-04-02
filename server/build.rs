fn main() -> std::io::Result<()> {
    let protos = vec!["proto/admin_service.proto"];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=build.rs");
    tonic_build::configure()
        .server_mod_attribute(".", "#[allow(non_camel_case_types)]")
        .client_mod_attribute(".", "#[allow(non_camel_case_types)]")
        .compile_protos(&protos, &["."])
}
