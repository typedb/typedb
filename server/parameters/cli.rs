use clap::Parser;

/// TypeDB Core usage
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CLIArgs {
    /// Enable/disable in-flight encryption. Specify to enable, or leave out to disable
    #[arg(long="server.encryption.enabled",)]
    pub server_encryption_enabled: bool,

    /// Encryption certificate in PEM format. Must be supplied if encryption is enabled
    #[arg(long="server.encryption.cert", value_name="FILE")]
    pub server_encryption_cert: Option<String>,

    /// Encryption certificate key. Must be supplied if encryption is enabled
    #[arg(long="server.encryption.cert-key", value_name="FILE")]
    pub server_encryption_cert_key: Option<String>,

    /// Encryption CA in PEM format.
    #[arg(long="server.encryption.root-ca", value_name="FILE")]
    pub server_encryption_root_ca: Option<String>,

}