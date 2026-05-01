**This is an alpha release for CLUSTERED TypeDB 3.x. Do not use this as a stable version of TypeDB.**
**Instead, reference a non-alpha release of the same major and minor versions.**

**Download from TypeDB Package Repository:**

[Distributions for 3.10.0-alpha-1](https://cloudsmith.io/~typedb/repos/public-release/packages/?q=name%3A%5Etypedb-all+version%3A3.10.0-alpha-1)

**Pull the Docker image:**

```docker pull typedb/typedb:3.10.0-alpha-1```

## New Features

### Extend TypeDB address configuration

Introduce better extensibility to TypeDB CE, enabling TypeDB services to be extended and modified by distributions with additional functionality (e.g., clustered distributions for TypeDB Enterprise/Cloud).

- **Change server configuration.** Rename `server.address` and `server.http.address` to `listen-address`es (`address` aliases are still accepted for backwards compatibility). Add `advertise-address` configuration for both gRPC and HTTP endpoints, distinguishing the address the server binds to from the address clients use to connect (address advertised to the clients). Supports NAT, reverse proxy, and load balancer deployments. Specifying both `listen-address` and `address` will be rejected by the system.
- Add an admin CLI tool (`typedb admin`) bundled with all distributions. The admin tool communicates with the server through a localhost-only gRPC endpoint (default port 1728). Supports interactive REPL mode, one-shot (`--command`), and script (`--script`) execution modes. Commands: `server version`, `server status`, `help`, `exit`.
- Add `server.admin` configuration section (default: enabled on port 1728).
- Restructure server startup output to a multi-line format showing each endpoint (gRPC, HTTP, Admin) with TLS status:

```
=====================================================================================
     ________ __      __ _____    _______  _____    _____       _______  _______
    |__    __|\  \  /  /|   _  \ |   _   ||   _  \ |   _  \    |   _   ||   _   |
       |  |    \  \/  / |  | |  ||  | |__||  | |  ||  | |  |   |  | |  ||  | |__|
       |  |     \    /  |  |/  / |  |___  |  | |  ||  |/  /    |  | |__||  |___
       |  |      |  |   |   __/  |   ___| |  | |  ||   _  \    |  |  __ |   ___|
       |  |      |  |   |  |     |  |  __ |  | |  ||  | |  |   |  | |  ||  |  __
       |  |      |  |   |  |     |  |_|  ||  |/  / |  |/  /    |  |_|  ||  |_|  |
       |__|      |__|   |__|     |_______||_____/  |_____/     |_______||_______|

=====================================================================================

Running TypeDB CE 3.8.3 in development mode.
Serving:
  gRPC:  0.0.0.0:1729 (connect via 127.0.0.1:1729)
  HTTP:  0.0.0.0:8000 (connect via http://127.0.0.1:8000)
  Admin: 127.0.0.1:1728 (localhost only)
TLS: disabled
  WARNING: TLS NOT ENABLED. Credentials are transmitted unencrypted in plaintext.
  Drivers must be configured to connect *without TLS*.
```

### Reorganize TypeDB services for better extensibility

- Expose config extensibility for distribution-specific settings. Cluster distributions need to parse additional config sections (e.g., server.clustering) from the same YAML file without modifying the base Config struct.
- Add `server_version` and `server_status` RPCs to the admin protocol, returning distribution info and endpoint addresses.
- Restructure the `server` package, introducing `Operator`s that serve as a middleware between network services and inner TypeDB components (`DatabaseManager`, `UserManager`, etc.).
- Introduce `ErrorResponseCategory` for mapping server state errors to appropriate gRPC and HTTP status codes and HTTP 307 redirect support (currently cannot be used in TypeDB for most of the endpoints due to local-only authentication -- instead, an additional `AuthenticatedRedirect` is introduced).
- Add close signals for `TransactionService`s to extend the ability of the system to interrupt existing transactions without shutting down.
