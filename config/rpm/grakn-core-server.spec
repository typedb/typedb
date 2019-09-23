Name: grakn-core-server
Version: devel
Release: 1
Summary: Grakn Core (server)
URL: https://grakn.ai
License: Apache License, v2.0
AutoReqProv: no

Source0: {_grakn-core-server-rpm-tar.tar.gz}

Requires: java-1.8.0-openjdk-headless
Requires: grakn-bin >= %{@graknlabs_common}

%description
Grakn Core (server) - description

%prep

%build

%install
mkdir -p %{buildroot}
tar -xvf {_grakn-core-server-rpm-tar.tar.gz} -C %{buildroot}
rm -fv {_grakn-core-server-rpm-tar.tar.gz}

%files

/opt/grakn/
/var/lib/grakn/
%attr(777, root, root) /opt/grakn/core/server/services/cassandra/cassandra.yaml
%attr(777, root, root) /var/lib/grakn/db/cassandra/
%attr(777, root, root) /var/lib/grakn/db/queue/
