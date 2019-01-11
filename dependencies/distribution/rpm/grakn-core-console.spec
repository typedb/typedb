Name: grakn-core-console
Version: devel
Release: 1
Summary: Grakn Core (console)
URL: https://grakn.ai
License: Apache License, v2.0

Source0: {_grakn-core-console-rpm-tar.tar.gz}

Requires: java-1.8.0-openjdk-headless
Requires: grakn-core-bin

%description
Grakn Core (server) - description

%prep

%build

%install
mkdir -p %{buildroot}
tar -xvf {_grakn-core-console-rpm-tar.tar.gz} -C %{buildroot}

%files

/opt/grakn/
