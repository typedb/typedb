#
# Copyright (C) 2021 Grakn Labs
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

Name: grakn-core-server
Version: devel
Release: 1
Summary: Grakn Core (server)
URL: https://grakn.ai
License: AGPLv3
AutoReqProv: no

Source0: {_grakn-core-server-rpm-tar.tar.gz}

Requires: java-11-openjdk-headless
Requires: grakn-bin = %{@graknlabs_common}

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
%attr(777, root, root) /var/lib/grakn/
