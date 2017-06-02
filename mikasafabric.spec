%if 0%{?rhel} && 0%{?rhel} <= 5
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%endif

Summary:       mikasafabric for MySQL is patched MySQL Fabric by GMO Media, Inc.
Name:          mikasafabric
Version:       0.6.0
Release:       1%{?dist}
License:       GPLv2
Group:         Development/Libraries
URL:           https://github.com/gmo-media/mikasafabric
Source0:       https://github.com/gmo-media/mikasafabric/archive/%{version}.tar.gz#/mikasafabric-%{version}.tar.gz
BuildArch:     noarch
BuildRequires: python-devel > 2.6
Requires:       mysql-connector-python >= 2.0.4
Conflicts:      mysql-connector-python = 2.1.0, mysql-connector-python = 2.1.1
BuildRoot:     %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)


%description

mikasafabric for MySQL is clone of MySQL Fabric(included MySQL Utilities) 1.5.6.

- Fixes some bugs which were left.
- Improvemoents for using persistent connection environment(Including Replication via MySQL Router).
- Add useful information into some of mysqlfabric subcommands.
- mikasafabric for MySQL only supports MySQL 5.7.5 and later(Because using offline_mode).


%prep
%setup -q

%build
%{__python} setup.py build

%install
rm -rf %{buildroot}

%{__python} setup.py install --prefix=%{_prefix} --skip-build --root %{buildroot}
#install -d %{buildroot}%{_mandir}/man1
#%{__python} setup.py install_man --root %{buildroot}
ln -s mysqlfabric %{buildroot}/%{_prefix}/bin/mikasafabric

# Shipped in c/python
rm -f  %{buildroot}%{python_sitelib}/mysql/__init__.py*

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root, -)
%doc CHANGES_*.txt LICENSE.txt README_*.txt
%config(noreplace) %{_sysconfdir}/mysql/fabric.cfg
%dir %{_sysconfdir}/mysql
%{_bindir}/mysqlfabric
%{_bindir}/mikasafabric
%{python_sitelib}/mysql
%if 0%{?rhel} > 5 || 0%{?fedora} > 12
%{python_sitelib}/mikasafabric-*.egg-info
%endif
#%{_mandir}/man1/mysql*.1*

%changelog
* Tue Aug 03 2016 yoku0825 <yoku0825@gmail.com> - 0.0.1
- Forked from mysql-utilities 1.5.6

* Thu Sep 10 2015 Nuno Mariz <nuno.mariz@oracle.com> - 1.5.6-1
- Updated the Version to 1.5.6

* Thu Jun 18 2015 Murthy Narkedimilli <murthy.narkedimilli@oracle.com> - 1.5.5-1
- Updated the SuSe version to 1315 to handle mysql_utilities-*.egg-info file.
- Updated the Version to 1.5.5

* Thu Jan 29 2015 Israel Gomez <israel.gomez@oracle.com> - 1.5.4-1
- Updated the Version to 1.5.4

* Wed Dec 17 2014 Murthy Narkedimilli <murthy.narkedimilli@oracle.com> - 1.6.1-1
- Added new utilities binaries mysqlbinlogpurge, mysqlbinlogrotate and mysqlslavetrx
- Changed the build prefix for SLES platform.
- Added condition to include the mysql_utilities-*.egg-info file

* Mon Oct 20 2014 Israel Gomez <israel.gomez@oracle.com> - 1.5.3-1
- Updated the Version to 1.5.3

* Tue Sep 09 2014 Balasubramanian Kandasamy <balasubramanian.kandasamy@oracle.com> - 1.6.0-1
- Added new utilities mysqlgrants and mysqlbinlogmove

* Mon Aug 11 2014 Nelson Goncalves <nelson.goncalves@oracle.com> - 1.6.0-1
- Updated the Version to 1.6.0

* Mon Aug 11 2014 Chuck Bell <chuck.bell@oracle.com> - 1.5.2-1
- Updated the Version to 1.5.2

* Tue Jul 01 2014 Chuck Bell <chuck.bell@oracle.com> - 1.5.1-1
- Updated the Version to 1.5.1

* Mon May 26 2014  Murthy Narkedimilli <murthy.narkedimilli@oracle.com> - 1.5.0-1
- Updated the Version to 1.5.0

* Wed Feb 26 2014  Balasubramanian Kandasamy <balasubramanian.kandasamy@oracle.com> - 1.4.2-1
- Updated for 1.4.2
- Add extra subpackage

* Fri Jan 03 2014  Balasubramanian Kandasamy <balasubramanian.kandasamy@oracle.com> - 1.3.6-1
- initial package
