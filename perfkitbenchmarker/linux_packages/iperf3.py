# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Module containing iperf3 installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import INSTALL_DIR


PACKAGE_NAME = 'iperf3'
IPERF3_ZIP = '3.7.zip'
IPERF3_DIR = 'iperf3-3.7-RELEASE'
PREPROVISIONED_DATA = {
    IPERF3_ZIP:
        'a591cb8d7d77727b1febc9aef8506be528b234e10a52ecb4504608d3d9347d05'
}
PACKAGE_DATA_URL = {
    IPERF3_ZIP: posixpath.join('https://github.com/esnet/iperf/archive',
                              IPERF3_ZIP)}

def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.InstallPackages('iperf3')


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  try:
    vm.InstallEpelRepo()
    _Install(vm)
  # RHEL 7 does not have an iperf package in the standard/EPEL repositories
  except errors.VirtualMachine.RemoteCommandError:
    vm.Install('build_tools')
    vm.Install('unzip')
    vm.InstallPreprovisionedPackageData(
        PACKAGE_NAME, PREPROVISIONED_DATA.keys(), INSTALL_DIR)
    vm.RemoteCommand(
        'cd %s; unzip %s; cd %s; ./configure; make; sudo make install' % (
            INSTALL_DIR, IPERF3_ZIP, IPERF3_DIR))


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
