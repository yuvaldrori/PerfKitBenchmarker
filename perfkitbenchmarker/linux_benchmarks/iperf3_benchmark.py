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

"""Runs plain Iperf.

Docs:
http://iperf.fr/

Runs Iperf to collect network throughput.
"""

from pprint import pprint

import logging
import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

flags.DEFINE_integer('iperf3_sending_thread_count', 1,
                     'Number of connections to make to the '
                     'server for sending traffic.',
                     lower_bound=1)
flags.DEFINE_integer('iperf3_runtime_in_seconds', 30,
                     'Number of seconds to run iperf3.',
                     lower_bound=1)
flags.DEFINE_integer('iperf3_bandwidth_in_mbits', 100,
                     'Bandwidth in Mbits.',
                     lower_bound=1)
flags.DEFINE_integer('iperf3_timeout', None,
                     'Number of seconds to wait in '
                     'addition to iperf3 runtime before '
                     'killing iperf3 client command.',
                     lower_bound=1)

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'iperf3'
BENCHMARK_CONFIG = """
iperf3:
  description: Run iperf3
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
"""

IPERF_PORT = 20000
IPERF_RETRIES = 5


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install iperf3 and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  if len(vms) != 2:
    raise ValueError(
        'iperf3 benchmark requires exactly two machines, found {0}'.format(len(
            vms)))

  for vm in vms:
    vm.Install('iperf3')
    if vm_util.ShouldRunOnExternalIpAddress():
      vm.AllowPort(IPERF_PORT)
    stdout, _ = vm.RemoteCommand(('nohup iperf3 --server --port %s &> /dev/null'
                                  '& echo $!') % IPERF_PORT)
    # TODO store this in a better place once we have a better place
    vm.iperf3_server_pid = stdout.strip()
    stdout, _ = vm.RemoteCommand('sysctl -a')


@vm_util.Retry(max_retries=IPERF_RETRIES)
def _RunIperf(sending_vm, receiving_vm, receiving_ip_address, ip_type):
  """Run iperf3 using sending 'vm' to connect to 'ip_address'.

  Args:
    sending_vm: The VM sending traffic.
    receiving_vm: The VM receiving traffic.
    receiving_ip_address: The IP address of the iperf3 server (ie the receiver).
    ip_type: The IP type of 'ip_address' (e.g. 'internal', 'external')
  Returns:
    A Sample.
  """
  iperf3_cmd = ('iperf3 --client %s --port %s --format m --time %s --parallel %s --udp --bandwidth %sM' %
               (receiving_ip_address, IPERF_PORT,
                FLAGS.iperf3_runtime_in_seconds,
                FLAGS.iperf3_sending_thread_count,
                FLAGS.iperf3_bandwidth_in_mbits))
  # the additional time on top of the iperf3 runtime is to account for the
  # time it takes for the iperf3 process to start and exit
  timeout_buffer = FLAGS.iperf3_timeout or 30 + FLAGS.iperf3_sending_thread_count
  stdout, _ = sending_vm.RemoteCommand(iperf3_cmd, should_log=True,
                                       timeout=FLAGS.iperf3_runtime_in_seconds +
                                       timeout_buffer)

  #Example for 1 thread:
  #- - - - - - - - - - - - - - - - - - - - - - - - -
  #[ ID] Interval           Transfer     Bandwidth       Jitter    Lost/Total Datagrams
  #[  4]   0.00-60.00  sec   714 MBytes  99.8 Mbits/sec  0.037 ms  57685/91386 (63%)  
  #[  4] Sent 91386 datagrams
  #
  #iperf Done.
  #
  #Example for >1 thread:
  #- - - - - - - - - - - - - - - - - - - - - - - - -
  #[ ID] Interval           Transfer     Bandwidth       Jitter    Lost/Total Datagrams
  #[  4]   0.00-60.00  sec   714 MBytes  99.8 Mbits/sec  0.039 ms  25457/91328 (28%)  
  #[  4] Sent 91328 datagrams
  #[  6]   0.00-60.00  sec   714 MBytes  99.8 Mbits/sec  0.052 ms  41304/91275 (45%)  
  #[  6] Sent 91275 datagrams
  #[SUM]   0.00-60.00  sec  1.39 GBytes   200 Mbits/sec  0.045 ms  66761/182603 (37%)  
  #
  #iperf Done.

  LASTLINE = 'iperf Done'
  last_line_index = 0
  result_line_index = 0

  lines = stdout.splitlines()
  for i,v in enumerate(lines):
    if v.find(LASTLINE) != -1:
      last_line_index = i

  if FLAGS.iperf3_sending_thread_count > 1:
    result_line_index = last_line_index - 2
  else:
    result_line_index = last_line_index - 3

  results = lines[result_line_index][6:].split()
  pprint(results)
  transfer = results[2]
  transfer_unit = results[3]
  bandwidth = results[4]
  bandwidth_unit = results[5]
  jitter = results[6]
  jitter_unit = results[7]
  datagrams_lost = results[8].split('/')[0]
  datagrams_total = results[8].split('/')[1]
  datagrams_prcnt = round(int(datagrams_lost) * 100 / int(datagrams_total))

  metadata = {
      # The meta data defining the environment
      'receiving_machine_type': receiving_vm.machine_type,
      'receiving_zone': receiving_vm.zone,
      'sending_machine_type': sending_vm.machine_type,
      'sending_thread_count': FLAGS.iperf3_sending_thread_count,
      'sending_zone': sending_vm.zone,
      'runtime_in_seconds': FLAGS.iperf3_runtime_in_seconds,
      'ip_type': ip_type
  }

  samples = []
  samples.append(sample.Sample('Transfer', float(transfer), transfer_unit, metadata))
  samples.append(sample.Sample('Bandwidth', float(bandwidth), bandwidth_unit, metadata))
  samples.append(sample.Sample('Jitter', float(jitter), jitter_unit, metadata))
  samples.append(sample.Sample('Datagram Lost', float(datagrams_lost), 'datagrams', metadata))
  samples.append(sample.Sample('Datagram Total', float(datagrams_total), 'datagrams', metadata))
  samples.append(sample.Sample('Datagram percentage', float(datagrams_prcnt), '%', metadata))

  return samples


def Run(benchmark_spec):
  """Run iperf3 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  results = []

  logging.info('Iperf Results:')

  # Send traffic in both directions
  for sending_vm, receiving_vm in vms, reversed(vms):
    # Send using external IP addresses
    if vm_util.ShouldRunOnExternalIpAddress():
      results += _RunIperf(sending_vm,
                               receiving_vm,
                               receiving_vm.ip_address,
                               'external')

    # Send using internal IP addresses
    if vm_util.ShouldRunOnInternalIpAddress(sending_vm,
                                            receiving_vm):
      results += _RunIperf(sending_vm,
                               receiving_vm,
                               receiving_vm.internal_ip,
                               'internal')

  return results


def Cleanup(benchmark_spec):
  """Cleanup iperf3 on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm in vms:
    vm.RemoteCommand('kill -9 ' + vm.iperf3_server_pid, ignore_failure=True)
