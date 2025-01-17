# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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


from abc import abstractmethod
import re
import uuid

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

# TODO(ferneyhough): change to enum
flags.DEFINE_string('managed_db_engine', None,
                    'Managed database flavor to use (mysql, postgres)')
flags.DEFINE_string('managed_db_engine_version', None,
                    'Version of the database flavor selected, e.g. 5.7')
flags.DEFINE_string('managed_db_database_name', None,
                    'Name of the database to create. Defaults to '
                    'pkb-db-[run-uri]')
flags.DEFINE_string('managed_db_database_username', None,
                    'Database username. Defaults to '
                    'pkb-db-user-[run-uri]')
flags.DEFINE_string('managed_db_database_password', None,
                    'Database password. Defaults to '
                    'a random 10-character alpha-numeric string')
flags.DEFINE_boolean('managed_db_high_availability', False,
                     'Specifies if the database should be high availability')
flags.DEFINE_boolean('managed_db_backup_enabled', True,
                     'Whether or not to enable automated backups')
flags.DEFINE_string('managed_db_backup_start_time', '07:00',
                    'Time in UTC that automated backups (if enabled) '
                    'will be scheduled. In the form HH:MM UTC. '
                    'Defaults to 07:00 UTC')
flags.DEFINE_list('managed_db_zone', None,
                  'zone or region to launch the database in. '
                  'Defaults to the client vm\'s zone.')
flags.DEFINE_string('managed_db_machine_type', None,
                    'Machine type of the database.')
flags.DEFINE_integer('managed_db_cpus', None,
                     'Number of Cpus in the database.')
flags.DEFINE_string('managed_db_memory', None,
                    'Amount of Memory in the database.  Uses the same format '
                    'string as custom machine memory type.')
flags.DEFINE_integer('managed_db_disk_size', None,
                     'Size of the database disk in GB.')
flags.DEFINE_string('managed_db_disk_type', None,
                    'Machine type of the database.')
flags.DEFINE_boolean(
    'use_managed_db', True, 'If true, uses the managed MySql '
    'service for the requested cloud provider. If false, uses '
    'MySql installed on a VM.')
flags.DEFINE_list(
    'mysql_flags', '', 'Flags to apply to the implementation of '
    'MySQL on the cloud that\'s being used. Example: '
    'binlog_cache_size=4096,innodb_log_buffer_size=4294967295')


BACKUP_TIME_REGULAR_EXPRESSION = '^\d\d\:\d\d$'
flags.register_validator(
    'managed_db_backup_start_time',
    lambda value: re.search(BACKUP_TIME_REGULAR_EXPRESSION, value) is not None,
    message=('--database_backup_start_time must be in the form HH:MM'))

MYSQL = 'mysql'
POSTGRES = 'postgres'
AURORA_POSTGRES = 'aurora-postgresql'
AURORA_MYSQL = 'aurora-mysql'
AURORA_MYSQL56 = 'aurora'

FLAGS = flags.FLAGS

# TODO: Implement DEFAULT BACKUP_START_TIME for instances.


class RelationalDbPropertyNotSet(Exception):
  pass


class RelationalDbEngineNotFoundException(Exception):
  pass


class UnsupportedError(Exception):
  pass


def GenerateRandomDbPassword():
  """Generate a random password 10 characters in length."""
  return str(uuid.uuid4())[:10]


def GetRelationalDbClass(cloud):
  """Get the RelationalDb class corresponding to 'cloud'.

  Args:
    cloud: name of cloud to get the class for

  Returns:
    BaseRelationalDb class with the cloud attribute of 'cloud'.
  """
  return resource.GetResourceClass(BaseRelationalDb, CLOUD=cloud)


class BaseRelationalDb(resource.BaseResource):
  """Object representing a relational database Service."""

  RESOURCE_TYPE = 'BaseRelationalDb'

  def __init__(self, relational_db_spec):
    """Initialize the managed relational database object.

    Args:
      relational_db_spec: spec of the managed database.

    Raises:
      UnsupportedError: if high availability is requested for an unmanaged db.
    """
    super(BaseRelationalDb, self).__init__()
    self.spec = relational_db_spec
    if not FLAGS.use_managed_db:
      if self.spec.high_availability:
        raise UnsupportedError('High availability is unsupported for unmanaged '
                               'databases.')
      self.endpoint = ''
      self.spec.database_username = 'root'
      self.spec.database_password = 'perfkitbenchmarker'
      self.is_managed_db = False
    else:
      self.is_managed_db = True

  @property
  def client_vm(self):
    """Client VM which will drive the database test.

    This is required by subclasses to perform client-vm
    network-specific tasks, such as getting information about
    the VPC, IP address, etc.

    Raises:
      RelationalDbPropertyNotSet: if the client_vm is missing.

    Returns:
      The client_vm.
    """
    if not hasattr(self, '_client_vm'):
      raise RelationalDbPropertyNotSet('client_vm is not set')
    return self._client_vm

  @client_vm.setter
  def client_vm(self, client_vm):
    self._client_vm = client_vm

  def MakePsqlConnectionString(self, database_name):
    return '\'host={0} user={1} password={2} dbname={3}\''.format(
        self.endpoint,
        self.spec.database_username,
        self.spec.database_password,
        database_name)

  def MakeMysqlConnectionString(self, use_localhost=False):
    return '-h {0}{1} -u {2} -p{3}'.format(
        self.endpoint if not use_localhost else 'localhost',
        ' -P 3306' if not self.is_managed_db else '',
        self.spec.database_username, self.spec.database_password)

  def MakeSysbenchConnectionString(self):
    return (
        '--mysql-host={0}{1} --mysql-user={2} --mysql-password="{3}" ').format(
            self.endpoint,
            ' --mysql-port=3306' if not self.is_managed_db else '',
            self.spec.database_username, self.spec.database_password)

  @property
  def endpoint(self):
    """Endpoint of the database server (exclusing port)."""
    if not hasattr(self, '_endpoint'):
      raise RelationalDbPropertyNotSet('endpoint not set')
    return self._endpoint

  @endpoint.setter
  def endpoint(self, endpoint):
    self._endpoint = endpoint

  @property
  def port(self):
    """Port (int) on which the database server is listening."""
    if not hasattr(self, '_port'):
      raise RelationalDbPropertyNotSet('port not set')
    return self._port

  @port.setter
  def port(self, port):
    self._port = int(port)

  def GetResourceMetadata(self):
    """Returns a dictionary of metadata.

    Child classes can extend this if needed.

    Raises:
       RelationalDbPropertyNotSet: if any expected metadata is missing.
    """
    metadata = {
        'zone': self.spec.vm_spec.zone,
        'disk_type': self.spec.disk_spec.disk_type,
        'disk_size': self.spec.disk_spec.disk_size,
        'engine': self.spec.engine,
        'high_availability': self.spec.high_availability,
        'backup_enabled': self.spec.backup_enabled,
        'backup_start_time': self.spec.backup_start_time,
        'engine_version': self.spec.engine_version,
    }
    if (hasattr(self.spec.vm_spec, 'machine_type') and
        self.spec.vm_spec.machine_type):
      metadata.update({
          'machine_type': self.spec.vm_spec.machine_type,
      })
    elif hasattr(self.spec.vm_spec, 'cpus') and (
        hasattr(self.spec.vm_spec, 'memory')):
      metadata.update({
          'cpus': self.spec.vm_spec.cpus,
      })
      metadata.update({
          'memory': self.spec.vm_spec.memory,

      })
    elif hasattr(self.spec.vm_spec, 'tier') and (
        hasattr(self.spec.vm_spec, 'compute_units')):
      metadata.update({
          'tier': self.spec.vm_spec.tier,
      })
      metadata.update({
          'compute_units': self.spec.vm_spec.compute_units,
      })
    else:
      raise RelationalDbPropertyNotSet(
          'Machine type of the database must be set.')

    if FLAGS.mysql_flags:
      metadata.update({
          'mysql_flags': FLAGS.mysql_flags,
      })

    return metadata

  @abstractmethod
  def GetDefaultEngineVersion(self, engine):
    """Return the default version (for PKB) for the given database engine.

    Args:
      engine: name of the database engine

    Returns: default version as a string for the given engine.
    """

  def _IsReadyUnmanaged(self):
    """Return true if the underlying resource is ready.

    Returns:
      True if MySQL was installed successfully, False if not.

    Raises:
      Exception: If this method is called when the database is a managed one.
        Shouldn't happen.
    """
    if self.is_managed_db:
      raise Exception('Checking state of unmanaged database when the database '
                      'is managed.')
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      mysql_name = 'mysql56'
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7.')):
      mysql_name = 'mysql57'
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                      'are supported.' % FLAGS.managed_db_engine_version)
    stdout, stderr = self.server_vm.RemoteCommand(
        'sudo service %s status' % self.server_vm.GetServiceName(mysql_name))
    return stdout and not stderr

  def _InstallMySQLClient(self):
    """Installs MySQL Client on the client vm.

    Raises:
      Exception: If the requested engine version is unsupported.
    """
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      self.client_vm.Install('mysqlclient56')
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7.')):
      self.client_vm.Install('mysqlclient')
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                      'are supported.' % FLAGS.managed_db_engine_version)

  def _InstallMySQLServer(self):
    """Installs MySQL Server on the server vm.

    Raises:
      Exception: If the requested engine version is unsupported, or if this
        method is called when the database is a managed one. The latter
        shouldn't happen.
    """
    if self.is_managed_db:
      raise Exception('Can\'t install MySQL Server when using a managed '
                      'database.')
    if (self.spec.engine_version == '5.6' or
        self.spec.engine_version.startswith('5.6.')):
      mysql_name = 'mysql56'
      self.server_vm.Install(mysql_name)
    elif (self.spec.engine_version == '5.7' or
          self.spec.engine_version.startswith('5.7.')):
      mysql_name = 'mysql57'
      self.server_vm.Install(mysql_name)
    else:
      raise Exception('Invalid database engine version: %s. Only 5.6 and 5.7 '
                      'are supported.' % FLAGS.managed_db_engine_version)
    self.server_vm.RemoteCommand('chmod 777 %s' %
                                 self.server_vm.GetScratchDir())
    self.server_vm.RemoteCommand('sudo service %s stop' %
                                 self.server_vm.GetServiceName(mysql_name))
    self.server_vm.RemoteCommand(
        'sudo sed -i '
        '"s/datadir=\\/var\\/lib\\/mysql/datadir=\\%s\\/mysql/" '
        '%s' % (self.server_vm.GetScratchDir(),
                self.server_vm.GetPathToConfig(mysql_name)))
    self.server_vm.RemoteCommand('sudo cp -R -p /var/lib/mysql %s/' %
                                 self.server_vm.GetScratchDir())
    # Comments out the bind-address line in mysqld.cnf.
    self.server_vm.RemoteCommand(
        'sudo sed -i \'s/bind-address/#bind-address/g\' '
        '/etc/mysql/mysql.conf.d/mysqld.cnf')
    self.server_vm.RemoteCommand('sudo service %s restart' %
                                 self.server_vm.GetServiceName(mysql_name))
    self.server_vm.RemoteCommand(
        ('mysql %s -e "CREATE USER \'%s\'@\'%s\' IDENTIFIED BY \'%s\';"') %
        (self.MakeMysqlConnectionString(use_localhost=True),
         self.spec.database_username, self.client_vm.ip_address,
         self.spec.database_password))
    self.server_vm.RemoteCommand(
        ('mysql %s -e "GRANT ALL PRIVILEGES ON *.* TO \'%s\'@\'%s\';"') %
        (self.MakeMysqlConnectionString(use_localhost=True),
         self.spec.database_username, self.client_vm.ip_address))
    self.server_vm.RemoteCommand(
        'mysql %s -e "FLUSH PRIVILEGES;"' %
        self.MakeMysqlConnectionString(use_localhost=True))

  def _ApplyMySqlFlags(self):
    if FLAGS.mysql_flags:
      for flag in FLAGS.mysql_flags:
        cmd = 'mysql %s -e \'SET %s;\'' % self.MakeMysqlConnectionString(), flag
        _, stderr, _ = vm_util.IssueCommand(cmd)
        if stderr:
          raise Exception('Invalid MySQL flags: %s' % stderr)

  def Failover(self):
    """Fail over the database.  Throws exception if not high available."""
    if not self.spec.high_availability:
      raise Exception('Attempt to fail over a database that isn\'t marked '
                      'as high available')
    self._FailoverHA()

  @abstractmethod
  def _FailoverHA(self):
    """Fail over from master to replica."""
    pass
