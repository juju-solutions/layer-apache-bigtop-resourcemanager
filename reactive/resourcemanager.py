from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv, host
from jujubigdata import utils
import subprocess


@when_not('namenode.joined')
def blocked():
    hookenv.status_set('blocked', 'waiting for namenode relation')


@when('namenode.joined', 'puppet.available')
@when_not('resourcemanager.installed')
def install_hadoop(namenode):
    '''Install only if the resourcemanager has sent its FQDN.'''
    if namenode.namenodes():
        hookenv.status_set('maintenance', 'installing resourcemanager')
        nn_host = namenode.namenodes()[0]
        # rm_host = utils.resolve_private_address(hookenv.unit_private_ip())
        rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
        bigtop = get_bigtop_base()
        hosts = {'namenode': nn_host, 'resourcemanager': rm_host}
        bigtop.install(hosts=hosts, roles='resourcemanager')
        set_state('resourcemanager.installed')
        hookenv.status_set('maintenance', 'resourcemanager installed')
    else:
        hookenv.status_set('waiting', 'waiting for namenode to become ready')


@when('namenode.joined')
@when('resourcemanager.installed')
@when_not('resourcemanager.started')
def start_resourcemanager(namenode):
    hookenv.status_set('maintenance', 'starting resourcemanager')
    # NB: services should be started by install, but this may be handy in case
    # we have something that removes the .started state in the future.
    host.service_start('hadoop-yarn-resourcemanager')
    host.service_start('hadoop-mapreduce-historyserver')
    for port in get_layer_opts().exposed_ports('resourcemanager'):
        hookenv.open_port(port)
    set_state('resourcemanager.started')
    hookenv.status_set('active', 'ready')


@when('namenode.joined')
@when('resourcemanager.started')
@when('nodemanager.joined')
def send_info(nodemanager, namenode):
    '''Send nodemanagers our master FQDNs so they can install as slaves.'''
    nn_host = namenode.namenodes()[0]
    # rm_host = utils.resolve_private_address(hookenv.unit_private_ip())
    rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    # TODO: fix below. nodemgrs need both nn and rm to install, but clients
    # only need the rm. it's confusing to use 'send_resourcemanagers' for both.
    nodemanager.send_resourcemanagers([nn_host, rm_host])

    # update status with slave count and report ready for hdfs
    slaves = [node['host'] for node in nodemanager.nodes()]
    hookenv.status_set('active', 'ready ({count} nodemanager{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))
    set_state('resourcemanager.ready')


@when('resourcemanager.clients')
def accept_clients(clients):
    rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    clients.send_resourcemanagers([rm_host])
    port = get_layer_opts().port('resourcemanager')
    hs_http = get_layer_opts().port('jh_webapp_http')
    hs_ipc = get_layer_opts().port('jobhistory')
    clients.send_ports(port, hs_http, hs_ipc)
    clients.send_ready(True)
