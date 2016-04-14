from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv
import subprocess


@when('namenode.joined')
@when_not('resourcemanager.installed')
def install_hadoop(namenode):
    '''Install only if the resourcemanager has sent its FQDN.'''
    if namenode.namenodes():
        hookenv.status_set('maintenance', 'installing resourcemanager')
        nn_host = namenode.namenodes()[0]
        rm_host = subprocess.check_output(['hostname', '-f']).strip().decode()
        bigtop = get_bigtop_base()
        bigtop.install(NN=nn_host, RM=rm_host)
        set_state('resourcemanager.installed')
        hookenv.status_set('maintenance', 'resourcemanager installed')
    else:
        hookenv.status_set('waiting', 'waiting for namenode to become ready')


@when('resourcemanager.installed')
@when_not('resourcemanager.started')
def start_resourcemanager():
    hookenv.status_set('maintenance', 'starting resourcemanager')
    for port in get_layer_opts().exposed_ports('resourcemanager'):
        hookenv.open_port(port)
    set_state('resourcemanager.started')
    hookenv.status_set('active', 'ready')


@when('resourcemanager.started', 'nodemanager.joined')
def send_info(nodemanager):
    '''Send nodemanagers our FQDN so they can install as slaves.'''
    hostname = subprocess.check_output(['hostname', '-f']).strip().decode()
    nodemanager.send_resourcemanagers([hostname])

    slaves = [node['host'] for node in nodemanager.nodes()]
    hookenv.status_set('active', 'ready ({count} nodemanager{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))
