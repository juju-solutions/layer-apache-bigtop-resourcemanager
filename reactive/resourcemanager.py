from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv
import subprocess


@when_not('namenode.joined')
def blocked():
    hookenv.status_set('blocked', 'waiting for namenode relation')


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


@when('namenode.joined')
@when('resourcemanager.installed')
@when_not('resourcemanager.started')
def start_resourcemanager():
    hookenv.status_set('maintenance', 'starting resourcemanager')
    for port in get_layer_opts().exposed_ports('resourcemanager'):
        hookenv.open_port(port)
    set_state('resourcemanager.started')
    hookenv.status_set('active', 'ready')


@when('namenode.joined')
@when('resourcemanager.started')
@when('nodemanager.joined')
def send_info(namenode, nodemanager):
    '''Send nodemanagers our master FQDNs so they can install as slaves.'''
    nn_fqdn = namenode.namenodes()[0]
    rm_fqdn = subprocess.check_output(['hostname', '-f']).strip().decode()
    nodemanager.send_resourcemanagers([nn_fqdn, rm_fqdn])
