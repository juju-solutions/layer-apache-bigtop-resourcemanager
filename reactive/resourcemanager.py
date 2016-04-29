from charms.reactive import is_state, remove_state, set_state, when, when_not
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv, host
from jujubigdata import utils
import subprocess


###############################################################################
# Utility methods
###############################################################################
def send_early_install_info(namenode, remote):
    """Send clients/slaves enough relation data to start their install.

    If slaves or clients join before the resourcemanager is installed (and even
    before the namenode is ready), we can still provide enough info to start
    their installation. This will help parallelize installation among our
    cluster.

    Bigtop puppet scripts require a namenode fqdn, so don't send anything
    to slaves or clients unless we have that. Also note that slaves can safely
    install early, but should not start until the 'resourcemanager.ready' state
    is set by the mapred-slave interface.
    """
    if namenode.namenodes()[0]:
        nn_host = namenode.namenodes()[0]
        rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
        rm_ipc = get_layer_opts().port('resourcemanager')
        jh_ipc = get_layer_opts().port('jobhistory')
        jh_http = get_layer_opts().port('jh_webapp_http')

        # TODO: fix below. we're sending both NN and RM. it's confusing to call
        # that 'send_resourcemanagers'.
        remote.send_resourcemanagers([nn_host, rm_host])
        remote.send_ports(rm_ipc, jh_http, jh_ipc)


###############################################################################
# Core methods
###############################################################################
@when_not('namenode.joined')
def blocked():
    hookenv.status_set('blocked', 'missing required namenode relation')


@when('puppet.available', 'namenode.joined')
@when_not('apache-bigtop-resourcemanager.installed')
def install_resourcemanager(namenode):
    """Install if the namenode has sent its FQDN.

    We only need the namenode FQDN to perform the RM install, so poll for
    namenodes() data whenever we have a namenode relation. This allows us to
    install asap, even if 'namenode.ready' is not set yet.
    """
    if namenode.namenodes()[0]:
        hookenv.status_set('maintenance', 'installing resourcemanager')
        nn_host = namenode.namenodes()[0]
        rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
        bigtop = get_bigtop_base()
        hosts = {'namenode': nn_host, 'resourcemanager': rm_host}
        bigtop.install(hosts=hosts, roles='resourcemanager')

        # /etc/hosts entries from the KV are not currently used for bigtop,
        # but a hosts_map attribute is required by some interfaces (eg: mapred-slave)
        # to signify RM's readiness. Set our RM info in the KV to fulfill this
        # requirement.
        utils.initialize_kv_host()

        set_state('apache-bigtop-resourcemanager.installed')
        hookenv.status_set('maintenance', 'resourcemanager installed')
    else:
        hookenv.status_set('waiting', 'waiting for namenode fqdn')


@when('apache-bigtop-resourcemanager.installed', 'namenode.joined')
@when_not('namenode.ready')
def send_nn_spec(namenode):
    """Send our resourcemanager spec so the namenode can become ready."""
    bigtop = get_bigtop_base()
    # Send RM spec (must match NN spec for 'namenode.ready' to be set)
    namenode.set_local_spec(bigtop.spec())
    hookenv.status_set('waiting', 'waiting for namenode to become ready')


@when('apache-bigtop-resourcemanager.installed', 'namenode.ready')
@when_not('apache-bigtop-resourcemanager.started')
def start_resourcemanager(namenode):
    hookenv.status_set('maintenance', 'starting resourcemanager')
    # NB: service should be started by install, but this may be handy in case
    # we have something that removes the .started state in the future. Also
    # note we restart here in case we modify conf between install and now.
    host.service_restart('hadoop-yarn-resourcemanager')
    host.service_restart('hadoop-mapreduce-historyserver')
    for port in get_layer_opts().exposed_ports('resourcemanager'):
        hookenv.open_port(port)
    set_state('apache-bigtop-resourcemanager.started')
    hookenv.status_set('active', 'ready')


###############################################################################
# Slave methods
###############################################################################
@when('namenode.joined', 'nodemanager.joined')
@when_not('apache-bigtop-resourcemanager.installed')
def send_nm_install_info(namenode, nodemanager):
    """Send nodemanagers enough relation data to start their install."""
    send_early_install_info(namenode, nodemanager)


@when('apache-bigtop-resourcemanager.started')
@when('namenode.ready', 'nodemanager.joined')
def send_nm_all_info(namenode, nodemanager):
    """Send nodemanagers all mapred-slave relation data.

    At this point, the resourcemanager is ready to serve nodemanagers. Send all
    mapred-slave relation data so that our 'resourcemanager.ready' state becomes set.
    """
    bigtop = get_bigtop_base()
    nn_host = namenode.namenodes()[0]
    rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    rm_ipc = get_layer_opts().port('resourcemanager')
    jh_ipc = get_layer_opts().port('jobhistory')
    jh_http = get_layer_opts().port('jh_webapp_http')

    # TODO: fix below. we're sending both NN and RM. it's confusing to call
    # that 'send_resourcemanagers'.
    nodemanager.send_resourcemanagers([nn_host, rm_host])
    nodemanager.send_spec(bigtop.spec())
    nodemanager.send_ports(rm_ipc, jh_http, jh_ipc)

    # hosts_map and ssh_key are required by the mapred-slave interface to signify
    # RM's readiness. Send them, even though they are not utilized by bigtop.
    # NB: update KV hosts with all nodemanagers prior to sending the hosts_map
    # because mapred-slave gates readiness on a NM's presence in the hosts_map.
    utils.update_kv_hosts(nodemanager.hosts_map())
    nodemanager.send_hosts_map(utils.get_kv_hosts())
    nodemanager.send_ssh_key('invalid')

    # update status with slave count and report ready for hdfs
    num_slaves = len(nodemanager.nodes())
    hookenv.status_set('active', 'ready ({count} nodemanager{s})'.format(
        count=num_slaves,
        s='s' if num_slaves > 1 else '',
    ))
    set_state('apache-bigtop-resourcemanager.ready')


@when('apache-bigtop-resourcemanager.started')
@when('namenode.ready', 'nodemanager.departing')
def remove_nm(namenode, nodemanager):
    """Handle a departing nodemanager.

    This simply logs a message about a departing nodemanager and removes
    the entry from our KV hosts_map. The hosts_map is not used by bigtop, but
    it is required for the 'resourcemanager.ready' state, so we may as well
    keep it accurate.
    """
    slaves_leaving = nodemanager.nodes()  # only returns nodes in "departing" state
    hookenv.log('Nodemanagers leaving: {}'.format(slaves_leaving))
    utils.remove_kv_hosts(slaves_leaving)
    nodemanager.dismiss()


@when('apache-bigtop-resourcemanager.started', 'namenode.ready')
@when_not('nodemanager.joined')
def wait_for_nm(namenode):
    remove_state('apache-bigtop-resourcemanager.ready')
    # NB: we're still active since a user may be interested in our web UI
    # without any NMs, but let them know yarn is caput without a NM relation.
    hookenv.status_set('active', 'yarn requires a nodemanager relation')


###############################################################################
# Client methods
###############################################################################
@when('namenode.joined', 'resourcemanager.clients')
@when_not('apache-bigtop-resourcemanager.installed')
def send_client_install_info(namenode, client):
    """Send clients enough relation data to start their install."""
    send_early_install_info(namenode, client)


@when('apache-bigtop-resourcemanager.started')
@when('namenode.ready', 'resourcemanager.clients')
def send_client_all_info(namenode, client):
    """Send clients (plugin, RM, non-DNs) all dfs relation data.

    At this point, the resourcemanager is ready to serve clients. Send all
    mapred relation data so that our 'resourcemanager.ready' state becomes set.
    """
    bigtop = get_bigtop_base()
    nn_host = namenode.namenodes()[0]
    rm_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    rm_ipc = get_layer_opts().port('resourcemanager')
    jh_ipc = get_layer_opts().port('jobhistory')
    jh_http = get_layer_opts().port('jh_webapp_http')

    # TODO: fix below. we're sending both NN and RM. it's confusing to call
    # that 'send_resourcemanagers'.
    client.send_resourcemanagers([nn_host, rm_host])
    client.send_spec(bigtop.spec())
    client.send_ports(rm_ipc, jh_http, jh_ipc)

    # resourcemanager.ready implies we have at least 1 nodemanager, which means
    # yarn is ready for use. Inform clients of that with send_ready().
    if is_state('apache-bigtop-resourcemanager.ready'):
        client.send_ready(True)
    else:
        client.send_ready(False)

    # hosts_map is required by the mapred interface to signify
    # RM's readiness. Send it, even though it is not utilized by bigtop.
    client.send_hosts_map(utils.get_kv_hosts())


###############################################################################
# Benchmark methods
###############################################################################
@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('mrbench', 'nnbench', 'terasort', 'testdfsio')
