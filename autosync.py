# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The ``autosync`` middleware automates the creation and setup
of containers across clusters and allows control over the placement
of objects and containers between a set of swift clusters. It is based
on the swift container_sync. The middleware implements placement
decided by an external placement engine. The automation is implemented
during account/container PUT/POST/DELETE operations.

During account/container PUT, the account/container is created in all clusters
based on the placement decission. Once created, container_sync is used for
object synchronization. POST and DELTE are changed accordingly.

The first implementation supports two clustersbut the code was designed with
multiplicity of clusters in mind.

Clusters communicate with each other using standard Swift API with an
extra header indicating the identity of the peer cluster communicating.

+---------------------------------------------+-------------------------------+
|Header                                       | Use                           |
+=============================================+===============================+
| X-Orig-Cluster                              | Indicate the peer cluster URL |
+---------------------------------------------+-------------------------------+

placement decissions are made using a middleware hooked previously to the
autosync middleware. The middleware sets env parameters to
control the placement. Alternativly, config params are set as a defult
placement:
 
+------------------+-----------------------+----------------------------------+
| Env Param:       | Default config param: | Use:                             |
+==================+=======================+==================================+
| swift.my_cluster | autosync_my_cluster   | Indicate the current cluster URL |
+------------------+-----------------------+----------------------------------+
| swift.placement  | autosync_placement    | Indicates the list of all URLs   |
|                  |                       | of clusters for placement        |
+------------------+-----------------------+----------------------------------+

E.g.
[autosync]
autosync_my_cluster = http://Serengeti.Tanzania.biz:8081
autosync_placement = http://Annapurna.Nepal.info:8080, http://Serengeti.Tanzania.biz:8081

A placement control middleware may set the following parameters to override
the default: 

env['swift.placement'] = 'http://swift.Varanasi.India.com,
                          http://Serengeti.Tanzania.biz:8081,
                          http://www.ChiangMai.Tailand.me/swift:8080'

"""

from swift.common.http import is_success, HTTP_SERVICE_UNAVAILABLE
from swift.common.swob import Request, HTTPMovedPermanently, \
    HTTPServiceUnavailable, Response, HTTPInternalServerError
import sys
import os
import string
import random
from eventlet import Timeout, GreenPile
from eventlet.green.httplib import HTTPSConnection, HTTPConnection
from random import choice
from swift.common.utils import get_logger, config_true_value


class AutosyncMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.req_timeout = 2
        self.conn_timeout = 10
        self.keychars = string.ascii_letters + string.digits
        self.logger = get_logger(self.conf, log_route='nemo')
        self.override_auth = \
            config_true_value(conf.get('override_auth', False))
        self.default_my_cluster = conf.get('autosync_my_cluster', None)
        self.default_placement = conf.get('autosync_placement',
                                          [self.default_my_cluster])
        if self.default_placement:
            self.default_placement = self.default_placement.split(',')

    #def redirect(self):
    #    peer = choice(self.placement)
    #    resp = HTTPMovedPermanently(location=(peer + self.env['PATH_INFO']))
    #    return resp(self.env, self.start_response)

    def send_to_peer(self, peer, sync_to_peer, key):
        peer = peer.lower()
        ssl = False
        if peer.startswith('https://'):
            ssl = True
            peer = peer[8:]
        if peer.startswith('http://'):
            peer = peer[7:]
        try:
            with Timeout(self.conn_timeout):
                if ssl:
                    #print 'HTTPS %s ' % peer
                    conn = HTTPSConnection(peer)
                else:
                    #print 'HTTP %s ' % peer
                    conn = HTTPConnection(peer)
            conn.putrequest(self.req.method, self.req.path_qs)
            conn.putheader('X-Orig-Cluster', self.my_cluster)
            conn.putheader('X-Account-Meta-Orig-Cluster', self.my_cluster)
            conn.putheader('X-Container-Meta-Orig-Cluster', self.my_cluster)
            if key:
                sync_to = sync_to_peer + self.env['PATH_INFO']
                conn.putheader('X-Container-Sync-To', sync_to)
            for header, value in self.req.headers.iteritems():
                if header != 'X-Container-Sync-To':
                    conn.putheader(header, value)
            conn.endheaders(message_body=None)
            with Timeout(self.req_timeout):
                resp = conn.getresponse()
                status = resp.status
                return (status, resp.getheaders(), resp.read())
        except (Exception, Timeout) as e:
            # Print error log
            print >> sys.stderr, peer + ': Exception, Timeout error: %s' % e

        print '<<<<<<<< HTTP_SERVICE_UNAVAILABLE'
        #return HTTP_SERVICE_UNAVAILABLE, None, None

    def send_to_peers(self, peers, key):
        pile = GreenPile(len(peers))
        # Have the first peer to sync to the local cluster
        sync_to_peer = self.my_cluster
        for peer in peers:
            # create thread per peer and send a request
            pile.spawn(self.send_to_peer, peer, sync_to_peer, key)
            # Have the next peer to sync to the present peer
            sync_to_peer = peer
        # collect the results, if anyone failed....
        response = [resp for resp in pile if resp]
        while len(response) < len(peers):
            response.append((HTTP_SERVICE_UNAVAILABLE, None, None))
        return response

    def highest_response(self, resps, swap={}):
        highest_resp = None
        highest_status = -1
        for resp in resps:
            status = resp[0]
            if status in swap:
                status = swap[status]
            status = int(status)
            if status > highest_status:
                highest_status = status
                highest_resp = resp
        if highest_resp:
            return Response(body=highest_resp[2], status=highest_resp[0],
                            headers=highest_resp[1])
        return HTTPServiceUnavailable(request=self.req)

    def all_success(self, resps):
        for resp in resps:
            if not is_success(resp[0]):
                return False
        return True

    def __call__(self, env, start_response):
        def my_start_response(status, headers, exc_info=None):
            self.status = status
            self.headers = list(headers)
            self.exc_info = exc_info
        self.env = env
        self.start_response = start_response

        # If request was already processed by autosync
        # (here or at the original cluster where it first hit)
        if 'HTTP_X_ORIG_CLUSTER' in env:
            print >> sys.stderr, 'HTTP_X_ORIG_CLUSTER found!'
            if self.override_auth:
                env['swift_owner'] = True
            return self.app(env, start_response)

        # If it is a local call or a tempurl object call
        if 'swift.authorize_override' in env:
            return self.app(env, start_response)

        # Get Placement parameters
        if 'swift.my_cluster' in env:
            self.my_cluster = env['swift.my_cluster']
        else:
            self.my_cluster = self.default_my_cluster

        if 'swift.placement' in env:
            placement = env['swift.placement']
        else:
            placement = self.default_placement or self.my_cluster
        
        if not self.my_cluster or not placement:
            return self.app(env, start_response)

        self.req = Request(env)
        # For now we support only placement here and in one other place
        if self.my_cluster not in placement:
            return HTTPInternalServerError(request=self.req)
        #   return self.redirect()

        peers = [p for p in placement if p != self.my_cluster]
        if len(peers) != 1:
            return HTTPInternalServerError(request=self.req) 

        # This request needs to be handled localy
        try:
            (version, account, container, obj) = \
                self.req.split_path(2, 4, True)
        except ValueError:
            return self.app(env, start_response)
        if obj or self.req.method in ('OPTIONS', 'GET', 'HEAD'):
            # business as usual - I will serve the request locally and be done
            # TBD, in case of 404 returned from GET object, try a remote copy?
            return self.app(env, start_response)

        # Lets see, its either PUT, POST or DELETE account/container
        # Otherwise said - 'we need to change the account/container'
        # both here and with peers...

        # As part of any container creation/modification (PUT/POST):
        # Create a new key  to protect the container communication from now
        # and until the next time the container is updated.
        # Note that race may occur with container-sync daemons resulting in
        # container-sync failing due to misaligned keys.
        # Changing the keys per update help support changes in the placement
        # and can serve as a simple mechanism for replacing conatienr sync keys
        # If this turns out to be an issue, we may extract and reuse the same
        # key for the duration of the container existance. 
        if container and self.req.method in ['POST', 'PUT']:
            key = ''.join(choice(self.keychars) for x in range(64))
            # Add the key to the env when calling the local cluster 
            env['HTTP_X_CONTAINER_SYNC_KEY'] = key
            # Set the container replica of the local cluster to sync to the
            # last cluster in the list of peers
            sync_to_peer = peers[-1]  # Sync to the prev peer
            sync_to = sync_to_peer + self.env['PATH_INFO']
            env['HTTP_X_CONTAINER_SYNC_TO'] = sync_to
        else:
            key = None  # Signals that there are no Container-Sync headers

        # Try localy, if we fail and not DELETE respond with a faliure.
        resp_data = self.app(self.env, my_start_response)
        data = ''.join(iter(resp_data))
        if hasattr(resp_data, 'close'):
            resp_data.close()
        resp_status_int = int(self.status[:3])


        # Faliure at local cluster during anything but DELETE... abandon ship
        if not is_success(resp_status_int) and self.req.method != 'DELETE':
            # Dont even try the peers
            start_response(self.status, self.headers, self.exc_info)
            return data

        # Call peers and accomulate responses
        try:
            # Note that key is None if not during container PUT/POST
            resps = self.send_to_peers(peers, key)
            # Append the local cluster response
            resps.append((resp_status_int, self.headers, data))
        except:
            return HTTPServiceUnavailable(request=self.req)

        resp = None
        if self.req.method == 'DELETE':
            # Special treatment to DELETE - respond with the best we have
            resp = self.highest_response(resps, swap={'404': '1'})
        else:  # PUT/POST - respond only if all success
            if self.all_success(resps):
                resp = self.highest_response(resps)
            else:
                # PUT/POST with local success and remote faliure
                resp = HTTPServiceUnavailable(request=self.req)
        return resp(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def autosync_filter(app):
        return AutosyncMiddleware(app, conf)
    return autosync_filter
