Swift Autosync
--------------

* WARNING: THIS IS WORK IN PROGRESS AND IS PUBLISHED TO ALLOW *
* ADDITIONAL COMMUNITY DEVELOPMENT AND TESTING                *

An automation layer to container sync ensuring that containers are created on
both a local and remote clusters.
Enables adding a placement engine.
Maintains a unified namespace between datacenters

Current version is applicable to two sites (Primary serving clients
and Secondary used as a backup). 
Code was designed with multiple sites in mind.
Code was designed for multiple active sites in mind
The auth work needed to allow autosync requests to go through is out of scope.
Future work will require adding a daemon for eventuqal consistency.
Present work returns error if datacenters are inconsistent leaving 
cleanups to the client. 

How to Install
--------------
The example and current code allows working with a remote back cluster.
It is assumed that authentication and authorization issues allowing one
cluster to address teh other are solved outside of teh scope of the autosync,
though the autosync middleware offers an initial auth override mechanism
to allow testing and initial work with a remot backup cluster.

Assuming two clusters:
* Primary.com - serving clients via port 8080
* Secondary.com - serving as a backup to Primary via 8080, not serving clients

1. Add the autosync.py to your swift/common/middleware/
2. Edit setup.py to install autosync.py as a filter named autosync
3. Edit proxy-server.conf in Primary and add:

    [DEFAULT]

    allowed_sync_hosts = Secondary.com

    [pipeline:main]

    \# Add autosync after the request is authenticated by the auth system used

    pipeline = ...autosync... 

    [filter:autosync]

    use = egg:swift#autosync

    autosync_my_cluster = http://Primary.com:8080

    autosync_placement = http://Primary.com:8080,http://Secondary.com:8080

3. Edit container-server.conf of Primary and add:

    [DEFAULT]

    allowed_sync_hosts = Secondary.com

4. Edit proxy-server.conf in Secondary and add:

    [DEFAULT]

    allowed_sync_hosts = Primary.com

    [pipeline:main]

    \# Remove the auth system used or change it to allow autosync requests to go through

    \# Add autosync to the pipeline

    pipeline = ...autosync... 

    [filter:autosync]

    use = egg:swift#autosync

    autosync_my_cluster = http://Secondary.com:8080

    autosync_placement = http://Primary.com:8080,http://Secondary.com:8080

5. Edit container-server.conf of Secondary and add:

    [DEFAULT]

    allowed_sync_hosts = Primary.com

