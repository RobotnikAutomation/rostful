from __future__ import absolute_import
import sys
import json
import argparse
import urllib2
import time
from .jwt_interface import JwtInterface
from collections import namedtuple
from .util import ROS_MSG_MIMETYPE, ROS_MSG_MIMETYPE_WITH_TYPE, get_json_bool
from . import deffile
from . import message_conversion as msgconv
from importlib import import_module
from cStringIO import StringIO
import threading
import actionlib_msgs.msg
import rospy

import roslib
roslib.load_manifest('rostful')


_timeout = 5


class IndividualServiceProxy:
    def __init__(self, url, name, srv_module, service_type_name, binary=None, use_jwt=False, jwt_key='ros'):
        self._use_jwt = use_jwt

        self.jwt_iface = JwtInterface(key=jwt_key, algorithm='HS256')

        self.url = url
        self.name = name

        self.binary = binary or False

        self.rostype = getattr(srv_module, service_type_name)
        self.rostype_req = getattr(srv_module, service_type_name + 'Request')
        self.rostype_resp = getattr(srv_module, service_type_name + 'Response')

        self.proxy = rospy.Service(self.name, self.rostype, self.call)
        rospy.logwarn(
            'IndividualServiceProxy::__init__: listening to %s and sending to remote %s', name, self.url)

    def call(self, rosreq):
        if self.binary:
            req = StringIO()
            rosreq.serialize(req)
            reqs = req.getvalue()
            content_type = ROS_MSG_MIMETYPE_WITH_TYPE(self.rostype_req)
        elif self._use_jwt:
            req = msgconv.extract_values(rosreq)
            req['_format'] = 'ros'
            reqs = self.jwt_iface.encode(req)
            content_type = 'application/jwt'

        else:
            req = msgconv.extract_values(rosreq)
            req['_format'] = 'ros'
            reqs = json.dumps(req)
            content_type = 'application/json'

        wsreq = urllib2.Request(self.url.encode(
            'utf-8'), data=reqs, headers={'Content-Type': content_type})
        rosresp = self.rostype_resp()

        try:
            wsres = urllib2.urlopen(wsreq, timeout=_timeout)
        except Exception, e:
            rospy.logerr('IndividualServiceProxy::callback: %s.  Exception: %s. Content type: %s, reqs: %s',
                         self.name, e, content_type, reqs)
            return False

        if wsres.getcode() != 200:
            # TODO: flip out
            return False

        data_str = wsres.read().strip()

        if wsres.info()['Content-Type'].split(';')[0].strip() == ROS_MSG_MIMETYPE:
            rosresp.deserialize(data_str)
        elif wsres.info()['Content-Type'].split(';')[0].strip() == 'application/jwt':
            data = self.jwt_iface.decode(data_str)
            data.pop('_format', None)
            msgconv.populate_instance(data, rosresp)
        else:
            data = json.loads(data_str)
            data.pop('_format', None)
            msgconv.populate_instance(data, rosresp)

        return rosresp


def create_service_proxy(url, name, service_type, binary=None, use_jwt=False, jwt_key='ros'):
    try:
        service_type_module, service_type_name = tuple(service_type.split('/'))
        roslib.load_manifest(service_type_module)
        srv_module = import_module(service_type_module + '.srv')
        return IndividualServiceProxy(url, name, srv_module, service_type_name, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)
    except Exception, e:
        print "Unknown service type %s" % service_type
        return None


class IndividualTopicProxy:
    _publisher_threads = []

    @classmethod
    def start(cls):
        for thread in cls._publisher_threads:
            if not thread.isAlive():
                try:
                    thread.start()
                except Exception, e:
                    rospy.logerr("IndividualTopicProxy::start: %s", str(e))

    def __init__(self, url, name, msg_module, topic_type_name, pub=True, sub=True, publish_interval=None, binary=None, use_jwt=False, jwt_key='ros'):
        self._use_jwt = use_jwt

        self.jwt_iface = JwtInterface(key=jwt_key, algorithm='HS256')

        self.url = url
        self.name = name

        self.binary = binary or False

        self.pub = pub
        self.sub = sub

        self.rostype = getattr(msg_module, topic_type_name)

        self.publish_interval = publish_interval or 1
        self.publisher = None
        if self.pub:
            rospy.logwarn(
                'IndividualTopicProxy::__init__: publishing to %s from remote %s', name, self.url)
            self.publisher = rospy.Publisher(name, self.rostype, queue_size=10)
            self.publisher_thread = threading.Thread(
                target=self.publish, name=name)
            self._publisher_threads.append(self.publisher_thread)

        self.subscriber = None
        if self.sub:
            if self.pub:
                name += '_out'
            rospy.logwarn(
                'IndividualTopicProxy::__init__: subscribing to %s and sending to remote %s', name, self.url)
            self.subscriber = rospy.Subscriber(
                name, self.rostype, self.callback)

    def callback(self, msg):
        # rospy.loginfo('IndividualTopicProxy::callback')

        if self.binary:
            req = StringIO()
            msg.serialize(req)
            reqs = req.getvalue()
            content_type = ROS_MSG_MIMETYPE_WITH_TYPE(self.rostype)
        elif self._use_jwt:
            req = msgconv.extract_values(msg)
            reqs = self.jwt_iface.encode(req)
            content_type = 'application/jwt'
        else:
            req = msgconv.extract_values(msg)
            req['_format'] = 'ros'
            reqs = json.dumps(req)
            content_type = 'application/json'

        wsreq = urllib2.Request(self.url.encode(
            'utf-8'), data=reqs, headers={'Content-Type': content_type})
        try:
            wsres = urllib2.urlopen(wsreq, timeout=_timeout)
        except Exception, e:
            rospy.logerr(
                'IndividualTopicProxy::callback: Exception: %s. Content type: %s, reqs: %s', e, content_type, reqs)
            return
            # raise e

        if wsres.getcode() != 200:
            # TODO: flip out
            pass

    def publish(self):
        stop = False
        while not (stop or rospy.is_shutdown()):

            if self.binary:
                content_accept = ROS_MSG_MIMETYPE
            elif self._use_jwt:
                content_accept = 'application/jwt'
            else:
                content_accept = 'application/json'

            wsreq = urllib2.Request(
                self.url, headers={'Accept': content_accept})
            ret_code = 0
            try:
                wsres = urllib2.urlopen(wsreq, timeout=_timeout)
                ret_code = wsres.getcode()
            except Exception, e:
                rospy.logerr("IndividualTopicProxy::publish: Encountered an error while retrieving a message on topic %s: %s\n" % (
                    self.name, str(e)))

            if ret_code == 200:
                data_str = None
                try:
                    data_str = wsres.read()
                except urllib2.URLError, e:
                    rospy.logerr("IndividualTopicProxy::publish: urllib2.URLError on topic %s: %s\n" % (
                        self.name, str(e)))
                except socket.timeout, e:
                    rospy.logerr("IndividualTopicProxy::publish: socket.timeout on topic %s: %s\n" % (
                        self.name, str(e)))

                msg = self.rostype()
                if wsres.info()['Content-Type'].split(';')[0].strip() == ROS_MSG_MIMETYPE:
                    if not data_str:
                        msg = None
                    else:
                        msg.deserialize(data_str)
                elif wsres.info()['Content-Type'].split(';')[0].strip() == 'application/jwt':
                    if len(data_str.strip()) > 0:
                        data = self.jwt_iface.decode(data_str.strip())
                        if not data:
                            msg = None
                        else:
                            data.pop('_format', None)
                            try:
                                msgconv.populate_instance(data, msg)
                            except Exception, e:
                                rospy.logerr("IndividualTopicProxy::publish: Encountered an error while creating a message on topic %s: %s\n" % (
                                    self.name, str(e)))
                                msg = None
                    else:
                        msg = None
                else:
                    try:
                        data = json.loads(data_str.strip())
                    except Exception, e:
                        rospy.logerr("IndividualTopicProxy::publish: Encountered an error while decoding a message on topic %s: %s\n" % (
                            self.name, str(e)))
                        data = None

                    if not data:
                        msg = None
                    else:
                        data.pop('_format', None)
                        try:
                            msgconv.populate_instance(data, msg)
                        except Exception, e:
                            rospy.logerr("IndividualTopicProxy::publish: Encountered an error while creating a message on topic %s: %s\n" % (
                                self.name, str(e)))
                            msg = None

                if msg is not None:
                    self.publisher.publish(msg)

            rospy.sleep(self.publish_interval)


def create_topic_proxy(url, name, topic_type, pub=True, sub=True, publish_interval=None, binary=None, use_jwt=False, jwt_key='ros'):
    try:
        topic_type_module, topic_type_name = tuple(topic_type.split('/'))
        # rospy.loginfo('create_topic_proxy: module = %s, name = %s, pub = %s, sub = %s', topic_type_module, topic_type_name, str(pub), str(sub))
        roslib.load_manifest(topic_type_module)
        msg_module = import_module(topic_type_module + '.msg')

        return IndividualTopicProxy(url, name, msg_module, topic_type_name,
                                    pub=pub, sub=sub, publish_interval=publish_interval, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)
    except Exception, e:
        rospy.logerr(
            "create_topic_proxy: Unknown msg type %s for topic %s" % (topic_type, name))
        return None


def create_action_proxies(url, name, action_type, publish_interval=None, binary=None, use_jwt=False, jwt_key='ros'):
    try:
        action_type_module, action_type_name = tuple(action_type.split('/'))
        roslib.load_manifest(action_type_module)
        msg_module = import_module(action_type_module + '.msg')

        proxies = {}

        proxies['status'] = IndividualTopicProxy(url + '/status', name + '/status', actionlib_msgs.msg, 'GoalStatusArray',
                                                 pub=True, sub=False, publish_interval=publish_interval, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)
        proxies['result'] = IndividualTopicProxy(url + '/result', name + '/result', msg_module, action_type_name + 'ActionResult',
                                                 pub=True, sub=False, publish_interval=publish_interval, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)
        proxies['feedback'] = IndividualTopicProxy(url + '/feedback', name + '/feedback', msg_module, action_type_name + 'ActionFeedback',
                                                   pub=True, sub=False, publish_interval=publish_interval, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)

        proxies['goal'] = IndividualTopicProxy(url + '/goal', name + '/goal', msg_module, action_type_name +
                                               'ActionGoal', pub=False, sub=True, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)
        proxies['cancel'] = IndividualTopicProxy(url + '/cancel', name + '/cancel', actionlib_msgs.msg,
                                                 'GoalID', pub=False, sub=True, binary=binary, use_jwt=use_jwt, jwt_key=jwt_key)

        return proxies
    except Exception, e:
        rospy.logerr(
            "create_action_proxies: Unknown action type %s", action_type)
        return None


class RostfulServiceProxy:
    def __init__(self, url, remap=False, subscribe=False, publish_interval=None, binary=None, prefix=None, use_jwt=False, jwt_key='ros', rest_prefix_server=''):

        self.rest_prefix_server = rest_prefix_server

        if url.endswith('/'):
            url = url[:-1]

        if self.rest_prefix_server.endswith('/'):
            self.rest_prefix_server = self.rest_prefix_server[:-1]
        if not self.rest_prefix_server.startswith('/'):
            self.rest_prefix_server = '/' + self.rest_prefix_server

        self.url = url+self.rest_prefix_server

        if self.url.endswith('/'):
            self.url = self.url[:-1]

        self.binary = binary

        self.prefix = prefix

        self.services = {}
        self.topics = {}
        self.actions = {}

        self.subscribe = subscribe
        self.publish_interval = publish_interval
        self.remap = remap
        self.use_jwt = use_jwt
        self.jwt_key = jwt_key
        self.ros_setup_timer = 10

        self.rosSetup()

        return

    def setup_service(self, service_url, service_name, service_type, remap=False, use_jwt=False, jwt_key='ros'):
        if remap:
            service_name = service_name + '_ws'

        if not self.services.has_key(service_name):
            proxy = create_service_proxy(service_url, service_name, service_type,
                                         binary=self.binary, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
            if proxy is None:
                return False
            self.services[service_name] = proxy
        # else:
        # rospy.loginfo('RostfulServiceProxy::setup_service: Service %s already exists', service_name)

        return True

    def setup_topic(self, topic_url, topic_name, topic_type, pub=None, sub=None, remap=False, publish_interval=None, use_jwt=False, jwt_key='ros'):
        if remap:
            topic_name = topic_name + '_ws'

        if pub is None:
            pub = True
        if sub is None:
            sub = True

        if not self.topics.has_key(topic_name):
            proxy = create_topic_proxy(topic_url, topic_name, topic_type, pub=pub, sub=sub,
                                       publish_interval=publish_interval, binary=self.binary, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
            if proxy is None:
                return False
            self.topics[topic_name] = proxy
        # else:
        # rospy.loginfo('RostfulServiceProxy::setup_topic: Topic %s already exists', topic_name)

        return True

    def setup_action(self, action_url, action_name, action_type, remap=False, publish_interval=None, use_jwt=False, jwt_key='ros'):
        if remap:
            action_name = action_name + '_ws'

        if not self.actions.has_key(action_name):
            proxy = create_action_proxies(action_url, action_name, action_type, publish_interval=publish_interval,
                                          binary=self.binary, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
            if proxy is None:
                return False
            self.actions[action_name] = proxy
        # else:
        # rospy.loginfo('RostfulServiceProxy::setup_action: Action %s already exists', action_name)

        return True

    def rosSetup(self):
        '''
                Creates and inits ROS components
        '''
        if not rospy.is_shutdown():
            config_url = self.url + '/_rosdef'

            req = urllib2.Request(config_url)
            ret = True
            try:
                res = urllib2.urlopen(req, timeout=_timeout)
                if res.getcode() != 200:
                    rospy.logerr(
                        'RostfulServiceProxy::rosSetup: error getting info from url %s', self.url)
                    ret = False
            except Exception, e:
                rospy.logerr(
                    'RostfulServiceProxy::rosSetup: error waiting for url %s: %s', self.url, e)
                ret = False

            if ret:
                parser = deffile.DefFileParser()
                parser.add_default_section_parser(deffile.INISectionParser)

                dfile = parser.parse(res.read().strip())

                # print 'rosSetup'
                # print dfile

                if dfile.type == 'Node':
                    if self.prefix is None:
                        prefix = dfile.manifest['Name'] or ''
                    else:
                        prefix = self.prefix
                    if prefix:
                        prefix += '/'
                    services = dfile.get_section('Services')
                    if services:
                        # print 'Services:'
                        for service_name, service_type in services.iteritems():
                            ret = self.setup_service(self.url + '/' + service_name, prefix + service_name,
                                                     service_type, remap=self.remap, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
                            # if ret: print '  %s (%s)' % (prefix + service_name, service_type)

                    topic_dict = {}
                    topic_info = namedtuple('topic_info', 'type pub sub')

                    topic_section = dfile.get_section('Topics')
                    if topic_section:
                        # print 'Topic section'
                        for topic_name, topic_type in topic_section.iteritems():
                            topic_dict[topic_name] = topic_info(
                                type=topic_type, pub=True, sub=self.subscribe)

                    published_section = dfile.get_section('Publishes')
                    if published_section:
                        # print 'Published section'
                        for topic_name, topic_type in published_section.iteritems():
                            sub = topic_dict[topic_name].sub if topic_dict.has_key(
                                topic_name) else False
                            topic_dict[topic_name] = topic_info(
                                type=topic_type, pub=True, sub=sub)
                            # print '%s-%s'%(topic_name,str(topic_dict[topic_name]))

                    subscribed_section = dfile.get_section('Subscribes')
                    if subscribed_section:
                        # print 'Subscribed section'
                        for topic_name, topic_type in subscribed_section.iteritems():
                            pub = topic_dict[topic_name].pub if topic_dict.has_key(
                                topic_name) else False
                            topic_dict[topic_name] = topic_info(
                                type=topic_type, pub=pub, sub=self.subscribe)
                            # print '%s-%s'%(topic_name,str(topic_dict[topic_name]))

                    topics = {}
                    published_topics = {}
                    subscribed_topics = {}
                    for topic_name, info in topic_dict.iteritems():
                        if info.pub and info.sub:
                            topics[topic_name] = info.type
                        elif info.pub:
                            published_topics[topic_name] = info.type
                        elif info.sub:
                            subscribed_topics[topic_name] = info.type

                    if topics:
                        # rospy.loginfo('rostful_client::RostfulServiceProxy: Publishing and subscribing')
                        for topic_name, topic_type in topics.iteritems():
                            ret = self.setup_topic(self.url + '/' + topic_name, prefix + topic_name, topic_type, pub=True, sub=True,
                                                   remap=self.remap, publish_interval=self.publish_interval, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
                            # if ret: print '  %s (%s)' % (prefix + topic_name, topic_type)

                    if published_topics:
                        # rospy.loginfo('rostful_client::RostfulServiceProxy: Publishing')
                        for topic_name, topic_type in published_topics.iteritems():
                            ret = self.setup_topic(self.url + '/' + topic_name, prefix + topic_name, topic_type, pub=True,
                                                   remap=self.remap, publish_interval=self.publish_interval, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
                            # if ret: print '  %s (%s)' % (prefix + topic_name, topic_type)

                    if subscribed_topics:
                        # rospy.loginfo('rostful_client::RostfulServiceProxy: Subscribing')
                        for topic_name, topic_type in subscribed_topics.iteritems():
                            ret = self.setup_topic(self.url + '/' + topic_name, prefix + topic_name, topic_type, pub=False, sub=True,
                                                   remap=self.remap, publish_interval=self.publish_interval, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
                            # if ret: print '  %s (%s)' % (prefix + topic_name, topic_type)

                    actions = dfile.get_section('Actions')
                    if actions:
                        # print 'Actions:'
                        for action_name, action_type in actions.iteritems():
                            ret = self.setup_action(self.url + '/' + action_name, prefix + action_name, action_type, remap=self.remap,
                                                    publish_interval=self.publish_interval, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
                            # if ret: print '  %s (%s)' % (prefix + action_name, action_type)
                elif dfile.type == 'Service':
                    ret = self.setup_service(
                        self.url, dfile.manifest['Name'], dfile.manifest['Type'], remap=self.remap)
                    # if ret: print 'Connected to service %s (%s)' % (dfile.manifest['Name'], dfile.manifest['Type'])
                elif dfile.type == 'Topic':
                    pub = dfile.manifest['Subscribes'].lower() == 'true'
                    sub = dfile.manifest['Publishes'].lower(
                    ) == 'true' and self.subscribe
                    ret = self.setup_topic(self.url, dfile.manifest['Name'], dfile.manifest['Type'], pub=pub, sub=sub,
                                           remap=self.remap, publish_interval=self.publish_interval, use_jwt=self.use_jwt, jwt_key=self.jwt_key)
                    # if ret: print 'Connected to topic %s (%s)' % (dfile.manifest['Name'], dfile.manifest['Type'])
                elif dfile.type == 'Action':
                    ret = self.setup_action(
                        self.url, dfile.manifest['Name'], dfile.manifest['Type'], remap=self.remap, publish_interval=self.publish_interval)
                    # if ret: print 'Connected to action %s (%s)' % (dfile.manifest['Name'], dfile.manifest['Type'])

                IndividualTopicProxy.start()

            self.t_ros_setup = threading.Timer(
                self.ros_setup_timer, self.rosSetup)
            self.t_ros_setup.start()


def clientmain():
    rospy.init_node('rostful_client', anonymous=True)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        'url', help='The url of the web service or one of its individual services, topics, or actions.')

    parser.add_argument('--allow-subscription', '--sub', dest='subscribe', action='store_true', default=False,
                        help='This option must be given to allow the web service to subscribe to local topics.')
    parser.add_argument('--publish-interval', '-i', type=float,
                        help='The rate to retrieve and publish messages from the web service.')

    parser.add_argument('--binary', action='store_true', default=False,
                        help='Using serialized ROS messages instead of rosbridge JSON.')

    parser.add_argument('--test', action='store_true', default=False,
                        help='Use if server and client are using the same ROS master for testing. Client service and topic names will have _ws appended.')
    parser.add_argument('--jwt', action='store_true', default=False,
                        help='This argument enables the use of JWT to guarantee a secure transmission')
    parser.add_argument('--jwt-key', default='ros',
                        help='This arguments sets the key to encode/decode the data')

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument(
        '--prefix', help='Specify a prefix for the service and topic names. By default, this is the name given by the web service if it provides one.')
    grp.add_argument('--no-prefix', action='store_const', const='', dest='prefix',
                     help='Use the service and topic names as-is as relative names.')
    parser.add_argument('--rest-prefix-server', default='/',
                        help='The prefix path of the http request to the server, usually starting with a "/".')
    parser.add_argument('--connection-timeout', default=5,
                        help='Timeout for any petition to the server')

    args = parser.parse_args(rospy.myargv()[1:])

    if not args.url.startswith('http'):
        args.url = 'http://' + args.url

    _timeout = args.connection_timeout

    proxy = RostfulServiceProxy(args.url, remap=args.test, subscribe=args.subscribe, publish_interval=args.publish_interval,
                                binary=args.binary, prefix=args.prefix, use_jwt=args.jwt, jwt_key=args.jwt_key, rest_prefix_server=args.rest_prefix_server)

    rospy.spin()
