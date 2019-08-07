from __future__ import absolute_import
import urlparse

import roslib
from .transforms import mappings, BadTransform, NullTransform

roslib.load_manifest('rostful')
import rospy
from rospy.service import ServiceManager
import rosservice, rostopic
import actionlib_msgs.msg

from importlib import import_module
from collections import deque

import json
import sys
import re
from StringIO import StringIO

from . import message_conversion as msgconv
from . import deffile, definitions

from .util import ROS_MSG_MIMETYPE, get_query_bool

import time, threading

from .jwt_interface import JwtInterface

from xml.etree.ElementTree import fromstring

import xmltodict
import dicttoxml


TOPIC_SUBSCRIBER_TIMEOUT=5.0

class Service:
	def __init__(self, service_name, service_type):
		self.name = service_name

		service_type_module, service_type_name = tuple(service_type.split('/'))
		roslib.load_manifest(service_type_module)
		srv_module = import_module(service_type_module + '.srv')

		self.rostype_name = service_type
		self.rostype = getattr(srv_module, service_type_name)
		self.rostype_req = getattr(srv_module, service_type_name + 'Request')
		self.rostype_resp = getattr(srv_module, service_type_name + 'Response')

		self.proxy = rospy.ServiceProxy(self.name, self.rostype)

	def call(self, rosreq):
#		rosreq = self.rostype_req()
#		if use_ros:
#			rosreq.deserialize(req)
#		else:
#			msgconv.populate_instance(req, rosreq)

		fields = []
		for slot in rosreq.__slots__:
			fields.append(getattr(rosreq, slot))
		fields = tuple(fields)

		return self.proxy(*fields)

class Topic:
	def __init__(self, topic_name, topic_type, allow_pub=True, allow_sub=True, queue_size=10):
		self.name = topic_name

		topic_type_module, topic_type_name = tuple(topic_type.split('/'))
		roslib.load_manifest(topic_type_module)
		msg_module = import_module(topic_type_module + '.msg')

		self.rostype_name = topic_type
		self.rostype = getattr(msg_module, topic_type_name)

		self.allow_pub = allow_pub
		self.allow_sub = allow_sub

		self.msg = deque([], queue_size)

		self.pub = None
		if self.allow_pub:
			self.pub = rospy.Publisher(self.name, self.rostype, queue_size=10)

		self.sub = None
		if self.allow_sub:
			self.sub = rospy.Subscriber(self.name, self.rostype, self.topic_callback)

		self._last_msg_time = rospy.Time(0)

	def publish(self, msg):
		self.pub.publish(msg)
		return

	def get(self, num=None):
		if not self.msg or len(self.msg) == 0:
			return None

		return self.msg.popleft()

	def topic_callback(self, msg):
		self.msg.append(msg)
		self._last_msg_time = rospy.Time.now()

	def is_active(self):
		'''
			Returns true if the topic is receiving messages
		'''
		return (rospy.Time.now() - self._last_msg_time ).to_sec() <= TOPIC_SUBSCRIBER_TIMEOUT

"""
Publications:
 * /averaging/status [actionlib_msgs/GoalStatusArray]
 * /averaging/result [actionlib_tutorials/AveragingActionResult]
 * /rosout [rosgraph_msgs/Log]
 * /averaging/feedback [actionlib_tutorials/AveragingActionFeedback]

Subscriptions:
 * /random_number [unknown type]
 * /averaging/goal [actionlib_tutorials/AveragingActionGoal]
 * /averaging/cancel [actionlib_msgs/GoalID]
 """


class Action:
	STATUS_SUFFIX = 'status'
	RESULT_SUFFIX = 'result'
	FEEDBACK_SUFFIX = 'feedback'
	GOAL_SUFFIX = 'goal'
	CANCEL_SUFFIX = 'cancel'

	def __init__(self, action_name, action_type, queue_size=1):
		self.name = action_name

		action_type_module, action_type_name = tuple(action_type.split('/'))
		roslib.load_manifest(action_type_module)
		msg_module = import_module(action_type_module + '.msg')

		self.rostype_name = action_type

		self.rostype_action = getattr(msg_module, action_type_name + 'Action')

		self.rostype_action_goal = getattr(msg_module, action_type_name + 'ActionGoal')
		self.rostype_action_result = getattr(msg_module, action_type_name + 'ActionResult')
		self.rostype_action_feedback = getattr(msg_module, action_type_name + 'ActionFeedback')

		self.rostype_goal = getattr(msg_module, action_type_name + 'Goal')
		self.rostype_result = getattr(msg_module, action_type_name + 'Result')
		self.rostype_feedback = getattr(msg_module, action_type_name + 'Feedback')

		self.status_msg = deque([], queue_size)
		self.status_sub = rospy.Subscriber(self.name + '/' +self.STATUS_SUFFIX, actionlib_msgs.msg.GoalStatusArray, self.status_callback)

		self.result_msg = deque([], queue_size)
		self.result_sub = rospy.Subscriber(self.name + '/' + self.RESULT_SUFFIX, self.rostype_action_result, self.result_callback)

		self.feedback_msg = deque([], queue_size)
		self.feedback_sub = rospy.Subscriber(self.name + '/' +self.FEEDBACK_SUFFIX, self.rostype_action_feedback, self.feedback_callback)

		self.goal_pub = rospy.Publisher(self.name + '/' + self.GOAL_SUFFIX, self.rostype_action_goal, queue_size = 10)
		self.cancel_pub = rospy.Publisher(self.name + '/' +self.CANCEL_SUFFIX, actionlib_msgs.msg.GoalID, queue_size = 10)

	def get_msg_type(self, suffix):
		if suffix == self.STATUS_SUFFIX:
			return actionlib_msgs.msg.GoalStatusArray
		elif suffix == self.RESULT_SUFFIX:
			return self.rostype_action_result
		elif suffix == self.FEEDBACK_SUFFIX:
			return self.rostype_action_feedback
		elif suffix == self.GOAL_SUFFIX:
			return self.rostype_action_goal
		elif suffix == self.CANCEL_SUFFIX:
			return actionlib_msgs.msg.GoalID
		else:
			return None

	def publish_goal(self, msg):
		self.goal_pub.publish(msg)
		return

	def publish_cancel(self, msg):
		self.cancel_pub.publish(msg)
		return

	def publish(self, suffix, msg):
		if suffix == self.GOAL_SUFFIX:
			self.publish_goal(msg)
		elif suffix == self.CANCEL_SUFFIX:
			self.publish_cancel(msg)

	def get_status(self, num=None):
		if not self.status_msg:
			return None

		return self.status_msg[0]

	def get_result(self, num=None):
		if not self.result_msg:
			return None

		return self.result_msg[0]

	def get_feedback(self, num=None):
		if not self.feedback_msg:
			return None

		return self.feedback_msg[0]

	def get(self, suffix, num=None):
		if suffix == self.STATUS_SUFFIX:
			return self.get_status(num=num)
		elif suffix == self.RESULT_SUFFIX:
			return self.get_result(num=num)
		elif suffix == self.FEEDBACK_SUFFIX:
			return self.get_feedback(num=num)
		else:
			return None

	def status_callback(self, msg):
		self.status_msg.appendleft(msg)

	def result_callback(self, msg):
		self.result_msg.appendleft(msg)

	def feedback_callback(self, msg):
		self.feedback_msg.appendleft(msg)

CONFIG_PATH = '_rosdef'
SRV_PATH = '_srv'
MSG_PATH = '_msg'
ACTION_PATH = '_action'

def get_suffix(path):
	suffixes = '|'.join([re.escape(s) for s in [CONFIG_PATH,SRV_PATH,MSG_PATH,ACTION_PATH]])
	match = re.search(r'/(%s)$' % suffixes, path)
	return match.group(1) if match else ''

def response(start_response, status, data, content_type):
	content_length = 0
	if data is not None:
		content_length = len(data)
	headers = [('Content-Type', content_type), ('Content-Length', str(content_length))]
	start_response(status, headers)
	return data

def response_200(start_response, data='', content_type='application/json'):
	return response(start_response, '200 OK', data, content_type)

def response_404(start_response, data='Invalid URL!', content_type='text/plain'):
	return response(start_response, '404 Not Found', data, content_type)

def response_405(start_response, data=[], content_type='text/plain'):
	return response(start_response, '405 Method Not Allowed', data, content_type)

def response_500(start_response, error, content_type='text/plain'):
	e_str = '%s: %s' % (str(type(error)), str(error))
	return response(start_response, '500 Internal Server Error', e_str, content_type)

def response_503(start_response, error, content_type='text/plain'):
	e_str = '%s: %s' % (str(type(error)), str(error))
	return response(start_response, '503 Service Unavailable', e_str, content_type)

class RostfulServer:
	def __init__(self, args, args_dict):
		self.services = {}
		self.topics = {}
		self.actions = {}
		self.rest_prefix = args_dict["rest-prefix"]
		self._use_jwt = args.jwt
		self.jwt_iface = JwtInterface(key = args.jwt_key, algorithm = args.jwt_alg)
		self.ros_setup_timer = 10
		self.args = args
		self.args_dict = args_dict

		if self.rest_prefix != '/':
			if not self.rest_prefix.startswith('/'):
				self.rest_prefix = '/'+self.rest_prefix
			if not self.rest_prefix.endswith('/'):
				self.rest_prefix = self.rest_prefix+'/'

		self.rosSetup()

	def add_service(self, service_name, ws_name=None, service_type=None):
		resolved_service_name = rospy.resolve_name(service_name)
		if service_type is None:
			try:
				service_type = rosservice.get_service_type(resolved_service_name)
				if not service_type:
					rospy.logerr('RostfulServer::add_service: Unknown service %s',service_name)
					return False
			except Exception, e:
				rospy.logerr("RostfulServer::add_service: Error getting Service Type for %s" %(service_name))
				return False

		if ws_name is None:
			ws_name = service_name
		else:
			ws_name = ws_name + service_name
		if ws_name.startswith('/'):
			ws_name = ws_name[1:]
		try:
			self.services[ws_name] = Service(service_name, service_type)
			rospy.loginfo('RostfulServer::add_service: service_name=%s, service_type = %s, service_url = %s',
							service_name, service_type, ws_name)
		except Exception, e:
			rospy.logerr("RostfulServer::add_service: Error creating Service for %s:%s" %(service_name, service_type))
			return False

		return True

	def add_services(self, service_names):
		if not service_names:
			return
		#print "Adding services:"
		for service_name in service_names:
			if service_name.startswith('/'):
				service_name_key = service_name[1:]
			else:
				service_name_key = service_name
			for prefix_type in self.args_dict['types']:
				if not self.services.has_key(prefix_type+"/"+service_name_key) and not self.services.has_key(service_name_key):
					ret = self.add_service(service_name, ws_name=prefix_type)
					if ret: rospy.loginfo('RostfulServer::add_services: added %s', service_name)

	def add_topic(self, topic_name, ws_name=None, topic_type=None, allow_pub=True, allow_sub=True):
		resolved_topic_name = rospy.resolve_name(topic_name)
		#rospy.loginfo('RostfulServer: add_topic: topic_name=%s, resolved_topic_name = %s', topic_name, resolved_topic_name)
		if topic_type is None:
			topic_type, _, _ = rostopic.get_topic_type(resolved_topic_name)
			if not topic_type:
				rospy.logerr('RostfulServer::add_topic: Unknown topic %s', topic_name)
				return False

		if ws_name is None:
			ws_name = topic_name
		else:
			ws_name = ws_name + topic_name
		if ws_name.startswith('/'):
			ws_name = ws_name[1:]

		try:
			self.topics[ws_name] = Topic(topic_name, topic_type, allow_pub=allow_pub, allow_sub=allow_sub)
			rospy.loginfo('RostfulServer::add_topic: topic_name=%s, topic_type = %s, topic_url = %s',
							topic_name, topic_type, ws_name)
		except Exception, e:
			rospy.logerr("RostfulServer::add_topic: Error creating Topic for %s:%s" %(topic_name, topic_type))
			return False

		return True

	def add_topics(self, topic_names, allow_pub=True, allow_sub=True):
		if not topic_names:
			return
		'''if allow_pub and allow_sub:
			print "Publishing and subscribing to topics:"
		elif allow_sub:
			print "Publishing topics:"
		elif allow_pub:
			print "Subscribing to topics"
		'''
		for topic_name in topic_names:
			if topic_name.startswith('/'):
				topic_name_key = topic_name[1:]
			else:
				topic_name_key = topic_name
			for prefix_type in self.args_dict['types']:
				if not self.topics.has_key(prefix_type+"/"+topic_name_key) and not self.topics.has_key(topic_name_key):
					self.add_topic(topic_name, ws_name=prefix_type,
					               allow_pub=allow_pub, allow_sub=allow_sub)

	def add_action(self, action_name, ws_name=None, action_type=None):
		if action_type is None:
			resolved_topic_name = rospy.resolve_name(action_name + '/result')
			topic_type, _, _ = rostopic.get_topic_type(resolved_topic_name)
			if not topic_type:
				rospy.logerr('RostfulServer::add_action: Unknown action %s', action_name)
				return False
			action_type = topic_type[:-len('ActionResult')]

		if ws_name is None:
			ws_name = action_name
		if ws_name.startswith('/'):
			ws_name = ws_name[1:]

		self.actions[ws_name] = Action(action_name, action_type)
		return True

	def add_actions(self, action_names):
		if not action_names:
			return
		#print "Adding actions:"
		for action_name in action_names:
			if action_name.startswith('/'):
				action_name_key = action_name[1:]
			else:
				action_name_key = action_name
			if not self.actions.has_key(action_name_key):
				ret = self.add_action(action_name)
				if ret: rospy.loginfo('RostfulServer::add_actions: added action %s', action_name)

	def wsgifunc(self):
		"""Returns the WSGI-compatible function for this server."""
		return self._handle

	def _handle(self, environ, start_response):
		if environ['REQUEST_METHOD'] == 'GET':
			return self._handle_get(environ, start_response)
		elif environ['REQUEST_METHOD'] == 'POST':
			return self._handle_post(environ, start_response)
		else:
			#TODO: flip out
			pass

	def _handle_get(self, environ, start_response):
		output_data = None
		path = environ['PATH_INFO']

		# Create index page
		if path == '/':
			data = "<html><head></head><body><h1>Server configuration</h1>"
			data = data + "<h2>Topics</h2>"
			for topic_key in sorted(self.topics.keys())	:
				data = data + "<h3><a href=" + \
                                    self.args_dict["host"]+":"+str(self.args_dict["port"]) + \
                                    "/"+topic_key+">"+topic_key+"</a></h3>"
			data = data + "</body></html>"

			data = data + "<h2>Services</h2>"
			for service_key in sorted(self.services.keys())	:
				data = data + "<h3><a href=" + \
                                    self.args_dict["host"]+":"+str(self.args_dict["port"]) + \
                                    "/"+service_key+">"+service_key+"</a></h3>"
			data = data + "</body></html>"
			headers = [('Content-Type', 'text/html'),
                            ('Content-Length', str(data))]
			start_response('200 OK', headers)
			return data

		if path.endswith('/'):
			path = path[:len(path)-1]
		full = get_query_bool(environ['QUERY_STRING'], 'full')

		kwargs = urlparse.parse_qs(environ['QUERY_STRING'], keep_blank_values=True)
		for key in kwargs:
			kwargs[key] = kwargs[key][-1]

		# TODO: rest_prefix should be after the response type prefix
		if self.rest_prefix and path.startswith(self.rest_prefix):
			path = path[len(self.rest_prefix):]

		jsn = False
		handler = NullTransform()
		first_path = path.split('/')[0].split('.')
		last_path = path.split('/')[-1].split('.')
		if len(last_path) > 1:
			file_type = last_path[-1]
			path = path[:-(len(file_type) + 1)]
			# cuts suffix ('json') and preceding period
			if file_type in mappings:
				handler = mappings.get(file_type)
			else:
				handler = BadTransform()
		else:
			jsn = get_query_bool(environ['QUERY_STRING'], 'json')



		use_ros = environ.get('HTTP_ACCEPT','').find(ROS_MSG_MIMETYPE) != -1

		suffix = get_suffix(path)
		# TODO: Figure out what this does
		#rospy.loginfo('jsn = %s, path = %s, suffix = %s, use_ros=%s', str(jsn),path,suffix,str(use_ros))

		if path == CONFIG_PATH:
			dfile = definitions.manifest(self.services, self.topics, self.actions, full=full)
			if jsn:
				return response_200(start_response, str(dfile.tojson()), content_type='application/json')
			else:
				return response_200(start_response, dfile.tostring(suppress_formats=True), content_type='text/plain')

		if not suffix:
			topic_name = ''
			if not self.topics.has_key(path):
				for action_suffix in [Action.STATUS_SUFFIX,Action.RESULT_SUFFIX,Action.FEEDBACK_SUFFIX]:
					action_name = path[:-(len(action_suffix)+1)]
					if path.endswith('/' + action_suffix) and self.actions.has_key(action_name):
						action = self.actions[action_name]
						topic_name = action.name
						msg = action.get(action_suffix)
						break
				else:
					# If the path does not exist, we need to
					# check if the user is trying to get an
					# attribute of a message returned by a topic
					# that has an existing path
					topic_keys = self.topics.keys()
					for key in topic_keys:
						if path.startswith(key):
							topic = self.topics[key]
							topic_name = topic.name
							if not topic.allow_sub:
								rospy.logwarn(
									"RostfulServer::handle_get: %s does not allow subscription", topic)
								return response_405(start_response)
							msg = topic.get()

							msg_atribs = path[len(key)+1:].split('/')
							if len(msg_atribs) > 0:
								msg_json = msgconv.extract_values(msg)
								for atrib in msg_atribs:
									msg_json = msg_json[atrib]
									output_data = {atrib: msg_json}
							break
					else:
						return response_404(start_response)
			else:
				topic = self.topics[path]

				if not topic.allow_sub:
					rospy.logwarn("RostfulServer::handle_get: %s does not allow subscription", topic)
					return response_405(start_response)

				topic_name = topic.name
				msg = topic.get()

			if msg is None:
				rospy.logerr('RostfulServer::_handle_get: topic %s not available', topic_name)
				return response_503(start_response, 'ROS message unavailable')

			if handler and handler.valid():
				return handler.get(start_response, msg, **kwargs)
			elif use_ros:
				content_type = ROS_MSG_MIMETYPE
				output_data = StringIO()
				if msg is not None:
					msg.serialize(output_data)
				output_data = output_data.getvalue()
			elif self._use_jwt:
				content_type = 'application/jwt'
				output_data = msgconv.extract_values(msg)
				output_data = self.jwt_iface.encode(output_data)
			else:
				if output_data is None:
					output_data = msgconv.extract_values(msg)
				if first_path[0] == "xml":
					content_type = 'application/xml'
					if len(output_data.keys()) > 1:
						xml = dicttoxml.dicttoxml(output_data, attr_type=False, root=True)
						output_data = xml.replace('root>', path.split('/')[-1] + '>')
					else:
						xml = dicttoxml.dicttoxml(output_data, attr_type=False, root=False)
						output_data = xml
				else:
					content_type = 'application/json'
					output_data = json.dumps(output_data)

			#rospy.loginfo('ret 1: %s, type: %s',output_data, content_type)
			return response_200(start_response, output_data, content_type=content_type)

		path = path[:-(len(suffix)+1)]

		if suffix == MSG_PATH and self.topics.has_key(path):
				return response_200(start_response, definitions.get_topic_msg(self.topics[path]), content_type='text/plain')
		elif suffix == SRV_PATH and self.services.has_key(path):
				return response_200(start_response, definitions.get_service_srv(self.services[path]), content_type='text/plain')
		elif suffix == ACTION_PATH and self.actions.has_key(path):
				return response_200(start_response, definitions.get_action_action(self.actions[path]), content_type='text/plain')
		elif suffix == CONFIG_PATH:
			if self.services.has_key(path):
				service_name = path

				service = self.services[service_name]
				dfile = definitions.describe_service(service_name, service, full=full)

				if jsn:
					return response_200(start_response, str(dfile.tojson()), content_type='application/json')
				else:
					return response_200(start_response, dfile.tostring(suppress_formats=True), content_type='text/plain')
			elif self.topics.has_key(path):
				topic_name = path

				topic = self.topics[topic_name]
				dfile = definitions.describe_topic(topic_name, topic, full=full)

				if jsn:
					return response_200(start_response, str(dfile.tojson()), content_type='application/json')
				else:
					return response_200(start_response, dfile.tostring(suppress_formats=True), content_type='text/plain')
			elif self.actions.has_key(path):
				action_name = path

				action = self.actions[action_name]
				dfile = definitions.describe_action(action_name, action, full=full)

				if jsn:
					return response_200(start_response, str(dfile.tojson()), content_type='application/json')
				else:
					return response_200(start_response, dfile.tostring(suppress_formats=True), content_type='text/plain')
			else:
				for suffix in [Action.STATUS_SUFFIX,Action.RESULT_SUFFIX,Action.FEEDBACK_SUFFIX,Action.GOAL_SUFFIX,Action.CANCEL_SUFFIX]:
					if path.endswith('/' + suffix):
						path = path[:-(len(suffix)+1)]
						if self.actions.has_key(path):
							action_name = path

							action = self.actions[action_name]
							dfile = definitions.describe_action_topic(action_name, suffix, action, full=full)

							if jsn:
								return response_200(start_response, str(dfile.tojson()), content_type='application/json')
							else:
								return response_200(start_response, dfile.tostring(suppress_formats=True), content_type='text/plain')
				return response_404(start_response)
		else:
			return response_404(start_response)

	def _handle_post(self, environ, start_response):
		name =  environ['PATH_INFO']

		if self.rest_prefix and name.startswith(self.rest_prefix):
			name = name[len(self.rest_prefix):]

		try:
			length = int(environ['CONTENT_LENGTH'])
			content_type = environ['CONTENT_TYPE'].split(';')[0].strip()
			use_ros = content_type == ROS_MSG_MIMETYPE

			if self.services.has_key(name):
				mode = 'service'
				service = self.services[name]
				input_msg_type = service.rostype_req
			elif self.topics.has_key(name):
				mode = 'topic'
				topic = self.topics[name]
				if not topic.allow_pub:
					rospy.logwarn(
						"RostfulServer::handle_post: %s does not allow publication", topic)
					return response_405(start_response)
				input_msg_type = topic.rostype
			else:
				for suffix in [Action.GOAL_SUFFIX,Action.CANCEL_SUFFIX]:
					action_name = name[:-(len(suffix)+1)]
					if name.endswith('/' + suffix) and self.actions.has_key(action_name):
						mode = 'action'
						action_mode = suffix
						action = self.actions[action_name]
						input_msg_type = action.get_msg_type(suffix)
						break
				else:
					return response_404(start_response)

			input_data = environ['wsgi.input'].read(length)

			input_msg = input_msg_type()
			#content_type = 'application/json'
			if use_ros:
				input_msg.deserialize(input_data)
			elif self._use_jwt:
				input_data = self.jwt_iface.decode(input_data)
				input_data.pop('_format', None)
				msgconv.populate_instance(input_data, input_msg)
				content_type = 'application/jwt'
			elif content_type == 'application/json':
				input_data = json.loads(input_data)
				input_data.pop('_format', None)
				msgconv.populate_instance(input_data, input_msg)
			elif content_type == 'application/xml':
				input_data = xmltodict.parse(input_data, postprocessor=postprocessor)
				input_data = json.dumps(input_data)
				input_data = json.loads(input_data)
				#print input_data
				#print input_msg

				# First check if the the key mission exist, otherwise it will raise an exception
				if input_data.get('mission') != None:

					# Then check if inside mission we have the key params due to we have to convert the dict to string
					if input_data["mission"].get('params') != None:
						input_data["mission"]["params"] = json.dumps(input_data["mission"]["params"])

				if input_data.get('goal') != None:
					if input_data["goal"].get('steps') != None:
						input_data["goal"]["steps"] = input_data["goal"]["steps"].replace("[", "").replace("]", "").replace("\"", "").replace(" ", "")
						input_data["goal"]["steps"] = input_data["goal"]["steps"].split(",")
						print(input_data["goal"]["steps"])

				input_data.pop('_format', None)
				msgconv.populate_instance(input_data, input_msg)

			ret_msg = None

			if len(input_data) > 0:
				if mode == 'service':
					rospy.logwarn('RostfulServer::_handle_post: calling service %s with msg : %s', service.name, input_msg)
					ret_msg = service.call(input_msg)
				elif mode == 'topic':
					rospy.logwarn('RostfulServer::_handle_post: publishing -> %s to topic %s', input_msg, topic.name)
					topic.publish(input_msg)
					return response_200(start_response, [], content_type=content_type)
				elif mode == 'action':
					rospy.logwarn('RostfulServer::_handle_post: publishing %s to action %s', input_msg, action.name)
					action.publish(action_mode, input_msg)
					return response_200(start_response, [], content_type=content_type)
			else:
				return response_405(start_response, 'error format', content_type='text/plain')

			if use_ros:
				content_type = ROS_MSG_MIMETYPE
				output_data = StringIO()
				ret_msg.serialize(output_data)
				output_data = output_data.getvalue()
			elif self._use_jwt:
				output_data = msgconv.extract_values(ret_msg)
				#output_data['_format'] = 'ros'
				output_data = self.jwt_iface.encode(output_data)
				content_type = 'application/jwt'
			# Check if the petition is in xml
			elif content_type == 'application/xml':
				output_data = msgconv.extract_values(ret_msg)
				#output_data['_format'] = 'ros'
				output_data = dicttoxml.dicttoxml(output_data, attr_type=False, root=False)
				print(output_data)
				content_type = 'application/xml'
			# By default uses JSON
			else:
				output_data = msgconv.extract_values(ret_msg)
				#output_data['_format'] = 'ros'
				output_data = json.dumps(output_data)
				content_type = 'application/json'

			return response_200(start_response, output_data, content_type=content_type)
		except Exception, e:
			print 'An exception occurred!', e
			return response_500(start_response, e)

	def rosSetup(self):
		'''
			Creates and inits ROS components
		'''
		if not rospy.is_shutdown():
			self.add_services(self.args_dict['services'])
			self.add_topics(self.args_dict['topics'])
			self.add_topics(self.args_dict['publishes'], allow_sub=False)
			self.add_topics(self.args_dict['subscribes'], allow_pub=False)
			self.add_actions(self.args.actions)

			self.t_ros_setup = threading.Timer(self.ros_setup_timer, self.rosSetup)
			self.t_ros_setup.start()

def postprocessor(path, key, value):
	try:
		return key + '', int(value)
	except (ValueError, TypeError):
		try:
			return key + '', float(value)
		except (ValueError, TypeError):
			return key, value


import argparse
from wsgiref.simple_server import make_server

def servermain():
	rospy.init_node('rostful_server', anonymous=True, disable_signals=True)

	args_dict = {}

	default_params = {
		'host'       : 'localhost',
		'port'       : 8080,
		'topics'     : [],
		'subscribes' : [],
		'publishes'  : [],
		'services'   : [],
		'types'	     : [],
		'rest-prefix': '/'
	}

	for key in default_params.keys():
		if rospy.has_param('~' + key):
			args_dict[key] = rospy.get_param('~' + key)
		else:
			args_dict[key] = default_params[key]
			rospy.logwarn("%s: '%s' param not defined, the default value is %s",
			              rospy.get_name(), key, default_params[key])

	parser = argparse.ArgumentParser()

	#parser.add_argument('--services', '--srv', nargs='+', help='Services to advertise')
	#parser.add_argument('--topics', nargs='+', help='Topics to both publish and subscribe')
	#parser.add_argument('--publishes', '--pub', nargs='+', help='Topics to publish via web services')
	#parser.add_argument('--subscribes', '--sub', nargs='+', help='Topics to allowing publishing to via web services')
	parser.add_argument('--actions', nargs='+', help='Actions to advertise')

	#parser.add_argument('--host', default='')
	#parser.add_argument('-p', '--port', type=int, default=8080)
	#parser.add_argument('--rest-prefix', default='/', help='The prefix path of the http request, usually starting and ending with a "/".')
	parser.add_argument('--jwt', action='store_true', default=False, help='This argument enables the use of JWT to guarantee a secure transmission')
	parser.add_argument('--jwt-key', default='ros', help='This arguments sets the key to encode/decode the data')
	parser.add_argument('--jwt-alg', default='HS256', help='This arguments sets the algorithm to encode/decode the data')

	args = parser.parse_args(rospy.myargv()[1:])
	init_delay = 0.0
	if rospy.search_param('init_delay'):
		init_delay = rospy.get_param('~init_delay')

	rospy.logwarn('%s: Delaying the start %d seconds', rospy.get_name(), init_delay)
	time.sleep(init_delay)

	try:
		server = RostfulServer(args, args_dict)

		httpd = make_server(args_dict['host'], args_dict['port'], server.wsgifunc())
		rospy.loginfo('%s: Started server on %s:%d', rospy.get_name(),
		              args_dict['host'], args_dict['port'])

		#Wait forever for incoming http requests
		httpd.serve_forever()

	except KeyboardInterrupt:
		rospy.loginfo('%s: Shutting down the server', rospy.get_name())
		httpd.socket.close()
		rospy.signal_shutdown('Closing')
