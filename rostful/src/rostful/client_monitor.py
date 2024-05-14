import rospy
import time
import threading
import os
from datetime import datetime
import pytz

from rcomponent.rcomponent import RComponent
from robotnik_msgs.msg import State
from rosmon_msgs.srv import StartStop
from robot_local_control_msgs.msg import Status


class ClientMonitor(RComponent):

    def __init__(self, args):

        self._node_name = rospy.get_name()

        self.rosReadParams()

        self.timer_threshold = args['timer_threshold']
        self.rosmon_service = args['rosmon_service']
        self.rostful_client_node = args['rostful_client_node']
        self.monitored_topic = args['monitored_topic']

        self._real_freq = 0.0
        # Saves the state of the component
        self._state = State.INIT_STATE
        # Saves the previous state
        self._previous_state = State.INIT_STATE
        # flag to control the initialization of the component
        self._initialized = False
        # flag to control the initialization of ROS stuff
        self._ros_initialized = False
        # flag to control that the control loop is running
        self._running = False
        # Variable used to control the loop frequency
        self._time_sleep = 1.0 / self._desired_freq
        # State msg to publish
        self._msg_state = State()
        # Timer to publish state
        self._publish_state_timer = 1

        self.t_publish_state = threading.Timer(
            self._publish_state_timer, self.publishROSstate)

        self.last_message_time = rospy.Time.now()
        rospy.Timer(rospy.Duration(5), self.check_timer_callback)

    def rosSetup(self):
        '''
                Creates and inits ROS components
        '''
        if self._ros_initialized:
            return 0

        # Publishers
        self._state_publisher = rospy.Publisher('~state', State, queue_size=10)
        # Subscribers
        rospy.Subscriber(self.monitored_topic, Status,
                         self.monitored_topic_callback)
        # Service Clients
        self.restart_rosmon_node = rospy.ServiceProxy(
            self.rosmon_service, StartStop)

        self._ros_initialized = True

        self.publishROSstate()

        return 0

    def shutdown(self):
        '''
                Shutdowns device
                @return: 0 if it's performed successfully, -1 if there's any problem or the component is running
        '''
        if self._running or not self._initialized:
            return -1
        rospy.loginfo('%s::shutdown' % self._node_name)

        # Cancels current timers
        self._t_publish_state.cancel()

        self._state_publisher.unregister()

        self._initialized = False

        return 0

    def rosShutdown(self):
        '''
                Shutdows all ROS components
                @return: 0 if it's performed successfully, -1 if there's any problem or the component is running
        '''
        if self._running or not self._ros_initialized:
            return -1

        # Performs ROS topics & services shutdown
        self._state_publisher.unregister()

        self._ros_initialized = False

        return 0

    def restart_rosmon_node_client(self):
        try:
            self.restart_rosmon_node(self.rostful_client_node, "", 3)
        except rospy.ServiceException, e:
            rospy.logerr("Failed calling service %s: %s",
                         self.rosmon_service, e)

    def monitored_topic_callback(self, data):
        self.last_message_time = rospy.Time.now()

    def check_timer_callback(self, data):
        time_difference = rospy.Time.now() - self.last_message_time
        if time_difference.to_sec() > self.timer_threshold:
            rospy.logwarn("[%s] Communication lost for %f seconds. Calling %s to restart the rostful client",
                          datetime.fromtimestamp(rospy.Time.now().to_sec(), pytz.timezone('Europe/Madrid')), time_difference.to_sec(), self.rosmon_service)
            self.restart_rosmon_node_client()
            self.last_message_time = rospy.Time.now()

    # def readyState(self):
    # 	'''
    # 		Actions performed in ready state
    # 	'''


def clientmonitormain():
    rospy.init_node('rostful_client_monitor')

    _name = rospy.get_name().replace('/', '')

    arg_defaults = {
        'timer_threshold': '30',
        'rosmon_service': '/rosmon_rostful_rb2_x_client/start_stop',
        'rostful_client_node': 'rostful_client_rb2X',
        'monitored_topic': '/rb_x/rb2/robot_local_control/state'
    }

    args = {}

    for name in arg_defaults:
        try:
            if rospy.search_param(name):
                args[name] = rospy.get_param('~%s' % name)
            else:
                args[name] = arg_defaults[name]
        except rospy.ROSException, e:
            rospy.logerror('%s: %s' % (e, _name))

    node = ClientMonitor(args)
    rospy.loginfo('%s: starting' % (_name))
    node.start()
