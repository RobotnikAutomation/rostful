from __future__ import absolute_import

import rospy
import jwt

class JwtInterface:
	
	def __init__(self, key='ros', algorithm='HS256'):
		
		self.key = key
		self.algorithm = algorithm
	
	'''
		Method to encode a json string by using JWT library
		@param payload as a json string
		@returns the encoded string 
	'''
	def encode(self, payload):
		
		encoded = ''
		
		try:
			encoded = jwt.encode(payload, self.key, algorithm=self.algorithm)
		except jwt.exceptions.InvalidKeyError as e:
			rospy.logerr('JwtInterface::encode: %s. payload %s', e, str(payload))
		except jwt.exceptions.InvalidAlgorithmError as e:
			rospy.logerr('JwtInterface::encode: %s. payload %s',e, str(payload))
		except TypeError as e:
			rospy.logerr('JwtInterface::encode: %s. payload %s',e, str(payload))
			
		return encoded
		
	'''
		Method to decode a jwt string to json by using JWT library
		@param msg as a string
		@returns the decoded json dictionary 
	'''
	def decode(self, msg):
		
		decoded = {}
		
		try:
			decoded = jwt.decode(msg, self.key, algorithm=self.algorithm)
		except jwt.exceptions.InvalidTokenError as e:
			rospy.logerr('JwtInterface::decode:InvalidTokenError: %s. key = %s, algorithm = %s, msg = %s',e, self.key, self.algorithm, msg)
		except jwt.exceptions.DecodeError as e:
			rospy.logerr('JwtInterface::decode:DecodeError: %s. key = %s, algorithm = %s, msg = %s',e, self.key, self.algorithm, msg)
		except TypeError as e:
			rospy.logerr('JwtInterface::decode:TypeError: %s. key = %s, algorithm = %s, msg = %s',e, self.key, self.algorithm, msg)
			
		return decoded
