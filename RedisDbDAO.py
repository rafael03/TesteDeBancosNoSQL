import redis
import json
import uuid

import datetime

class RedisDbDAO:
	redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

	def insertObject(self, objeto):
		id_ = uuid.uuid4()
		self.redis_db.set(str(id_), json.dumps(objeto))
		print id_

	def getAllObjects(self):
		pass

	def getObjectsByCity(self, city):
		pass

	def updateCityFromObjects(self, old_city, new_city):
		pass

	def deleteObjectsFromCity(self, city):
		pass

	def getObjects(self):
		pass

	def getQuantityObjects(self):
		pass