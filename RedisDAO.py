#!/usr/bin/env python
# coding=utf-8

import redis
import json
import uuid

import datetime

class RedisDAO:
	db_name = 'Redis'
	redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

	def insertObject(self, objeto):
		# id_ = uuid.uuid4()
		# self.redis_db.set(str(id_), json.dumps(objeto))
		# print id_
		pass

	def insertObject(self, objetos):
		pass

	def insertGroupOfObjects(self):
		pass

	def getAllObjects(self):
		pass

	def getQuantityOfValuesOnDB(self):
		pass

	def getByCivilStatus(self, civil_status):
		pass

	def updateCivilStatus(self, old_status, new_status):
		pass

	def deleteByCivilStatus(self, civil_status_to_delete):
		pass

	def deleteAllObjects(self):
		pass