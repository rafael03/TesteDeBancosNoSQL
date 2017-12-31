#!/usr/bin/env python
# coding=utf-8

import redis
import uuid
import ast

class RedisDAO:
	db_name = 'Redis'
	redis_db = redis.StrictRedis(host="localhost", port=6379, db=1)

	def insertObject(self, objeto):
		id_ = uuid.uuid4().hex
		objeto['id_'] = id_
		self.redis_db.set(str(id_), objeto)

	def insertGroupOfObjects(self):
		pass

	def getAllObjects(self):
		keys = self.redis_db.keys()
		values = self.redis_db.mget(keys)
		size = len(values)
		return values, size

	def getQuantityOfValuesOnDB(self):
		keys = self.redis_db.keys()
		return len(keys)

	def getByCivilStatus(self, civil_status):
		object_dict = {}
		objects, quantity = self.getAllObjects()
		for objeto in objects:
			obj = ast.literal_eval(objeto)
			if obj['ESTADO_CIVIL'] == civil_status:
				object_dict[obj['id_']] = obj
		return object_dict, len(object_dict)

	def updateCivilStatus(self, old_status, new_status):
		updated_objects = {}
		list_id, quantity = self.getByCivilStatus(old_status)
		for id_objeto in list_id:
			object_to_update = self.redis_db.get(id_objeto)
			object_dict = ast.literal_eval(object_to_update)
			object_dict['ESTADO_CIVIL'] = new_status
			self.redis_db.set(id_objeto, object_dict)
			updated_objects[id_objeto] = object_dict
		return updated_objects, len(updated_objects)


	def deleteByCivilStatus(self, civil_status_to_delete):
		list_id, quantity = self.getByCivilStatus(civil_status_to_delete)
		self.redis_db.delete(list_id)
		return list_id, len(list_id)

	def deleteAllObjects(self):
		self.redis_db.flushall()