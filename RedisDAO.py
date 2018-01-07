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

	def insertGroupOfObjects(self, objetos):
		values = []
		objetos_parts = [objetos[i:i+100] for i in range(0, len(objetos), 100)]
		for obj in objetos_parts[:]:
			pipe = self.redis_db.pipeline()
			for objeto in obj:
				id_ = uuid.uuid4().hex
				pipe.set(str(id_), objeto)
			values = pipe.execute()
			objetos_parts.remove(obj)

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
		objetos_parts = [objects[i:i+200] for i in range(0, len(objects), 200)]
		del objects

		for parts in objetos_parts[:]:
			for objeto in parts:
				obj = ast.literal_eval(objeto)
				if obj['ESTADO_CIVIL'] == civil_status:
					object_dict[obj['id_']] = obj
			objetos_parts.remove(parts)
		return object_dict, len(object_dict)

	def updateCivilStatus(self, old_status, new_status):
		updated_objects = {}
		ids_list = []
		objects_dict, quantity = self.getByCivilStatus(old_status)
		for ids in objects_dict:
			ids_list.append(ids)
		del objects_dict

		ids_in_parts = [ids_list[i:i+200] for i in range(0, len(ids_list), 200)]
		del ids_list

		for parts_from_ids in ids_in_parts[:]:
			for id_objeto in parts_from_ids:
				object_to_update = self.redis_db.get(id_objeto)
				object_dict = ast.literal_eval(object_to_update)
				object_dict['ESTADO_CIVIL'] = new_status
				self.redis_db.set(id_objeto, object_dict)
				updated_objects[id_objeto] = object_dict
			ids_in_parts.remove(parts_from_ids)
		return updated_objects, len(updated_objects)


	def deleteByCivilStatus(self, civil_status_to_delete):
		ids_list = []
		objects_dict, quantity = self.getByCivilStatus(civil_status_to_delete)
		for ids in objects_dict:
			ids_list.append(ids)
		del objects_dict

		ids_in_parts = [ids_list[i:i+200] for i in range(0, len(ids_list), 200)]
		del ids_list

		for parts_from_ids in ids_in_parts[:]:
			for id_objeto in parts_from_ids:
				self.redis_db.delete(id_objeto)
			ids_in_parts.remove(parts_from_ids)
		return {}, quantity

	def deleteAllObjects(self):
		self.redis_db.flushall()