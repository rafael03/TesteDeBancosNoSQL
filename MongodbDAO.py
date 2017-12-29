#!/usr/bin/env python
# coding=utf-8

from pymongo import MongoClient
import pprint

class MongodbDAO:
	db_name = 'MongoDB'

	cliente = MongoClient('mongodb://localhost:27017')
	db = cliente['database']
	recrutas_collections = db['recrutas']

	def insertObject(self, objetos):
		id_ = self.recrutas_collections.insert_one(objetos).inserted_id

	def insertGroupOfObjects(self):
		pass

	def getAllObjects(self):
		collections = self.recrutas_collections.find()
		return collections, collections.count()

	def getQuantityOfValuesOnDB(self):
		quantidade = self.db['recrutas'].count()
		return quantidade

	def getByCivilStatus(self, civil_status):
		objects = self.db['recrutas'].find({"ESTADO_CIVIL": civil_status})
		return objects, objects.count()

	def updateCivilStatus(self, old_status, new_status):
		collections = self.db['recrutas'].update_many(
									    {"ESTADO_CIVIL": old_status},
									    {"$set": {"ESTADO_CIVIL": new_status},
									     "$currentDate": {"lastModified": True}})

		return collections, collections.modified_count

	def deleteByCivilStatus(self, civil_status_to_delete):
		collections = self.db['recrutas'].delete_many({"ESTADO_CIVIL": civil_status_to_delete})
		return collections, collections.deleted_count

	def deleteAllObjects(self):
		self.db.drop_collection('recrutas')