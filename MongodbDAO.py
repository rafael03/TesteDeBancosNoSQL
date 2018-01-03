#!/usr/bin/env python
# coding=utf-8

from pymongo import MongoClient

class MongodbDAO:
	db_name = 'MongoDB'

	cliente = MongoClient('mongodb://localhost:27017')
	db = cliente['database']
	recrutas_collections = db['recrutas']

	def insertObject(self, objeto):
		id_ = self.recrutas_collections.insert_one(objeto).inserted_id

	def insertGroupOfObjects(self, objetos):
		objetos_parts = [objetos[i:i+100] for i in range(0, len(objetos), 100)]
		x = {}
		for objects in objetos_parts:
			result = self.recrutas_collections.insert_many(objects, ordered=False)


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