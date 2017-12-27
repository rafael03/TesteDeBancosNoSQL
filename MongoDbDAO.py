from pymongo import MongoClient
import datetime
import pprint

class MongoDbDAO:

	cliente = MongoClient('mongodb://localhost:27017')
	db = cliente['database']
	recrutas_collections = db['recrutas']

	def insertObject(self, objetos):
		objetos = {'nome':'rafael uu', 'idade': 27}
		id_ = self.recrutas_collections.insert_one(objetos).inserted_id
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
		for post in self.recrutas_collections.find():
			pprint.pprint(post)

	def getQuantityObjects(self):
		quantidade = self.db['recrutas'].count()
		print 'qtd de objetos salvos', quantidade
		return quantidade