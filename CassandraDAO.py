#!/usr/bin/env python
# coding=utf-8

from cassandra.cluster import Cluster
from CassandraModel import CassandraModel
from cassandra.cqlengine.connection import setup
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.management import sync_table
from cassandra.query import BatchStatement
import uuid
class CassandraDAO:
	try:
		db_name = 'Cassandra'
		cassandra = CassandraModel()
		setup(["127.0.0.1"], "test_keyspace")
		inscritos = NamedTable("test_keyspace", "inscritos")
	except Exception as e:
		pass

	def insertObject(self, objeto):
		id_ = uuid.uuid4().hex
		self.cassandra.session.execute(
			"""
				INSERT INTO inscritos (id, ANO_NASCIMENTO, PESO, ALTURA, CABECA, CALCADO, CINTURA, RELIGIAO, MUN_NASCIMENTO, UF_NASCIMENTO, PAIS_NASCIMENTO, ESTADO_CIVIL, SEXO, ESCOLARIDADE, VINCULACAO_ANO, DISPENSA, ZONA_RESIDENCIAL, MUN_RESIDENCIA, UF_RESIDENCIA, PAIS_RESIDENCIA, JSM, MUN_JSM, UF_JSM)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
			""",
			(id_, objeto['ANO_NASCIMENTO'], objeto['PESO'], objeto['ALTURA'], objeto['CABECA'], objeto['CALCADO'], objeto['CINTURA'], objeto['RELIGIAO'], objeto['MUN_NASCIMENTO'], objeto['UF_NASCIMENTO'], objeto['PAIS_NASCIMENTO'], objeto['ESTADO_CIVIL'], objeto['SEXO'], objeto['ESCOLARIDADE'], objeto['VINCULACAO_ANO'], objeto['DISPENSA'], objeto['ZONA_RESIDENCIAL'], objeto['MUN_RESIDENCIA'], objeto['UF_RESIDENCIA'], objeto['PAIS_RESIDENCIA'], objeto['JSM'], objeto['MUN_JSM'], objeto['UF_JSM'])
		)

	def insertGroupOfObjects(self, objetos):
		insert_user = self.cassandra.session.prepare("INSERT INTO inscritos (id, ANO_NASCIMENTO, PESO, ALTURA, CABECA, CALCADO, CINTURA, RELIGIAO, MUN_NASCIMENTO, UF_NASCIMENTO, PAIS_NASCIMENTO, ESTADO_CIVIL, SEXO, ESCOLARIDADE, VINCULACAO_ANO, DISPENSA, ZONA_RESIDENCIAL, MUN_RESIDENCIA, UF_RESIDENCIA, PAIS_RESIDENCIA, JSM, MUN_JSM, UF_JSM)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		objetos_parts = [objetos[i:i+50] for i in range(0, len(objetos), 50)]
		for parts_objects in objetos_parts:
			batch = BatchStatement()
			for objeto in parts_objects:
				id_ = uuid.uuid4().hex
				batch.add(insert_user, (id_, objeto['ANO_NASCIMENTO'], objeto['PESO'], objeto['ALTURA'], objeto['CABECA'], objeto['CALCADO'], objeto['CINTURA'], objeto['RELIGIAO'], objeto['MUN_NASCIMENTO'], objeto['UF_NASCIMENTO'], objeto['PAIS_NASCIMENTO'], objeto['ESTADO_CIVIL'], objeto['SEXO'], objeto['ESCOLARIDADE'], objeto['VINCULACAO_ANO'], objeto['DISPENSA'], objeto['ZONA_RESIDENCIAL'], objeto['MUN_RESIDENCIA'], objeto['UF_RESIDENCIA'], objeto['PAIS_RESIDENCIA'], objeto['JSM'], objeto['MUN_JSM'], objeto['UF_JSM']))
			self.cassandra.session.execute(batch)

	def getAllObjects(self):
		all_objects = self.inscritos.objects.all()
		return all_objects, all_objects.count()

	def getQuantityOfValuesOnDB(self):
		quantity = self.inscritos.objects.count()
		return quantity

	def getByCivilStatus(self, civil_status):
		query = self.inscritos.objects.filter(estado_civil=civil_status)
		result = query.allow_filtering()
		return result, result.count()

	def updateCivilStatus(self, old_status, new_status):
		list_to_update, quantity = self.getByCivilStatus(old_status)
		enrolled = {}
		for insc in list_to_update:
			enrolled[insc['id']] = insc
			self.cassandra.session.execute("UPDATE inscritos SET estado_civil = %s WHERE id = %s;", (new_status, insc['id']))
		return enrolled, len(enrolled)

	def deleteByCivilStatus(self, civil_status_to_delete):
		list_to_delete, quantity = self.getByCivilStatus(civil_status_to_delete)
		deleted = {}
		query =  "DELETE FROM inscritos WHERE id=%s;"
		for insc in list_to_delete:
			deleted[insc['id']] = insc
			self.cassandra.session.execute(query, [insc['id']])
		return deleted, len(deleted)

	def deleteAllObjects(self):
		query = "TRUNCATE inscritos;"
		self.cassandra.session.execute(query)