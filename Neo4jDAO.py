#!/usr/bin/env python
# coding=utf-8
from neo4j.v1 import GraphDatabase
import uuid
from py2neo import Graph, Node, Relationship

# graph = Graph("http://neo4j:brasil@localhost:7474/db/data")



class Neo4jDAO:
	db_name = 'Neo4j'
	# uri = "bolt://localhost:7687"
	# #O segundo parâmetro, referente a senha deve ser configurado quando realizado o primeiro login no Neo4j
	# driver = GraphDatabase.driver(uri, auth=("neo4j", "brasil"), MaxConnectionPoolSize=1000)
	contador = 1
	graph = Graph("http://neo4j:brasil@localhost:7474/db/data")

	def insertObject(self, objetos):
		id_ = uuid.uuid4().hex
		uri = "bolt://localhost:7687"
		#O segundo parâmetro, referente a senha deve ser configurado quando realizado o primeiro login no Neo4j
		driver = GraphDatabase.driver(uri, auth=("neo4j", "brasil"), MaxConnectionPoolSize=1000)
		statement = "CREATE (a:Inscritos {ID:{id}, ANO_NASCIMENTO:{ano_nascimento}, PESO:{peso}, ALTURA:{altura}, CABECA:{cabeca}, CALCADO:{calcado}, CINTURA:{cintura}, RELIGIAO:{religiao}, MUN_NASCIMENTO:{mun_nascimento}, UF_NASCIMENTO:{uf_nascimento}, PAIS_NASCIMENTO:{pais_nascimento}, ESTADO_CIVIL:{estado_civil}, SEXO:{sexo}, ESCOLARIDADE:{escolaridade}, VINCULACAO_ANO:{vinculacao_ano}, DISPENSA:{dispensa}, ZONA_RESIDENCIAL:{zona_residencial}, MUN_RESIDENCIA:{mun_residencia}, UF_RESIDENCIA:{uf_residencia}, PAIS_RESIDENCIA:{pais_residencia}, JSM:{jsm}, MUN_JSM:{mun_jsm}, UF_JSM:{uf_jsm}})"
		driver.session().run(statement, {"id": id_, "ano_nascimento": objetos["ANO_NASCIMENTO"], "peso": objetos["PESO"], "altura": objetos["ALTURA"], "cabeca": objetos["CABECA"], "calcado": objetos["CALCADO"], "cintura": objetos["CINTURA"], "religiao": objetos["RELIGIAO"], "mun_nascimento": objetos["MUN_NASCIMENTO"], "uf_nascimento": objetos["UF_NASCIMENTO"], "pais_nascimento": objetos["PAIS_NASCIMENTO"], "estado_civil": objetos["ESTADO_CIVIL"], "sexo": objetos["SEXO"], "escolaridade": objetos["ESCOLARIDADE"], "vinculacao_ano": objetos["VINCULACAO_ANO"], "dispensa": objetos["DISPENSA"], "zona_residencial": objetos["ZONA_RESIDENCIAL"], "mun_residencia": objetos["MUN_RESIDENCIA"], "uf_residencia": objetos["UF_RESIDENCIA"], "pais_residencia": objetos["PAIS_RESIDENCIA"], "jsm": objetos["JSM"], "mun_jsm": objetos["MUN_JSM"], "uf_jsm": objetos["UF_JSM"]})
		driver.close()
		self.contador+=1
		return {}, 0
	

	def inserGroupOfObjects(self, all_objetos):
		statement = "CREATE (a:Inscritos {ID:{id}, ANO_NASCIMENTO:{ano_nascimento}, PESO:{peso}, ALTURA:{altura}, CABECA:{cabeca}, CALCADO:{calcado}, CINTURA:{cintura}, RELIGIAO:{religiao}, MUN_NASCIMENTO:{mun_nascimento}, UF_NASCIMENTO:{uf_nascimento}, PAIS_NASCIMENTO:{pais_nascimento}, ESTADO_CIVIL:{estado_civil}, SEXO:{sexo}, ESCOLARIDADE:{escolaridade}, VINCULACAO_ANO:{vinculacao_ano}, DISPENSA:{dispensa}, ZONA_RESIDENCIAL:{zona_residencial}, MUN_RESIDENCIA:{mun_residencia}, UF_RESIDENCIA:{uf_residencia}, PAIS_RESIDENCIA:{pais_residencia}, JSM:{jsm}, MUN_JSM:{mun_jsm}, UF_JSM:{uf_jsm}})"
		objetos_parts = [all_objetos[i:i+95] for i in range(0, len(all_objetos), 95)]
		for o in objetos_parts:
			tx = self.graph.begin()
			for objetos in o:
				id_ = uuid.uuid4().hex
				tx.append(statement, {"id": id_, "ano_nascimento": objetos["ANO_NASCIMENTO"], "peso": objetos["PESO"], "altura": objetos["ALTURA"], "cabeca": objetos["CABECA"], "calcado": objetos["CALCADO"], "cintura": objetos["CINTURA"], "religiao": objetos["RELIGIAO"], "mun_nascimento": objetos["MUN_NASCIMENTO"], "uf_nascimento": objetos["UF_NASCIMENTO"], "pais_nascimento": objetos["PAIS_NASCIMENTO"], "estado_civil": objetos["ESTADO_CIVIL"], "sexo": objetos["SEXO"], "escolaridade": objetos["ESCOLARIDADE"], "vinculacao_ano": objetos["VINCULACAO_ANO"], "dispensa": objetos["DISPENSA"], "zona_residencial": objetos["ZONA_RESIDENCIAL"], "mun_residencia": objetos["MUN_RESIDENCIA"], "uf_residencia": objetos["UF_RESIDENCIA"], "pais_residencia": objetos["PAIS_RESIDENCIA"], "jsm": objetos["JSM"], "mun_jsm": objetos["MUN_JSM"], "uf_jsm": objetos["UF_JSM"]})
			tx.commit()
			tx.finish
		return {}, len(all_objetos)

	def getAllObjects(self):
		pass

	def getQuantityOfValuesOnDB(self):
		pass

	def getByCivilStatus(self, civil_status):
		# MATCH (p:Person { name:"Keanu Reeves" })
		# RETURN p

		# MATCH (m:Movie)
		# WHERE m.title = "The Matrix"
		# RETURN m
		pass

	def updateCivilStatus(self, old_status, new_status):
		pass

	def deleteByCivilStatus(self, civil_status_to_delete):
		pass

	def deleteAllObjects(self):
		pass