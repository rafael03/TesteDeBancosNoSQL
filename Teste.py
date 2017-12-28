#!/usr/bin/env python
# coding=utf-8

import timeit
from leitor import Leitor
from MongodbDAO import MongodbDAO
from RedisDAO import RedisDAO
from CassandraDAO import CassandraDAO
from Neo4jDAO import Neo4jDAO
import sys

class Teste:
	'''
	Classe responsável por executar os testes e realizar a medição de tempo
	parâmetros para escolha do banco

	cassandra  		-c 		Realiza testes no banco de dados Cassandra
	mongodb   		-m  	Realiza testes no banco de dados MongoDB
	neo4j       	-n  	Realiza testes no banco de dados Neo3j
	redis       	-r   	Realiza testes no banco de dados Redis

	Exemplo de uso:
	python Teste.py mongodb
			ou
	python Teste.py -m
	'''

	def _mensagem(self, banco, tempo, operacao, quantidade):
		print "O %s demorou %f segundos para %s %d documentos " % (banco, tempo, operacao, quantidade)

	def _getTimeDifference(self, init_time):
		final_time = timeit.default_timer()
		return final_time - init_time

	def getObjectsFromFile(self, arquivo):
		leitor = Leitor()
		objetos = leitor.retorna_lista_de_objetos(arquivo)
		return objetos
	
	def insertObjecbyObject(self, objetos):
		init_time = timeit.default_timer()
		for objeto in objetos:
			self.classeDAO.insertObject(objeto)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'INSERIR', len(objetos))

	def inserGroupOfObjects(self, objetos):
		pass

	def getAllObjects(self):
		init_time = timeit.default_timer()
		documentos = self.classeDAO.getAllObjects()
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'RECEBER TODOS', documentos.count())

	def getByCivilStatus(self, civil_status):
		init_time = timeit.default_timer()
		quantidade = self.classeDAO.getByCivilStatus(civil_status)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'BUSCAR por estado civil de solteiro, e foram encontrados ', quantidade)

	def updateCivilStatus(self, old_status, new_status):
		init_time = timeit.default_timer()
		quantidade = self.classeDAO.updateCivilStatus(old_status, new_status)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'ATUALIZAR O ESTADO CIVIL DE ', quantidade)

	def deleteByCivilStatus(self, civil_status_to_delete):
		init_time = timeit.default_timer()
		quantidade = self.classeDAO.deleteByCivilStatus(civil_status_to_delete)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'DELETAR', quantidade)

	def searchObjectsByCity(self, city):
		init_time = timeit.default_timer()
		print 'O %s demorou %f segundos para buscar todos objetos da cidade de %s'\
		% (self.banco, self._getTimeDifference(init_time), city)

	def updateObjectsCity(self, old_city, new_city):
		init_time = timeit.default_timer()
		pass

	def deleteObjectsByCity(self, city):
		init_time = timeit.default_timer()
		pass

	def getQuantityOfValuesOnDB(self):
		init_time = timeit.default_timer()
		self.classeDAO.getQuantityOfValuesOnDB()

	def deleteAllObjects(self):
		self.classeDAO.deleteAllObjects()

	def __init__(self):
		if sys.argv[1] == 'cassandra' or sys.argv[1] == '-c':
			self.banco = 'Cassandra'
			self.classeDAO = CassandraAO()
		elif sys.argv[1] == 'mongodb' or sys.argv[1] == '-m':
			self.classeDAO = MongodbDAO()
			self.banco = 'MongoDB'
		elif sys.argv[1] == 'neo4j' or sys.argv[1] == '-n':
			self.banco = 'Neo4j'
			self.classeDAO = Neo4jDAO()
		elif sys.argv[1] == 'redis' or sys.argv[1] == '-r':
			self.classeDAO = RedisDAO()
			self.banco = 'Redis'
		else:
			self.classeDAO = MongodbDAO()
			self.banco = 'MongoDB'

		if sys.argv[2] == '1':
			self.carga = 'carga/1mil.txt'
		elif sys.argv[2] == '10':
			self.carga = 'carga/10mil.txt'
		elif str(sys.argv[2]) == '50':
			self.carga = 'carga/50mil.txt'
		elif sys.argv[2] == '100':
			self.carga = 'carga/100mil.txt'
		else:
			self.carga = 'carga/1mil.txt'



teste = Teste()
print teste.carga
objetos = teste.getObjectsFromFile(teste.carga)
teste.insertObjecbyObject(objetos)
teste.getQuantityOfValuesOnDB()
teste.getAllObjects()
teste.getByCivilStatus('Solteiro')
teste.updateCivilStatus('Solteiro', 'Alterado')
teste.deleteByCivilStatus('Alterado')
teste.deleteAllObjects()