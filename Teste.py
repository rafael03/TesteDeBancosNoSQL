#!/usr/bin/env python
# coding=utf-8

import timeit
from leitor import Leitor
from MongoDbDAO import MongoDbDAO
from RedisDbDAO import RedisDbDAO

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

	def getObjectsFromFile(self, arquivo):
		leitor = Leitor()
		objetos = leitor.retorna_lista_de_objetos(arquivo)
		return objetos
	
	def insertObjecbyObject(self, objetos):

		init_time = timeit.default_timer()
		for objeto in objetos:
			self.classeDAO.insertObject(objeto)
		print 'O %s levou %f segundos para inserir %d documentos' % (self.banco, self._getTimeDifference(init_time), len(objetos))

	def getAllObjects(self):
		init_time = timeit.default_timer()
		print 'O %s demorou %f segundos para buscar todos objetos' % (self.banco, self._getTimeDifference(init_time))

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
		self.classeDAO.getQuantityObjects()

	def _getTimeDifference(self, init_time):
		final_time = timeit.default_timer()
		return final_time - init_time

	def __init__(self):
		if sys.argv[1] == 'cassandra' or sys.argv[1] == '-c':
			self.banco = 'Cassandra'
		if sys.argv[1] == 'mongodb' or sys.argv[1] == '-m':
			self.classeDAO = MongoDbDAO()
			self.banco = 'MongoDB'
		if sys.argv[1] == 'neo4j' or sys.argv[1] == '-n':
			self.banco = 'Neo4j'
		if sys.argv[1] == 'redis' or sys.argv[1] == '-r':
			self.classeDAO = RedisDbDAO()
			self.banco = 'Redis'
		else:
			self.classeDAO = MongoDbDAO()
			self.banco = 'MongoDB'


teste = Teste()
objetos = teste.getObjectsFromFile('milhao.txt')
teste.insertObjecbyObject(objetos)
teste.getQuantityOfValuesOnDB()
teste.searchObjectsByCity('brasilia')