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
		str_time = str(round(tempo,7)).replace(".",",")
		print "%s %s %d" % (operacao.ljust(22), str_time.ljust(15), quantidade)

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

	def insertGroupOfObjects(self, objetos):
		init_time = timeit.default_timer()
		self.classeDAO.insertGroupOfObjects(objetos)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'INSERIR POR BATCH', len(objetos))

	def getAllObjects(self):
		init_time = timeit.default_timer()
		documentos, quantity = self.classeDAO.getAllObjects()
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'RECEBER TUDO', quantity)

	def getByCivilStatus(self, civil_status):
		init_time = timeit.default_timer()
		objects, quantity = self.classeDAO.getByCivilStatus(civil_status)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'BUSCA SELETIVA', quantity)

	def updateCivilStatus(self, old_status, new_status):
		init_time = timeit.default_timer()
		objects, quantity = self.classeDAO.updateCivilStatus(old_status, new_status)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'ATUALIZAR SELETIVA', quantity)

	def deleteByCivilStatus(self, civil_status_to_delete):
		init_time = timeit.default_timer()
		objects, quantity = self.classeDAO.deleteByCivilStatus(civil_status_to_delete)
		self._mensagem(self.classeDAO.db_name, self._getTimeDifference(init_time), 'REMOÇÃO SELETIVA', quantity)

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
		quantity_of_values = self.classeDAO.getQuantityOfValuesOnDB()
		return quantity_of_values

	def deleteAllObjects(self):
		self.classeDAO.deleteAllObjects()

	def __init__(self):
		if sys.argv[1] == 'cassandra' or sys.argv[1] == '-c':
			print 'cassandra'
			self.banco = 'Cassandra'
			self.classeDAO = CassandraDAO()
		elif sys.argv[1] == 'mongodb' or sys.argv[1] == '-m':
			self.classeDAO = MongodbDAO()
			self.banco = 'MongoDB'
		elif sys.argv[1] == 'neo4j' or sys.argv[1] == '-n':
			self.banco = 'Neo4j'
			self.classeDAO = Neo4jDAO()
		elif sys.argv[1] == 'redis' or sys.argv[1] == '-r':
			print 'redis'
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
		elif sys.argv[2] == '1m':
			self.carga = 'carga/1milhao.txt'
		else:
			self.carga = 'carga/5objetos.txt'

teste = Teste()
init_time = timeit.default_timer()
objetos = teste.getObjectsFromFile(teste.carga)
final_time = timeit.default_timer()
diff_time = final_time - init_time
print "Tempo para carregar toda carga em memória: ", diff_time

print "%s %s %s" % ("OPERAÇÃO".ljust(22), "SEGUNDOS".ljust(15), "QUANTIDADE DE DOC")
teste.insertObjecbyObject(objetos)
teste.insertGroupOfObjects(objetos)
objetos_inseridos = teste.getQuantityOfValuesOnDB()
teste.getAllObjects()
teste.getByCivilStatus('Solteiro')
teste.updateCivilStatus('Solteiro', 'Alterado')
teste.deleteByCivilStatus('Alterado')
teste.deleteAllObjects()
objetos_no_banco = teste.getQuantityOfValuesOnDB()

print "Foram inseridos um total de %d documentos" % (objetos_inseridos)
print "Quantidade de documentos existente no banco", objetos_no_banco