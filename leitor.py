#!/usr/bin/env python
# coding=utf-8
class Leitor:
	def carrega_arquivo_em_memoria(self, arquivo):
		'''
		@arquivo = arquivo que sera lido
		'''

		arquivo = open(arquivo, 'r')
		return arquivo

	def quebra_arquivo_por_linhas(self, arquivo):
		linhas = arquivo.readlines()
		return linhas

	# def separa_linhas_por_virgula(self, linhas):
	# 	objeto = linhas[0].split(',')
	# 	return objeto

	def transforma_cada_linha_em_um_objeto(self, linhas):
		lista = []
		for linha in linhas:
			objeto = linha.split(',')
			lista.append({"nome":objeto[0],
						 "idade": objeto[1]})
		return lista

	def retorna_lista_de_objetos(self, arquivo):
		arquivo = self.carrega_arquivo_em_memoria(arquivo)
		linhas = self.quebra_arquivo_por_linhas(arquivo)
		objetos = self.transforma_cada_linha_em_um_objeto(linhas)
		return objetos
