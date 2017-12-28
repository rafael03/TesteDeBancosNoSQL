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

	def transforma_cada_linha_em_um_objeto(self, linhas):
		lista = []
		for linha in linhas:
			linha = linha.replace('\"','')
			objeto = linha.split(',')

			lista.append({"ANO_NASCIMENTO": objeto[0],
							"PESO": objeto[1],
							"ALTURA": objeto[2],
							"CABECA": objeto[3],
							"CALCADO": objeto[4],
							"CINTURA": objeto[5],
							"RELIGIAO": objeto[6],
							"MUN_NASCIMENTO": objeto[7],
							"UF_NASCIMENTO": objeto[8],
							"PAIS_NASCIMENTO": objeto[9],
							"ESTADO_CIVIL": objeto[10],
							"SEXO": objeto[11],
							"ESCOLARIDADE": objeto[12],
							"VINCULACAO_ANO": objeto[13],
							"DISPENSA": objeto[14],
							"ZONA_RESIDENCIAL": objeto[15],
							"MUN_RESIDENCIA": objeto[16],
							"UF_RESIDENCIA": objeto[17],
							"PAIS_RESIDENCIA": objeto[18],
							"JSM": objeto[19],
							"MUN_JSM": objeto[20],
							"UF_JSM": objeto[21]})
		return lista

	def retorna_lista_de_objetos(self, arquivo):
		arquivo = self.carrega_arquivo_em_memoria(arquivo)
		linhas = self.quebra_arquivo_por_linhas(arquivo)
		objetos = self.transforma_cada_linha_em_um_objeto(linhas)
		return objetos