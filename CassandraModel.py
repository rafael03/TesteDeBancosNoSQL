from cassandra.cluster import Cluster

class CassandraModel:
	try:
		cluster = Cluster()
		session = cluster.connect()

		# Cria a KEYSPACE
		session.execute("CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
		session.set_keyspace('test_keyspace')

		# Cria a tabela inscritos
		session.execute("CREATE TABLE IF NOT EXISTS inscritos (id text PRIMARY KEY, ANO_NASCIMENTO text, PESO text, ALTURA text, CABECA text, CALCADO text, CINTURA text, RELIGIAO text, MUN_NASCIMENTO text, UF_NASCIMENTO text, PAIS_NASCIMENTO text, ESTADO_CIVIL text, SEXO text, ESCOLARIDADE text, VINCULACAO_ANO text, DISPENSA text, ZONA_RESIDENCIAL text, MUN_RESIDENCIA text, UF_RESIDENCIA text, PAIS_RESIDENCIA text, JSM text, MUN_JSM text, UF_JSM text);")
	except Exception as e:
		pass