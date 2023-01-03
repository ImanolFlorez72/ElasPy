from elasticsearch import Elasticsearch


class Connect():
	
	def __init__(self):
		self.client = Elasticsearch(
        hosts="http://localhost:9200",
        http_auth=("elastic", "m4Av97R6ITaLlNfxvNoF"),
        verify_certs=False,
        use_ssl=False,
        http_compress=True,
        )
