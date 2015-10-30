# -*- coding: utf-8 -*-

import rethinkdb as r
import threading

TABLE_1 = 'id_store_1'
TABLE_2 = 'id_store_2'
DATABASE = 'id_archive'

STATES = {'safe': 'SAFE', 'caution': 'CAUTION', 'danger': 'DANGER'}

class Supplier:

	def __init__(self, sample_size=20, store_size=5000, caution_threshold=3000):
		self.connection = None

		self.db = None
		self.table = None

		self.tables = set([TABLE_1, TABLE_2])
		self.master = set([TABLE_1])
		self.slave = self.tables.difference(self.master)

		self.count = 0
		self.lock = threading.Lock()

		self.sample_size = sample_size
		self.store_size = store_size
		self.caution_threshold = caution_threshold

	# connect the instance to the database
	def connect(self):
		try:
			self.connection = r.connect(host='localhost', port=28015)

			# use the 'id_archive' database with this connection
			r.db_create(DATABASE).run(self.connection)
			self.connection.use(DATABASE)

			# store reference to current 'table' and 'database'
			self.db = r.db(DATABASE)
			self.table = self.db.table(next(iter(self.master)))
		except Exception, e:
			raise e

	# create all the required tables
	def create_tables(self):
		try:
			for table in self.tables:
				r.table_create(table).run(self.connection)
		except Exception, e:
			raise e

	# query a set of 'id' documents and remove them
	def query_id(self):
		try:
			# using GIL approach
			with (self.lock):
				# collect documents of given sample size
				response = list(self.table.order_by('id').limit(self.sample_size).run(self.connection))

				# remove them from the database, makes use of the RethinkDB UUID keys
				self.table.order_by('id').limit(self.sample_size).delete().run(self.connection)

				# update the global 'counter' resource
				self.count += len(response)

				return response
		except Exception, e:
			raise e

	# return the state of 'master' storage table
	def master_state(self):
		count = self.table.count().run(self.connection)

		if (count < self.caution_threshold):
			return STATES['safe']
		elif (self.caution_threshold <= count < self.store_size):
			return STATES['caution']
		else:
			return STATES['danger']
