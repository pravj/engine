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
		self.lock = threading.RLock()

		self.sample_size = sample_size
		self.store_size = store_size
		self.caution_threshold = caution_threshold

		self.caution_aware = False
		self.slave_ready = False

		self.connect()

	# connect the instance to the database
	def connect(self):
		try:
			self.connection = r.connect(host='localhost', port=28015)

			# create the 'id_archive' database if not present
			db_list = r.db_list().run(self.connection)
			if (DATABASE not in db_list):
				r.db_create(DATABASE).run(self.connection)

			# use the 'id_archive' database with this connection
			self.connection.use(DATABASE)

			# store reference to current 'table' and 'database'
			self.db = r.db(DATABASE)
			self.table = self.db.table(next(iter(self.master)))

			# create 'storage' tables
			self.create_tables()
		except Exception, e:
			raise e

	# create all the required tables
	def create_tables(self):
		try:
			# existing tables for the database
			table_list = self.db.table_list().run(self.connection)

			for table in self.tables:
				if (table not in table_list):
					r.table_create(table).run(self.connection)
		except Exception, e:
			raise e

	# query a set of 'id' documents and remove them
	def query_id(self):
		# using GIL approach
		self.lock.acquire()

		# 'master' in 'danger' zone but 'slave' storage not ready
		if (not self.slave_ready and self.master_state() == STATES['danger']):
			self.lock.release()
			return []

		try:
			# collect documents of given sample size
			response = list(self.table.order_by('id').limit(self.sample_size).run(self.connection))

			# remove them from the database, makes use of the RethinkDB UUID keys
			self.table.order_by('id').limit(self.sample_size).delete().run(self.connection)

			# update the global 'counter' resource
			self.count += len(response)

			# regularly check for state change
			self.state_check()

		except Exception, e:
			raise e
		finally:
			self.lock.release()

		return response

	# return the state of 'master' storage table
	def master_state(self):
		self.lock.acquire()
		count = self.table.count().run(self.connection)

		if (count < self.caution_threshold):
			state = STATES['safe']
		elif (self.caution_threshold <= count < self.store_size):
			state = STATES['caution']
		else:
			state = STATES['danger']

		self.lock.release()
		return state

	# take further actions according to the current state
	def state_check(self):
		self.lock.acquire()
		state = self.master_state()

		if (state == STATES['caution']):
			if (not self.caution_aware):
				# ask 'controller' to start filling process [gevent]
				pass
		elif (state == STATES['danger']):
			# shuffle the storage
			pass
