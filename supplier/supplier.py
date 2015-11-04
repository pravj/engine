# -*- coding: utf-8 -*-

import rethinkdb as r
import threading
import gevent
from controller.controller import Controller
from Queue import Queue

TABLE_1 = 'id_store_1'
TABLE_2 = 'id_store_2'
DATABASE = 'id_archive'

STATES = {'safe': 'SAFE', 'caution': 'CAUTION', 'danger': 'DANGER'}

class Supplier:

	def __init__(self, sample_size=100, store_size=20000, caution_threshold=15000):
		self.connection = None

		self.db = None
		self.table = None

		self.origin = 'Faaa'
		self.response_queue = Queue()

		self.tables = set([TABLE_1, TABLE_2])
		self.master = set([TABLE_1])
		self.slave = self.tables.difference(self.master)

		self.count = 0
		self.lock = threading.RLock()

		self.sample_size = sample_size
		self.store_size = store_size
		self.caution_threshold = caution_threshold

		self.caution_aware = False

		self.controller = None
		self.call_controller(next(iter(self.master)))

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

	# invokes the filling process according to the system state
	def call_controller(self, control_table=None):
		table = next(iter(self.slave)) if control_table is None else control_table
		print "call_controller %s" % (table)

		if (self.controller == None):
			print self.origin
			print "no controller"
			self.controller = Controller(table)
			print "inserting data"
			self.origin = self.controller.insert_data(self.store_size, self.origin, queue=None)
			print self.origin
		else:
			if (self.response_queue.qsize() != 0):
				self.origin = self.response_queue.get()

			print self.origin

			print "shift control to table %s" % (table)
			self.controller.shift_control(table)
			print "start thread execution"
			t = threading.Thread(target = self.controller.insert_data, args=(self.store_size, self.origin, self.response_queue, ))
			t.start()

	# checks if the 'slave' storage is ready to serve or not
	def slave_ready(self):
		self.lock.acquire()

		count = self.db.table(next(iter(self.slave))).count().run(self.connection)
		answer = (count == self.store_size)

		self.lock.release()

		return answer

	# query a set of 'id' documents and remove them
	def query_id(self):
		# using GIL approach
		self.lock.acquire()

		print self.master_state()

		# 'master' in 'danger' zone but 'slave' storage not ready
		if (self.master_state() == STATES['danger']):
			print "'master' in 'danger' zone but 'slave' storage not ready"
			if (not self.slave_ready()):
				print "not slave ready"
				self.lock.release()
				return []
			else:
				print self.master, self.slave, self.caution_aware
				self.master, self.slave = self.slave, self.master
				self.table = self.db.table(next(iter(self.master)))
				self.caution_aware = not(self.caution_aware)
				print self.master, self.slave, self.caution_aware

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

		if (count == 0):
			state = STATES['danger']
		elif (count <= (self.store_size - self.caution_threshold)):
			state = STATES['caution']
		else:
			state = STATES['safe']

		self.lock.release()
		return state

	# take further actions according to the current state
	def state_check(self):
		self.lock.acquire()
		state = self.master_state()

		# in case the 'supplier' is not aware about the 'state'
		if (state == STATES['caution']):
			if (not self.caution_aware):
				self.caution_aware = not(self.caution_aware)

				# ask 'controller' to start filling process [gevent]
				self.call_controller()

		self.lock.release()