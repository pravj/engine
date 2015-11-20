# -*- coding: utf-8 -*-

from generator.generator import Series
from random import shuffle
import rethinkdb as r

DATABASE = 'id_archive'


"""
Controller class implements the functionality to fill the 'slave' store
"""
class Controller:

	def __init__(self, control_table):
		self.control_table = control_table
		self.connection = None

		self.db = None
		self.table = None

		self.connect()

	# connect the 'controller' instance to the database
	def connect(self):
		try:
			self.connection = r.connect(host='localhost', port=28015)

			# use the 'id_archive' database with this connection
			self.connection.use(DATABASE)

			# store reference to current 'table' and 'database'
			self.db = r.db(DATABASE)
			self.table = self.db.table(self.control_table)
		except Exception, e:
			raise e

	# shift control between 'master' and 'slave' tables
	def shift_control(self, table):
		self.table = self.db.table(table)

	# generate and insert elements of the series in bulk
	def insert_data(self, limit, origin, queue=None):
		print 'start series insertion'
		elements = Series(origin, limit, 4).elements()
		print origin, limit

		id_list = []
		element = None

		count = 0
		while (count < limit):
			next, element_list = elements.next()
			for element in element_list:
				id_list.append({'value': element})
				count += 1

		self.table.insert(id_list).run(self.connection)

		if (queue is None):
			return next
		else:
			queue.put(next)
