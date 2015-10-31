# -*- coding: utf-8 -*-

from .generator.generator import Series
import rethinkdb as r

DATABASE = 'id_archive'


"""
Controller class implements the functionality to fill the 'slave' store
"""
class Controller:

	def __init__(self, slave_table):
		self.slave_table = slave_table
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
			self.table = self.db.table(self.slave_table)
		except Exception, e:
			raise e

	# generate and insert elements of the series in bulk
	def insert_data(self):
		series = Series(4, 500, 'Faaa')
		id_list = []

		for i in range(500):
			id_list.append({'id': series.next()})

		self.table.insert(id_list).run()

