# -*- coding: utf-8 -*-
import string

# using alphabets only
chars = string.letters


"""
Series class implemets a sequential 'id' (of fixed length) generator

@id_size : length of the generated 'id' strings
@id_range: number of total 'id' in series
@id_origin: first 'id' of the series
"""
class Series:

	def __init__(self, id_origin, id_range, id_size=4):
		self.id_size = id_size
		self.id_range = id_range
		self.id_origin = id_origin
		self.id_cursor = id_origin

	# generator function that returns the next 'id'
	def elements(self):
		for i in range(self.id_range):
			yield self.generate_id(self.id_cursor)

	# returns alphabetical descendant of a 'string'
	def generate_id(self, reference):
		"""
		New proposal for ID generation

		@id_cursor : sequentially increasing ID

		if (id_cursor > ''.join(sorted(id_cursor))):
			IGNORE
		elif (id_cursor == ''.join(sorted(id_cursor))):
			ADD ALL ANAGRAMS
		else:
			IT WON'T HAPPEN
		"""

		length = len(reference)
		if (length != self.id_size):
			return False

		id = ''
		carry = [0] * (self.id_size - 1) + [1]

		for i in range(length - 1, -1, -1):
			temp = chars.index(reference[i]) + carry[i]
			if ((temp / len(chars)) > 0):
				if (i <= 0):
					return False
				else:
					carry[i - 1] += 1 

			new_char = chars[temp % len(chars)]
			id = "%s%s" % (new_char, id)

		self.id_cursor = id
		return id
