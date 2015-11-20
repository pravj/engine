import tornado.ioloop
import tornado.web
from supplier.supplier import Supplier

supplier = None

# returns index response
class MainHandler(tornado.web.RequestHandler):
	def get(self):
		self.write("Project Blitz : Engine powered by tornado")

# returns next IDs to collect
class IdHandler(tornado.web.RequestHandler):
    def get(self):
    	ids = supplier.query_id()
    	ids = [id['value'] for id in ids]

        self.write({'next': ids})

# returns 'supplier' statistics
class StatsHandler(tornado.web.RequestHandler):
	def get(self):
		res = "Project Blitz : Item(s) shipped %d" % (supplier.count)
		self.write(res)

if __name__ == "__main__":
	supplier = Supplier()

	application = tornado.web.Application([
		(r"/", MainHandler),
	    (r"/id", IdHandler),
	    (r"/stats", StatsHandler),
	])
	application.listen(8888)

	tornado.ioloop.IOLoop.current().start()
