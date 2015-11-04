import tornado.ioloop
import tornado.web
from supplier.supplier import Supplier

supplier = Supplier()

class IdHandler(tornado.web.RequestHandler):
    def get(self):
    	ids = supplier.query_id()
    	ids = [id['value'] for id in ids]

        self.write({'next': ids})

if __name__ == "__main__":
	application = tornado.web.Application([
	    (r"/", IdHandler),
	])
	application.listen(8888)
	tornado.ioloop.IOLoop.current().start()
