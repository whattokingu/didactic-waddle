from cassandra.query import SimpleStatement, named_tuple_factory
from dbconf import KEYSPACE
from udt import OrderLine

def orderStatus(w_id, d_id, c_id, cluster):
	session = cluster.connect(KEYSPACE)
	session.row_factory = named_tuple_factory
	cluster.register_user_type(KEYSPACE, 'order_line', OrderLine)

	# Get user data and last order asynchronously
	# to keep chances of screw up due to interleaving transactions to minimum
	userDataFuture = session.execute_async(SimpleStatement('SELECT c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s', [c_w_id, c_d_id, c_id]))
	lastOrderFuture = session.execute(SimpleStatement('SELECT o_id, o_entry_d, o_carrier_id, o_o_lines FROM "order" WHERE o_w_id=%s AND o_d_id=%s AND o_c_id=%s DESC LIMIT 1'))
	
	try:
		userData = userDataFuture.result()
		lastOrder = lastOrderFuture.result()
		session.shutdown()

		if len(userData)==0:
			print "Invalid user"
			session.shutdown()
			return

		userData = userData[0]

		print '%s %s %s with balance %f' % (userData.c_first, userData.c_middle, userData.c_last, userData.c_balance)
		
		for order in lastOrder:
			print 'Order ID: %d' % (order.o_id)
			print 'Order date: %s' % (order.o_entry_d)
			print 'Carrier ID: %d' % (order.o_carrier_id)
			print 'Items:'
			for item in order.o_o_lines:
				print 'Item ID: %d' % (item.ol_i_id)
				print 'Supplying warehouse: %d' % (item.ol_supply_w_id)
				print 'Quantity ordered: %d' % (item.ol_quantity)
				print 'Total amount: %f' % (item.ol_amount)
				print 'Delivered on: %s' % (item.ol_delivery_d)
				print ''
	except Exception:
		print 'An error occurred fetching data from database'
		session.shutdown()
