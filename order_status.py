from cassandra.query import SimpleStatement, named_tuple_factory
from dbconf import KEYSPACE, LOGGING_LEVEL, PRINT_OUTPUT
from udt import OrderLine
import logging

def orderStatus(w_id, d_id, c_id, session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("processing order status")
	# Get user data and last order asynchronously
	# to keep chances of screw up due to interleaving transactions to minimum
	userDataFuture = session.execute_async('SELECT c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s', [w_id, d_id, c_id])
	lastOrderFuture = session.execute_async('SELECT o_id, o_entry_d, o_carrier_id, o_o_lines FROM "order" WHERE o_w_id=%s AND o_d_id=%s AND o_c_id=%s', [w_id, d_id, c_id])
	
	try:
		userData = userDataFuture.result().current_rows
		lastOrder = max(lastOrderFuture.result(), key = lambda o: o.o_id)
		print lastOrder

		if len(userData)==0:
			logger.error("Invalid user")
			session.shutdown()
			return

		userData = userData[0]
		if PRINT_OUTPUT:
			print '%s %s %s with balance %f' % (userData.c_first, userData.c_middle, userData.c_last, userData.c_balance)
			# for order in lastOrder:
			print 'Order ID: %d' % (lastOrder.o_id)
			print 'Order date: %s' % (lastOrder.o_entry_d)
			print 'Carrier ID: %d' % (lastOrder.o_carrier_id)
			print 'Items:'
			# print lastOrder.o_o_lines
			for item in lastOrder.o_o_lines:
				print 'Item ID: %d' % (item.ol_i_id)
				print 'Supplying warehouse: %d' % (item.ol_supply_w_id)
				print 'Quantity ordered: %d' % (item.ol_quantity)
				print 'Total amount: %f' % (item.ol_amount)
				print 'Delivered on: %s' % (item.ol_delivery_d)
				print ''
	except Exception as e:
		logger.error('An error occurred fetching data from database')
		logger.error(str(e))
		session.shutdown()
