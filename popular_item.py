from cassandra.query import named_tuple_factory
from dbconf import KEYSPACE, LOGGING_LEVEL, PRINT_OUTPUT
from udt import OrderLine
import logging

# We can be sure customer and item won't get deleted or have name changed as these
# are not part of the transaction sets.
# So we somehow can achieve serializability without having to do anything for this transaction.
def popularItems(w_id, d_id, numOrders, session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("processing popular items")

	orders = session.execute('SELECT o_id, o_c_id, o_entry_d, o_o_lines from "order" WHERE o_w_id=%s AND o_d_id=%s LIMIT %s', [w_id, d_id, numOrders])
	
	# Keep a copy of orders in memory so we can iterate over it
	# as many times as needed
	orders = map(lambda o: o, orders)

	distinct_customers = set(map(lambda o: o.o_c_id, orders))

	# No distinct customer implies no order
	if len(distinct_customers)==0:
		return

	# Use async queries as the next computation step may take quite a while
	# so we want to exploit parallelism to speed things up since query and code logic
	# happen on different machines
	cname_future = session.execute_async('SELECT c_id, c_first, c_middle, c_last FROM customer WHERE c_w_id=%s AND c_d_id=%s', [w_id, d_id])
	iname_future = session.execute_async('SELECT i_id, i_name FROM item')

	# Compute list of popular items
	xact_popular_items = []
	for o in orders:
		pop_items = []
		max_cnt = 0
		for item in o.o_o_lines:
			if item.ol_quantity >= max_cnt:
				if item.ol_quantity>max_cnt:
					pop_items = []
				pop_items.append({'id': item.ol_i_id, 'qty': item.ol_quantity})
		xact_popular_items.append(pop_items)
	numOrders = len(xact_popular_items)

	# Get list of distinct popular item IDs
	i_ids = set()
	for r in xact_popular_items:
		for c in r:
			i_ids.add(c['id'])

	try:
		cname_map = {}
		for customer in cname_future.result():
			if customer.c_id in distinct_customers:
				cname_map[customer.c_id] = {'first': customer.c_first, 'middle': customer.c_middle, 'last': customer.c_last}
		iname_map = {}
		for item in iname_future.result():
			if item.i_id in i_ids:
				iname_map[item.i_id] = item.i_name

		item_xact_cnt = {}
		if PRINT_OUTPUT:
			for idx, ol in enumerate(xact_popular_items):
				print 'Order %d on %s' % (orders[idx].o_id, orders[idx].o_entry_d)
				print 'Customer %s %s %s' % (cname_map[orders[idx].o_c_id]['first'], cname_map[orders[idx].o_c_id]['middle'], cname_map[orders[idx].o_c_id]['last'])
				for item in ol:
					print 'Item: %s' % (iname_map[item['id']])
					print 'Quantity: %d' % (item['qty'])
					if item_xact_cnt.get(item['id']) is None:
						item_xact_cnt[item['id']] = 0
					item_xact_cnt[item['id']]+=1
					print ''
			print ''
			for i_id, num_xact in item_xact_cnt.items():
				print '%s is in %f%% of %d orders' % (iname_map[i_id], num_xact*100/float(numOrders), numOrders)
	except Exception as e:
		logging.error('Error fetching data from database')
		logging.error(str(e))
		
