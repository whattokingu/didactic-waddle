from cassandra.cluster import Cluster
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, LOGGING_LEVEL, PRINT_OUTPUT
import logging

def topbalance(session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("Getting customers with highest balances")

	# Asynchronously query for warehouse and district names so we can use
	# for printing later
	warehouse_name_future = session.execute_async('SELECT w_id, w_name FROM warehouse')
	district_name_future = session.execute_async('SELECT d_w_id, d_id, d_name FROM district')
	customer_future = session.execute_async('SELECT c_w_id, c_d_id, c_first, c_middle, c_last, c_balance FROM customer')

	try:
		# Get 10 customers with highest balance
		# Consistently 5s slower than calling sorted()
		# balance_dict = {}
		# for c in customer_future.result():
		# 	if balance_dict.get(c.c_balance) is None:
		# 		balance_dict[c.c_balance] = []
		# 	balance_dict[c.c_balance].append(c)
		# top_customers = []
		# cust_cnt = 0
		# for bal, clist in sorted(balance_dict.iteritems(), reverse=True):
		# 	for c in clist:
		# 		top_customers.append(c)
		# 		cust_cnt+=1
		# 		if cust_cnt==10:
		# 			break
		# 	if cust_cnt==10:
		# 		break

		top_customers = sorted(customer_future.result(), key = lambda c: c.c_balance, reverse = True)[0:9]

		# Map warehouse ID to warehouse name
		warehouse_dict = {}
		for row in warehouse_name_future.result():
			warehouse_dict[row.w_id] = row.w_name
		
		# Map district's warehouse ID and district ID to district name
		district_dict = {}
		for row in district_name_future.result():
			district_dict[(row.d_w_id, row.d_id)] = row.d_name
		if PRINT_OUTPUT:	
			for cust in top_customers:
				print 'Customer %s %s %s' % (cust.c_first, cust.c_middle, cust.c_last)
				print 'Outstanding Balance: %d' % (cust.c_balance)
				print 'Warehouse Name: %s' % (warehouse_dict[cust.c_w_id]) 
				print 'District Name: %s' % (district_dict[(cust.c_w_id, cust.c_d_id)])
	except Exception as e:
		print str(e)	

# cluster = Cluster()
# session = cluster.connect(KEYSPACE)
# topbalance(session)
