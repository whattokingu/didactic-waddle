
from cassandra.cluster import Cluster
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, LOGGING_LEVEL, PRINT_OUTPUT
import logging

def topbalance(session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("Getting customers with highest balances")

	# Asynchronously query for warehouse and district names so we can use
	# for printing later
	customer_balance_future = session.execute_async('SELECT c_w_id, c_d_id, c_id, c_balance FROM customer_balance where c_d_id in (1,2,3,4,5,6,7,8,9,10) per partition limit 10')
	warehouse_name_future = session.execute_async('SELECT w_id, w_name FROM warehouse')
	district_name_future = session.execute_async('SELECT d_w_id, d_id, d_name FROM district')
	try:
		# Map warehouse ID to warehouse name
		warehouse_dict = {}
		for row in warehouse_name_future.result():
			warehouse_dict[row.w_id] = row.w_name

		# Map district's warehouse ID and district ID to district name
		district_dict = {}
		for row in district_name_future.result():
			district_dict[(row.d_w_id, row.d_id)] = row.d_name


		# Get 10 customers with highest balance
		result = customer_balance_future.result()
		top_customers = sorted(result, key = lambda c: c.c_balance, reverse = True)[0:9]		
		for customer in top_customers:
			cust_info_res = session.execute(
				'SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last FROM customer where c_w_id=%s and c_d_id=%s and c_id=%s', 
				(customer.c_w_id, customer.c_d_id, customer.c_id))
			cust_info = None
			for row in cust_info_res:
				cust_info = row
			if PRINT_OUTPUT:	
				print 'Customer %s %s %s' % (cust_info.c_first, cust_info.c_middle, cust_info.c_last)
				print 'Customer %s %s %s' % (customer.c_w_id, customer.c_d_id, customer.c_id)
				print 'Outstanding Balance: %d' % (customer.c_balance)
				print 'Warehouse Name: %s' % (warehouse_dict[customer.c_w_id]) 
				print 'District Name: %s' % (district_dict[(customer.c_w_id, customer.c_d_id)])

	except Exception as e:
		print str(e)
		logger.error("An error occurred fetching data from database")
		logger.error(str(e))


# cluster = Cluster()
# session = cluster.connect(KEYSPACE)
# topbalance(session)
