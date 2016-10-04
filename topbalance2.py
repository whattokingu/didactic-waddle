from cassandra.cluster import Cluster
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, LOGGING_LEVEL, PRINT_OUTPUT
import logging


def topbalance(session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("Getting customers with highest balances")
	balance_res = session.execute('select c_balance from customer');
	sortedBalance = list(sorted(balance_res, key = lambda c: -c.c_balance))
	balance = sortedBalance[9].c_balance

	top_bal_query = session.prepare(
		"""
		SELECT c_w_id, c_d_id, c_id, c_balance, c_first, c_middle, c_last
		FROM customer
		WHERE c_balance >= ?
		ALLOW FILTERING
		""")
	top_bal_res = session.execute(top_bal_query, [balance])
	top_bal = list(sorted(top_bal_res, key = lambda c: -c.c_balance))
	top_bal = top_bal[0:9]
	district_set = set()
	warehouse_set = set()
	for row in top_bal:
		district_set.add(row.c_d_id)
		warehouse_set.add(row.c_w_id)
	
	warehouse_query = session.prepare(
		"""
		SELECT w_id, w_name
		FROM warehouse
		WHERE w_id in ?
		""")
	warehouse_name_res = session.execute(warehouse_query, [warehouse_set])

	warehouse_dict = dict()
	for row in warehouse_name_res:
		warehouse_dict[row.w_id] = row.w_name
	
	district_query = session.prepare(
		"""
		SELECT d_w_id, d_id, d_name
		FROM district
		WHERE d_w_id in ? AND d_id in ?
		""")
	district_name_res = session.execute(district_query, [warehouse_set, district_set])
	district_dict = dict()
	for row in district_name_res:
		district_dict[(row.d_w_id, row.d_id)] = row.d_name
	if PRINT_OUTPUT:	
		for cust in top_bal:
			print 'Customer %s %s %s' % (cust.c_first, cust.c_middle, cust.c_last)
			print 'Outstanding Balance: %d' % (cust.c_balance)
			print 'Warehouse Name: %s' % (warehouse_dict[cust.c_w_id]) 
			print 'District Name: %s' % (district_dict[(cust.c_w_id, cust.c_d_id)])

# cluster = Cluster()
# session = cluster.connect(KEYSPACE)
# topbalance(session)
