from cassandra.cluster import Cluster
from dbconf import KEYSPACE
from cassandra.query import named_tuple_factory

# top balance transaction
# to create index on customer(c_balance)

def topbalance():
	session = cluster.connect(KEYSPACE)

	# get a subset of customer info where c_id = 1 and 2
	customer = session.execute(
		"""
		SELECT c_w_id, c_d_id, c_id, c_balance, c_first, c_middle, c_last 
		FROM customer 
		PER PARTITION LIMIT 2
		""")

	# sort the subset info (desc order based on c_balance)
	sort_cust = list(sorted(customer, key = lambda c: -c.c_balance))

	# get the top 10 of subset
	current_top = sort_cust[:10]

	# get max(c_balance) and min(c_balance) from top 10
	top_bal = current_top[0].c_balance
	min_bal = current_top[9].c_balance

	top_bal_stmt = session.prepare(
		"""
		SELECT c_w_id, c_d_id, c_id, c_balance, c_first, c_middle, c_last
		FROM customer
		WHERE c_balance > ?
		ORDER BY c_balance DESC LIMIT 10
		ALLOW FILTERING
		""")

	flag = 0

	# get top 10 result where c_balance > max(c_balance) in subset
	query = session.execute(top_bal_stmt, [top_bal])

	# get top 10 result where c_balance > min(c_balance) in subset
	if len(query) == 0:
		query = session.execute(top_bal_stmt, [min_bal])
		flag = 1
	
	list(query)

	w_id_stmt = session.prepare(
		'SELECT w_id FROM warehouse WHERE w_name = ?')

	d_id_stmt = session.prepare(
		'SELECT d_id FROM district WHERE d_name = ?')
	
	result = []

	# top 10 = subset
	if len(query) == 0:
		result = current_top
	# top 10 in > max(c_balance) list
	elif flag == 0:
		# top 10 = whole max list
		if len(query) == 10:
			result = query
		# top 10 = max list + subset
		else:
			result = query + current_top
			result = result[:10]
	# top 10 in > min(c_balance) list
	elif flag == 1:
		result = current_top + query
		sorted(result, key = lambda r: -r.c_balance)
		result = result[:10]

	w_id = []
	d_id = []
	w_name_query = []
	d_name_query = []

	# get w_name and d_name
	for r in result:
		w_id.append(r.c_w_id)
		d_id.append(r.c_d_id)
	
	w_name_query = list(session.execute(w_id_stmt, [w_id]))
	d_name_query = list(session.execute(d_id_stmt, [d_id]))
	
	if PRINT_OUTPUT:
		for i in range(0,10):
			print 'Customer %s %s %s' % (result[i].c_first, result[i].c_middle, result[i].c_last)
			print 'Outstanding Balance: %d' % (result[i].c_balance)
			print 'Warehouse Name: %s' % (w_name_query[i]) 
			print 'District Name: %s' % (d_name_query[i]) 

