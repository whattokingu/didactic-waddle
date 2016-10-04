from cassandra.query import SimpleStatement, named_tuple_factory
from dbconf import KEYSPACE, PRINT_OUTPUT
from udt import OrderLine
from cassandra.cluster import Cluster 

def stockLevel(wid, did, threshold, L, session):
	district_res = session.execute(
		"""
		SELECT d_next_o_id
		FROM district
		WHERE d_id=%s AND d_w_id=%s
		"""
		,(did, wid))
	orderNum = 0
	for row in district_res:
		orderNum = row.d_next_o_id
	orderlines_res = session.execute(
		"""
		SELECT o_o_lines
		FROM "order"
		WHERE o_w_id = %s AND o_d_id = %s AND o_id < %s AND o_id >= %s
		""",
		(wid, did, orderNum, orderNum - L))
	stockId = set()
	for row in orderlines_res:
		for item in row.o_o_lines:
			stockId.add(item.ol_i_id)

	stocklevelquery = session.prepare(
		"""
		SELECT s_quantity, s_i_id
		FROM stock
		WHERE s_w_id = ? and s_i_id in ?
		""")

	count = 0
	stocklevel_res = session.execute(stocklevelquery, [wid, stockId])
	
	for row in stocklevel_res:
		if row.s_quantity < threshold:
			count+=1
	if PRINT_OUTPUT:
		print "number of stock below threshold: " + str(count)
# cluster = Cluster()
# session = cluster.connect(KEYSPACE)
# stockLevel(3,3,100,10, session)