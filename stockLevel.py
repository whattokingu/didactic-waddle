from cassandra.query import SimpleStatement, named_tuple_factory, ValueSequence
from dbconf import KEYSPACE, LOGGING_LEVEL, PRINT_OUTPUT
from udt import OrderLine
from cassandra.cluster import Cluster 
import logging

def stockLevel(wid, did, threshold, L, session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("processing stock level")

	# Orders partitioned by (w_id, d_id) and clustered by o_id in DESC order
	# So 1 query on correct partition without ORDER BY and applying LIMIT is enough
	orderlines_res = session.execute(
		"""
		SELECT o_o_lines
		FROM "order"
		WHERE o_w_id = %s AND o_d_id = %s LIMIT %s
		""",
		(wid, did, L))
	stockId = set()
	for row in orderlines_res:
		for item in row.o_o_lines:
			stockId.add(item.ol_i_id)

	stocklevel_res = session.execute(
		"""
		SELECT s_quantity, s_i_id
		FROM stock
		WHERE s_w_id = %s and s_i_id in %s
		""",
		[wid, ValueSequence(stockId)])

	# Count stock items below threshold quantity
	count = 0
	for row in stocklevel_res:
		if row.s_quantity < threshold:
			count+=1
	if PRINT_OUTPUT:
		print "number of stock below threshold: " + str(count)

# cluster = Cluster()
# session = cluster.connect(KEYSPACE)
# stockLevel(3,3,100,10, session)