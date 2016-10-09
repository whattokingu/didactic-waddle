import sys
sys.path.insert(0, '../')


from cassandra.cluster import Cluster 
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, LOGGING_LEVEL
from cassandra.query import BatchStatement, SimpleStatement, ValueSequence
from datetime import datetime
import time
import udt
import logging

# @params warehouse number of delivery
# @params identifier for carrier for deliver
# @param cluster: an instance of cassandra cluster
def delivery(wid, carrierid, session):
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	logger.info("processing delivery transaction")

	#getting order info
	oldestDistrictOrder = []
	district_ids = range(1, 11)
	oldestDistrictOrder = session.execute(
		"""
		SELECT * from "order"
		WHERE o_w_id=%s AND o_d_id IN %s AND o_carrier_id = %s
		PER PARTITION LIMIT 1
		ALLOW FILTERING
		""", [wid, ValueSequence(district_ids), -1])
	custBal = dict()
	custDelCnt = dict()
	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL) 
	for order in oldestDistrictOrder:
		cust_delivery_res = session.execute(
			"""
			SELECT c_delivery_cnt
			FROM customer
			WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s
			""",
			(order.o_c_id, order.o_d_id, order.o_w_id)
			)
		for row in cust_delivery_res:
			custDelCnt[order.o_c_id] = row.c_delivery_cnt
	
		cust_balance_res = session.execute(
			"""
			SELECT c_balance
			FROM customer
			WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s
			""",
			(order.o_c_id, order.o_d_id, order.o_w_id)
			)
		for row in cust_balance_res:
			custBal[order.o_c_id] = row.c_balance

		ol = order.o_o_lines
		totalAmt = 0
		delTime = int(time.mktime(datetime.now().timetuple()) * 1000) #current time in int
		orders = []
		for line in ol:
			totalAmt += line.ol_amount
			orders.append(udt.OrderLine(line.ol_i_id, delTime, line.ol_amount, line.ol_supply_w_id, line.ol_quantity, line.ol_dist_info))
		orderUpdate = session.prepare(
			"""
			UPDATE "order"
			SET o_carrier_id = ?, o_o_lines = ?
			WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?
			"""
			)
		batch.add(orderUpdate, [carrierid, orders, order.o_id, order.o_d_id, order.o_w_id])
		#update customer balance
		#batch.add(
		#	"""
		#	UPDATE customer			
		#	SET c_balance = %s, c_delivery_cnt = %s
		#	WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s
		#	""",
		#	(custBal[order.o_c_id] + totalAmt, custDelCnt[order.o_c_id] + 1, order.o_c_id, order.o_d_id, order.o_w_id)
		#	)
		batch.add(
			"""
			DELETE FROM customer_balance
			WHERE c_w_id = %s AND c_d_id = %s AND c_id=%s AND c_balance = %s
			""",
			(order.o_w_id, order.o_d_id, order.o_c_id, custBal[order.o_c_id])
			)
		batch.add(
			"""
			INSERT INTO customer_balance (c_w_id, c_d_id, c_id, c_balance) VALUES (%s, %s, %s, %s)
			""",
			(order.o_w_id, order.o_d_id, order.o_c_id, custBal[order.o_c_id] + totalAmt)
			)
		batch.add(
			"""
			UPDATE customer
			SET c_delivery_cnt = %s, c_balance=%s
			WHERE c_id = %s AND c_d_id = %s AND c_w_id =%s
			""",
			(custDelCnt[order.o_c_id] + 1, custBal[order.o_c_id] + totalAmt, order.o_c_id, order.o_d_id, order.o_w_id)
			)
		session.execute(batch)
		batch.clear()
# cluster = Cluster()
# cluster = cluster.connect(KEYSPACE)
# delivery(5, 11, cluster)

