from cassandra.cluster import Cluster 
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, PRINT_OUTPUT
from cassandra.query import BatchStatement, SimpleStatement
from datetime import datetime
import time
import udt

# @params warehouse number of delivery
# @params identifier for carrier for deliver
# @param cluster: an instance of cassandra cluster
def delivery(wid, carrierid, session):
	print "processing delivery transaction"

	#getting order info
	oldestDistrictOrder = []
	for i in range(1,11):
		district_res = session.execute(
			"""
			SELECT *
			FROM "order"
			WHERE o_carrier_id = %s AND o_d_id = %s AND o_w_id = %s
			order by o_id ASC
			limit 1
			ALLOW FILTERING
			""",
			(-1, i, wid)
			)
		for row in district_res:
			oldestDistrictOrder.append(row)
	#get customer info
	custBal = dict()
	custDelCnt = dict()
	for order in oldestDistrictOrder:
		cust_res = session.execute(
			"""
			SELECT c_balance, c_delivery_cnt
			FROM customer
			WHERE c_id = %s AND c_d_id = %s and c_w_id = %s
			""",
			(order.o_c_id, order.o_d_id, order.o_w_id)
			)
		for row in cust_res:
			custBal[order.o_c_id] = row.c_balance
			custDelCnt[order.o_c_id] = row.c_delivery_cnt

	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL) 
	# todo: add in delivery date
	for order in oldestDistrictOrder:
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
		batch.add(
			"""
			UPDATE customer
			SET c_balance = %s, c_delivery_cnt = %s
			WHERE c_id = %s AND c_d_id = %s AND c_w_id = %s
			""",
			(custBal[order.o_c_id] + totalAmt, custDelCnt[order.o_c_id] + 1, order.o_c_id, order.o_d_id, order.o_w_id)
			)
	session.execute(batch)
# cluster = Cluster()
# delivery(5, 11, cluster)
# time = int(time.mktime(datetime.now().timetuple())*1000)
# print time