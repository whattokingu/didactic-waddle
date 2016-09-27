from cassandra.cluster import Cluster 
from dbconf import KEYSPACE, CONSISTENCY_LEVEL
from cassandra.query import BatchStatement
from datetime import datetime
import time
import udt


# @param custId: a dict with 3 attribs (w_id, d_id, c_id)
# @param numItems: number of items in order
# @param itemNumbers: itemIdentifiers. Should be array of size numItems
# @param itemNumbers: warehouse for item. Should be array of size numItems
# @param itemNumbers: qty for item. Should be array of size numItems
# @param cluster: cassandra cluster object
def newOrder(custId, numItems, itemNumbers, supplierWarehouses, qty, cluster):
	print "Processing new order transaction"

	session = cluster.connect(KEYSPACE)
	#get order number
	orderNum_res = session.execute(
		"""
		SELECT d_next_o_id 
		FROM district 
		WHERE d_w_id = %s AND d_id = %s
		""",
		(custId['w_id'], custId['d_id'])
		)
	orderNum = 0
	for row in orderNum_res:
		orderNum = row.d_next_o_id
	#get taxes
	taxes = dict()
	taxes_res = session.execute(
		"""
		SELECT d_tax, d_w_tax
		FROM district
		WHERE d_id = %s AND d_w_id = %s
		""",
		(cust['d_id'], cust['w_id'])
	)
	for row in taxes_res:
		taxes['w_tax'] = row.d_w_tax
		taxes['d_tax'] = row.d_tax

	#get cust info	
	custDisc_res = session.execute(
		"""
		SELECT c_discount, c_last, c_credit
		FROM customer
		WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
		""",
		(cust['w_id'], cust['d_id'], cust['c_id'])
		)
	custInfo = dict()
	for row in custDisc_res:
		custInfo['c_discount'] = row.c_discount
		custInfo['c_last'] = row.c_last
		custInfo['c_credit'] = row.c_credit


	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL)
	#get and update stock info
	stockVol = dict()
	stockPrice = dict()
	sDistInfo = dict()
	sYTD = dict()
	sOrderCnt = dict()
	sRemoteCnt = dict()
	sName = dict()
	totalAmount = 0
	stockVolQuery = session.prepare(
		"""
		SELECT s_quantity, s_price, s_ytd, s_order_cnt, s_remote_cnt, s_name,
		""" + ' s_dist_' + helper(cust['d_id']) + ' as sinfo '
		+
		"""
		FROM stock
		WHERE s_i_id = ? AND s_w_id = ?
		""")
	stockUpdateQuery = session.prepare(
		"""
		UPDATE stock
		SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ?
		WHERE s_i_id = ? AND s_w_id = ?
		"""
		)
	for i in range(0, numItems):
		stockVol_res = session.execute(stockVolQuery, [itemNumbers[i], supplierWarehouses[i]])
		for row in stockVol_res:
			stockVol[itemNumbers[i]] = row.s_quantity
			stockPrice[itemNumbers[i]] = row.s_price
			sDistInfo[itemNumbers[i]] = row.sinfo
			sYTD[itemNumbers[i]] = row.s_ytd
			sOrderCnt[itemNumbers[i]] = row.s_order_cnt
			sRemoteCnt[itemNumbers[i]] = row.s_remote_cnt
			sName[itemNumbers[i]] = row.s_name
			totalAmount += row.s_price * qty[i]
		adjusted_qty = stockVol[itemNumbers[i]] - qty[i]
		if adjusted_qty < 10:
			adjusted_qty += 100
		stockVol[itemNumbers[i]] = adjusted_qty
		remoteCnt = sRemoteCnt[itemNumbers[i]]
		if supplierWarehouses[i] != custId['w_id']:
			remoteCnt+=1
		print 
		batch.add(stockUpdateQuery, [adjusted_qty, sYTD[itemNumbers[i]] + qty[i], sOrderCnt[itemNumbers[i]]+1, remoteCnt, itemNumbers[i], supplierWarehouses[i]])


	batch.add(
		"""
		UPDATE district 
		SET d_next_o_id= %s
		WHERE d_w_id = %s and d_id = %s
		""",
		(orderNum+1, custId['w_id'], custId['d_id'])
		)
	#new order details
	order = dict()
	order['id'] = orderNum
	order['d_id'] = cust['d_id']
	order['w_id'] = cust['w_id']
	order['c_id'] = cust['c_id']
	order['entry_d'] = int(time.mktime(datetime.now().timetuple())*1000) #current time in int
	order['carrier_id'] = -1
	order['ol_cnt'] = numItems
	order['all_local'] = 1 if all(int(w_id) == int(cust['w_id']) for w_id in supplierWarehouses) else 0
	order['o_lines'] = []
	for i in range(0, numItems):
		order['o_lines'].append(udt.OrderLine(itemNumbers[i], -1, stockPrice[itemNumbers[i]] * qty[i], supplierWarehouses[i], qty[i], sDistInfo[itemNumbers[i]]))
	insert_order = session.prepare('INSERT INTO "order" (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d, o_o_lines) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
	batch.add(insert_order,[order['w_id'], order['d_id'], order['id'], order['c_id'], order['carrier_id'], order['ol_cnt'], order['all_local'], order['entry_d'], order['o_lines']])

	session.execute(batch)
	print 'Customer: ' + str(cust['w_id']) + ' ' + str(cust['d_id']) + ' ' + str(cust['c_id']) + ' ' + str(custInfo['c_last']) + ' ' + str(custInfo['c_credit']) + ' ' + str(custInfo['c_discount'])
	print 'w_tax: ' + str(taxes['w_tax']) + ' d_tax: ' + str(taxes['d_tax'])
	print 'order: ' + str(order['id']) + ' date: ' + str(order['entry_d'])
	print 'numItems: ' + str(numItems) + ' total amount: ' + str(totalAmount*(1+taxes['w_tax']+taxes['d_tax'])*(1-custInfo['c_discount']))
	for item in order['o_lines']:
		print '  ' + str(item._i_id) + ' ' + sName[item._i_id] + ' ' + str(item._supply_w_id) + ' ' + str(item._quantity) + ' ' + str(item._amount) + ' ' + str(stockVol[item._i_id])


# helper to convert district number to string.
def helper(dist):
	if dist < 10: 
		return '0' + str(dist)
	else :
		return str(dist)

# cluster = Cluster()
# cust = dict()
# cust['d_id'] = 9
# cust['w_id'] = 5
# cust['c_id'] = 5
# newOrder(cust, 3, [60004,60005,60006], [5, 5, 5], [3, 3, 3], cluster)

