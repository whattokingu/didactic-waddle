from cassandra.cluster import Cluster 
from dbconf import KEYSPACE, CONSISTENCY_LEVEL
from cassandra.query import BatchStatement, SimpleStatement
from datetime import datetime
import time
import udt

# @param custId: a dict with 3 attribs (w_id, d_id, c_id) 
# @param payment: amount of payment
# @param cluster: an instance of cassandra cluster
def custPayment(custId, payment, cluster):
	print "processing customer payment"
	session = cluster.connect(KEYSPACE)

	#get warehouse info
	warehouseData = dict()
	warehouse_res = session.execute(
		"""
		SELECT w_ytd, w_address
		FROM warehouse
		WHERE w_id =
		""" + str(custId['w_id'])
		)
	for row in warehouse_res:
		warehouseData['w_ytd'] = row.w_ytd
		warehouseData['w_address'] = row.w_address
	#get district info
	districtData = dict()
	district_res = session.execute(
		"""
		SELECT d_ytd, d_address
		FROM district
		WHERE d_w_id = %s AND d_id = %s
		""",
		(custId['w_id'], cust['d_id'])
		)
	for row in district_res:
		print row
		districtData['d_ytd'] = row.d_ytd
		districtData['d_address'] = row.d_address
	#get customer info
	custInfo = dict()
	cust_res = session.execute(
		"""
		SELECT c_first, c_middle, c_last, c_address, c_phone, c_since, 
		c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt
		FROM customer
		WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s
		""",
		(custId['w_id'], custId['d_id'], custId['c_id'])
		)
	for row in cust_res:
		print row
		custInfo['c_first'] = row.c_first
		custInfo['c_middle'] = row.c_middle
		custInfo['c_last'] = row.c_last
		custInfo['c_address'] = row.c_address
		custInfo['c_phone'] = row.c_phone
		custInfo['c_since'] = row.c_since
		custInfo['c_credit'] = row.c_credit
		custInfo['c_credit_lim'] = row.c_credit_lim
		custInfo['c_discount'] = row.c_discount
		custInfo['c_balance'] = row.c_balance
		custInfo['c_ytd_payment'] = row.c_ytd_payment
		custInfo['c_payment_cnt'] = row.c_payment_cnt
	# batch update tables	
	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL)
	batch.add(
		"""
		UPDATE warehouse
		SET w_ytd = %s
		WHERE w_id = %s
		""",
		(warehouseData['w_ytd'] + payment, custId['w_id']))
	batch.add(
		"""
		UPDATE district
		SET d_ytd = %s
		WHERE d_w_id = %s AND d_id = %s
		""",
		(districtData['d_ytd'] + payment, custId['w_id'], custId['d_id'])
		)
	batch.add(
		"""
		UPDATE customer
		SET c_balance = %s, c_ytd_payment = %s, c_payment_cnt = %s
		WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
		""",
		(custInfo['c_balance'] - payment, custInfo['c_ytd_payment'] + payment, custInfo['c_payment_cnt'] + 1, custId['w_id'], custId['d_id'], custId['c_id'])
		)
	session.execute(batch)
	print "Customer: " + str(custId['w_id']) + ' ' + str(custId['d_id']) + ' ' + str(custId['c_id']) + ' ' + custInfo['c_first'] + ' ' + custInfo['c_middle'] + ' ' + custInfo['c_last']
	print formatAddress(custInfo['c_address'])
	print custInfo['c_phone']
	print custInfo['c_since']
	print custInfo['c_credit']
	print custInfo['c_credit_lim']
	print custInfo['c_discount']
	print custInfo['c_balance'] - payment
	print "Warehouse address: "
	print formatAddress(warehouseData['w_address'])
	print "district address: "
	print formatAddress(districtData['d_address'])
	print "payment: " + str(payment)

def formatAddress(address):
		return address.street_1 + '\n' + address.street_2 + '\n' + address.city + ', ' + address.state + '\n' + str(address.zipcode)
cluster = Cluster()
cust = dict()
cust['w_id'] = 5
cust['d_id'] = 9
cust['c_id'] = 5
custPayment(cust, 123123,cluster)