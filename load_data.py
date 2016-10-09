import sys
import csv
from datetime import datetime
from itertools import groupby
import time
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement
from dbconf import KEYSPACE, CONSISTENCY_LEVEL
import udt
import logging

# Cleans all data from DB
# @param cluster The cluster object used to connect to database
def cleanDb(cluster):
	logging.info("Cleaning DB data")
	session = cluster.connect(KEYSPACE)

	# To be updated with other tables that needs cleaning up
	session.execute(SimpleStatement('TRUNCATE stock'))
	session.execute(SimpleStatement('TRUNCATE "order"'))
	session.execute(SimpleStatement('TRUNCATE customer'))
	session.execute(SimpleStatement('TRUNCATE item'))
	session.execute(SimpleStatement('TRUNCATE district'))
	session.execute(SimpleStatement('TRUNCATE warehouse'))
	session.execute(SimpleStatement('TRUNCATE customer_balance'))

	session.shutdown()

# @param dirname Directory path where "warehouse.csv" is stored
# @param cluster The cluster object used to connect to database
def populateWarehouses(dirname, cluster):
	if not dirname.endswith("/"):
		dirname+="/"
	logging.info("Loading warehouse data from %s" % (dirname))

	# Read warehouse data from CSV
	warehouses = []
	with open(dirname+'warehouse.csv', 'r') as warehousecsv:
		warehousereader = csv.DictReader(warehousecsv, fieldnames=['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD'])
		for wh in warehousereader:
			warehouses.append(wh)

	# Populate DB
	cluster.register_user_type(KEYSPACE, 'address', udt.Address)
	session = cluster.connect(KEYSPACE)
	insert_warehouse = session.prepare('INSERT INTO warehouse (w_id, w_name, w_address, w_tax, w_ytd) VALUES (?, ?, ?, ?, ?)')
	for wh in warehouses:
		session.execute(insert_warehouse, [int(float(wh['W_ID'])), wh['W_NAME'], udt.Address(wh['W_STREET_1'], wh['W_STREET_2'], wh['W_CITY'], wh['W_STATE'], wh['W_ZIP']), float(wh['W_TAX']), float(wh['W_YTD'])])
	session.shutdown()

# @param dirname Directory where "district.csv" is stored
# @param cluster The cluster object used to connect to database
def populateDistricts(dirname, cluster):
	if not dirname.endswith('/'):
		dirname+='/'
	logging.info("Loading district data from %s" % (dirname))

	# Read district data from CSV
	districts = []
	with open(dirname+'district.csv', 'r') as districtcsv:
		districtreader = csv.DictReader(districtcsv, fieldnames=['D_W_ID', 'D_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID'])
		for d in districtreader:
			d['D_W_ID'] = int(float(d['D_W_ID']))
			d['D_ID'] = int(float(d['D_ID']))
			d['D_TAX'] = float(d['D_TAX'])
			d['D_YTD'] = float(d['D_YTD'])
			d['D_NEXT_O_ID'] = int(float(d['D_NEXT_O_ID']))
			d['D_ADDRESS'] = udt.Address(d['D_STREET_1'], d['D_STREET_2'], d['D_CITY'], d['D_STATE'], d['D_ZIP'])
			del d['D_STREET_1']
			del d['D_STREET_2']
			del d['D_CITY']
			del d['D_STATE']
			del d['D_ZIP']
			districts.append(d)

	# Populate DB
	cluster.register_user_type(KEYSPACE, 'address', udt.Address)
	session = cluster.connect(KEYSPACE)
	warehouseTaxes = session.execute('SELECT w_id, w_tax FROM warehouse')
	tmpWarehouseTaxes = {}
	for (w_id, w_tax) in warehouseTaxes:
		tmpWarehouseTaxes[w_id] = w_tax
	warehouseTaxes = tmpWarehouseTaxes
	tmpWarehouseTaxes = None

	insert_district = session.prepare('INSERT INTO district (d_w_id, d_id, d_name, d_address, d_tax, d_w_tax, d_ytd, d_next_o_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
	for d in districts:
		session.execute(insert_district, [d['D_W_ID'], d['D_ID'], d['D_NAME'], d['D_ADDRESS'], d['D_TAX'], warehouseTaxes.get(d['D_W_ID']), d['D_YTD'], d['D_NEXT_O_ID']])
	session.shutdown()

# @param dirname Directory path where "customer.csv" is stored
# @param cluster The cluster object used to connect to database
def populateCustomers(dirname, cluster):
	if not dirname.endswith("/"):
		dirname+="/"
	logging.info("Loading customer data from %s" % (dirname))

	# Read customer data from CSV
	customers = []
	with open(dirname+'customer.csv', 'r') as custcsv:
		custreader = csv.DictReader(custcsv, fieldnames=['C_W_ID', 'C_D_ID', 'C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT', 'C_BALANCE', 'C_YTD_PAYMENT', 'C_PAYMENT_CNT', 'C_DELIVERY_CNT', 'C_DATA'])
		for cust in custreader:
			cust['C_W_ID'] = int(float(cust['C_W_ID']))
			cust['C_D_ID'] = int(float(cust['C_D_ID']))
			cust['C_ID'] = int(float(cust['C_ID']))
			cust['C_ADDRESS'] = udt.Address(cust['C_STREET_1'], cust['C_STREET_2'], cust['C_CITY'], cust['C_STATE'], cust['C_ZIP'])
			del cust['C_STREET_1']
			del cust['C_STREET_2']
			del cust['C_CITY']
			del cust['C_STATE']
			del cust['C_ZIP']
			cust['C_SINCE'] = int(time.mktime(datetime.strptime(cust['C_SINCE'][:19], '%Y-%m-%d %H:%M:%S').timetuple()))
			cust['C_CREDIT_LIM'] = float(cust['C_CREDIT_LIM'])
			cust['C_DISCOUNT'] = float(cust['C_DISCOUNT'])
			cust['C_BALANCE'] = float(cust['C_BALANCE'])
			cust['C_YTD_PAYMENT'] = float(cust['C_YTD_PAYMENT'])
			cust['C_PAYMENT_CNT'] = int(float(cust['C_PAYMENT_CNT']))
			cust['C_DELIVERY_CNT'] = int(float(cust['C_DELIVERY_CNT']))
			customers.append(cust)

	# Populate DB
	cluster.register_user_type(KEYSPACE, 'address', udt.Address)
	session = cluster.connect(KEYSPACE)
	insert_customer = session.prepare('INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_address, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
	insert_cbalance = session.prepare('INSERT INTO customer_balance (c_w_id, c_d_id, c_id, c_balance) VALUES (?, ?, ?, ?)')
	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL)
	batch2 = BatchStatement(consistency_level=CONSISTENCY_LEVEL)
	for (w_id, d_id), clist in groupby(sorted(customers, key=lambda c: (c['C_W_ID'], c['C_D_ID'])), lambda c: (c['C_W_ID'], c['C_D_ID'])):
		batchsz = 0
		for c in clist:
			batch.add(insert_customer, [c['C_W_ID'], c['C_D_ID'], c['C_ID'], c['C_FIRST'], c['C_MIDDLE'], c['C_LAST'], c['C_ADDRESS'], c['C_PHONE'], c['C_SINCE'], c['C_CREDIT'], c['C_CREDIT_LIM'], c['C_DISCOUNT'], c['C_YTD_PAYMENT'], c['C_PAYMENT_CNT'], c['C_DELIVERY_CNT'], c['C_DATA']])
			batch2.add(insert_cbalance, [c['C_W_ID'], c['C_D_ID'], c['C_ID'], c['C_BALANCE']])
			batchsz+=1
			if batchsz >= 1500:
				session.execute(batch)
				session.execute(batch2)
				batch.clear()
				batch2.clear()
				batchsz = 0
		if batchsz > 0:
			session.execute(batch)
			session.execute(batch2)
			batch.clear()
			batch2.clear()
	session.shutdown()

# @param dirname Directory path where "item.csv" is stored
# @param cluster The cluster object used to connect to database
def populateItems(dirname, cluster):
	if not dirname.endswith('/'):
		dirname+='/'
	logging.info("Loading item data from %s" % (dirname))

	# Load item data from CSV
	items = []
	with open(dirname+'item.csv', 'r') as itemcsv:
		itemreader = csv.DictReader(itemcsv, fieldnames=['I_ID', 'I_NAME', 'I_PRICE', 'I_IM_ID', 'I_DATA'])
		for i in itemreader:
			i['I_ID'] = int(float(i['I_ID']))
			i['I_PRICE'] = float(i['I_PRICE'])
			i['I_IM_ID'] = int(float(i['I_IM_ID']))
			items.append(i)

	# Populate DB
	session = cluster.connect(KEYSPACE)
	insert_item = session.prepare('INSERT INTO item (i_id, i_name, i_price, i_im_id, i_data) VALUES (?, ?, ?, ?, ?)')
	for i in items:
		session.execute(insert_item, [i['I_ID'], i['I_NAME'], i['I_PRICE'], i['I_IM_ID'], i['I_DATA']])
	session.shutdown()

# @param dirname Directory path where "order.csv" and "order-line.csv" are stored
# @param cluster The cluster object used to connect to database
def populateOrders(dirname, cluster):
	if not dirname.endswith('/'):
		dirname+='/'
	logging.info("Loading order and order line data from %s" % (dirname))

	# Load order data from CSV
	orders = []
	ordermap = {}	# Map with (w_id, d_id, o_id) as key for fast lookup when processing order lines
	with open(dirname+'order.csv', 'r') as ordercsv:
		orderreader = csv.DictReader(ordercsv, fieldnames=['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID', 'O_CARRIER_ID', 'O_OL_CNT', 'O_ALL_LOCAL', 'O_ENTRY_D'])
		for i, o in enumerate(orderreader):
			o['O_W_ID'] = int(float(o['O_W_ID']))
			o['O_D_ID'] = int(float(o['O_D_ID']))
			o['O_ID'] = int(float(o['O_ID']))
			o['O_C_ID'] = int(float(o['O_C_ID']))
			o['O_CARRIER_ID'] = int(float(o['O_CARRIER_ID'])) if not o['O_CARRIER_ID']=='null' else -1
			o['O_OL_CNT'] = int(float(o['O_OL_CNT']))
			o['O_ALL_LOCAL'] = bool(o['O_ALL_LOCAL'])
			o['O_ENTRY_D'] = int(time.mktime(datetime.strptime(o['O_ENTRY_D'][:19], '%Y-%m-%d %H:%M:%S').timetuple()))
			o['O_O_LINES'] = []
			orders.append(o)
			ordermap[(o['O_W_ID'], o['O_D_ID'], o['O_ID'])] = i

	# Load order line data from CSV
	with open(dirname+'order-line.csv', 'r') as olcsv:
		olreader = csv.DictReader(olcsv, fieldnames=['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_DELIVERY_D', 'OL_AMOUNT', 'OL_SUPPLY_W_ID', 'OL_QUANTITY', 'OL_DISTRICT_INFO'])
		for ol in olreader:
			ol['OL_W_ID'] = int(float(ol['OL_W_ID']))
			ol['OL_D_ID'] = int(float(ol['OL_D_ID']))
			ol['OL_O_ID'] = int(float(ol['OL_O_ID']))
			ol['OL_NUMBER'] = int(float(ol['OL_NUMBER']))
			ol['OL_I_ID'] = int(float(ol['OL_I_ID']))
			ol['OL_DELIVERY_D'] = int(time.mktime(datetime.strptime(ol['OL_DELIVERY_D'][:19], '%Y-%m-%d %H:%M:%S').timetuple())) if ol['OL_DELIVERY_D']!='null' else -1
			ol['OL_AMOUNT'] = float(ol['OL_AMOUNT'])
			ol['OL_SUPPLY_W_ID'] = int(float(ol['OL_SUPPLY_W_ID']))
			ol['OL_QUANTITY'] = float(ol['OL_QUANTITY'])
			orderidx = ordermap.get((ol['OL_W_ID'], ol['OL_D_ID'], ol['OL_O_ID']))
			if orderidx is None:
				continue
			orders[orderidx]['O_O_LINES'].append(udt.OrderLine(ol['OL_I_ID'], ol['OL_DELIVERY_D'], ol['OL_AMOUNT'], ol['OL_SUPPLY_W_ID'], ol['OL_QUANTITY'], ol['OL_DISTRICT_INFO']))

	# Populate DB
	cluster.register_user_type(KEYSPACE, 'order_line', udt.OrderLine)
	session = cluster.connect(KEYSPACE)
	insert_order = session.prepare('INSERT INTO "order" (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d, o_o_lines) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL)
	for (w_id, d_id), orderlist in groupby(sorted(orders, key=lambda o: (o['O_W_ID'], o['O_D_ID'])), lambda o: (o['O_W_ID'], o['O_D_ID'])):
		batchsz = 0
		for o in orderlist:
			batch.add(insert_order, [w_id, d_id, o['O_ID'], o['O_C_ID'], o['O_CARRIER_ID'], o['O_OL_CNT'], o['O_ALL_LOCAL'], o['O_ENTRY_D'], o['O_O_LINES']])
			batchsz+=1
			if batchsz >= 1500:
				session.execute(batch)
				batch.clear()
				batchsz = 0
		if batchsz > 0:
			session.execute(batch)
			batch.clear()
	session.shutdown()

# @param dirname Directory path where "stock.csv" is stored
# @param cluster The cluster object used to connect to database
def populateStocks(dirname, cluster):
	if not dirname.endswith('/'):
		dirname+='/'
	logging.info("Loading stock data from %s" % (dirname))

	# Load stock data from CSV
	stocks = []
	with open(dirname+'stock.csv', 'r') as stockcsv:
		stockreader = csv.DictReader(stockcsv, fieldnames=['S_W_ID', 'S_I_ID', 'S_QUANTITY', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_DATA'])
		for s in stockreader:
			s['S_W_ID'] = int(float(s['S_W_ID']))
			s['S_I_ID'] = int(float(s['S_I_ID']))
			s['S_QUANTITY'] = int(float(s['S_QUANTITY']))
			s['S_YTD'] = float(s['S_YTD'])
			s['S_ORDER_CNT'] = int(float(s['S_ORDER_CNT']))
			s['S_REMOTE_CNT'] = int(float(s['S_REMOTE_CNT']))
			stocks.append(s)

	# Populate DB
	pricemap = {}
	namemap = {}
	session = cluster.connect(KEYSPACE)
	iprice_rows = session.execute(SimpleStatement('SELECT i_id, i_price, i_name FROM item'))
	for row in iprice_rows:
		pricemap[row.i_id] = row.i_price
		namemap [row.i_id] = row.i_name

	insert_stock = session.prepare('INSERT INTO stock (s_w_id, s_i_id, s_price, s_name, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
	batch = BatchStatement(consistency_level=CONSISTENCY_LEVEL)
	for w_id, slist in groupby(sorted(stocks, key=lambda s: s['S_W_ID']), lambda s: s['S_W_ID']):
		batchsz = 0
		for s in slist:
			price = pricemap.get(s['S_I_ID'])
			name = namemap.get(s['S_I_ID'])
			batch.add(insert_stock, [w_id, s['S_I_ID'], price, name, s['S_QUANTITY'], s['S_YTD'], s['S_ORDER_CNT'], s['S_REMOTE_CNT'], s['S_DIST_01'], s['S_DIST_02'], s['S_DIST_03'], s['S_DIST_04'], s['S_DIST_05'], s['S_DIST_06'], s['S_DIST_07'], s['S_DIST_08'], s['S_DIST_09'], s['S_DIST_10'], s['S_DATA']])
			batchsz+=1
			if batchsz >= 1500:
				session.execute(batch)
				batch.clear()
				batchsz = 0
		if batchsz > 0:
			session.execute(batch)
			batch.clear()
	session.shutdown()

if __name__ == "__main__":
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=logging.INFO)
	if len(sys.argv)<2:
		logging.info("Please specify a data input folder")
		exit()

	dirname = sys.argv[1]

	cluster = Cluster()

	cleanDb(cluster)
	populateWarehouses(dirname, cluster)
	populateDistricts(dirname, cluster)
	populateItems(dirname, cluster)
	populateStocks(dirname, cluster)
	populateCustomers(dirname, cluster)
	populateOrders(dirname, cluster)
	
	cluster.shutdown()
