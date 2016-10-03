import sys
import csv
import threading
import time
import traceback
from decimal import Decimal
from datetime import datetime
from cassandra.cluster import Cluster
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, MULTI_CLUSTER, CLUSTER_ADDRESSES

from newOrder import newOrder
from custPayment import custPayment
from delivery import delivery
from order_status import orderStatus
from stockLevel import stockLevel
from popular_item import popularItems

# @params dirname where the xact files reside
# @params file filename(should be an int) without extension
# @params cluster: an instance of the cluster
def readTransactions(dirname, file, cluster):
	timeStart = time.time()
	xactCount = 0
	try:
		with open(dirname+ str(file) + '.txt', 'r') as xactfile:
			print "reading from: " + str(file) + ".txt"
			xactreader = itewrapper(csv.reader(xactfile, delimiter=','))
			while xactreader.hasnext():
				line = xactreader.next()
				xactType = line[0]
				print "processing xactType:" +xactType
				if xactType == 'N':
					handleNewOrder(line, xactreader, cluster)
				elif xactType == 'P':
					handlePayment(line, xactreader, cluster)
				elif xactType == 'D':
					handleDelivery(line, xactreader, cluster)
				elif xactType == 'O':
					handleOrderStatus(line, xactreader, cluster)
				elif xactType == 'S':
					handleStockStatus(line, xactreader, cluster)
				elif xactType == 'I':
					handlePopularItem(line, xactreader, cluster)
				elif xactType == 'T':
					handleTopBalance(line, xactreader, cluster)
				else:
					print "something went wrong."
				xactCount+=1
	except Exception as e:
		print "An Error has occured:"
		print e
		exc_type, exc_value, exc_traceback = sys.exc_info()
		traceback.print_exc()
		timeEnd = time.time()
		return "transaction Stats: {0} transactions in {1} ms".format(xactCount, (timeEnd - timeStart)*1000.0)
	timeEnd = time.time()
	return "transaction Stats: {0} transactions in {1} ms".format(xactCount, (timeEnd - timeStart)*1000.0)


def handleNewOrder(line, xactreader, cluster):
	custId = dict()
	custId['c_id'] = int(line[1])
	custId['w_id'] = int(line[2])
	custId['d_id'] = int(line[3])
	numItems = int(line[4])
	supplyWarehouse = []
	itemId = []
	qty = []
	for i in range(0, numItems):
		ol_line = xactreader.next()
		itemId.append(int(ol_line[0]))
		supplyWarehouse.append(int(ol_line[1]))
		qty.append(int(ol_line[2]))
	newOrder(custId, numItems, itemId, supplyWarehouse, qty, cluster)

def handlePayment(line, xactreader, cluster):
	custId = dict()
	custId['w_id'] = int(line[1])
	custId['d_id'] = int(line[2])
	custId['c_id'] = int(line[3])
	custPayment(custId, Decimal(line[4]), cluster)

def handleDelivery(line, xactreader, cluster):
	delivery(int(line[1]), int(line[2]), cluster)

def handleOrderStatus(line, xactreader, cluster):
	orderStatus(int(line[1]), int(line[2]), int(line[3]), cluster)
def handleStockStatus(line, xactreader, cluster):
	stockLevel(int(line[1]), int(line[2]), int(line[3]), int(line[4]), cluster)
def handlePopularItem(line, xactreader, cluster):
	popularItems(int(line[1]), int(line[2]), int(line[3]), cluster)
def handleTopBalance(line, xactreader, cluster):
	print "top balance"

class itewrapper(object):
  def __init__(self, it):
    self.it = iter(it)
    self._hasnext = None
  def __iter__(self): return self
  def next(self):
    if self._hasnext:
      result = self._thenext
    else:
      result = next(self.it)
    self._hasnext = None
    return result
  def hasnext(self):
    if self._hasnext is None:
      try: self._thenext = next(self.it)
      except StopIteration: self._hasnext = False
      else: self._hasnext = True
    return self._hasnext



if __name__ == "__main__":
	if len(sys.argv)<3:
		print "Please specify a transaction input folder"
		print "please specify client number, e.g., 2"
		print "e.g. python driver.py <folder> <fileNum>"
		exit()
	dirname = sys.argv[1]
	clientNum = int(sys.argv[2])
	print "processing transactions: "

	if not dirname.endswith("/"):
		dirname+="/"
	cluster = None
	if MULTI_CLUSTER:
		cluster = Cluster(CLUSTE_ADDRESSES[clientNum % 3])
	else:
		cluster = Cluster()
	print "Client No. " + str(clientNum) + ": starting transactions"
	msg = readTransactions(dirname, clientNum , cluster)		
	print "Client No. " + str(clientNum) + ": " + msg
	print "Client No. " + str(clientNum) + ": ending transactions"


