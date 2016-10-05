import sys
import csv
import threading
import time
import traceback
import logging
from decimal import Decimal
from datetime import datetime
from cassandra.cluster import Cluster
from dbconf import KEYSPACE, CONSISTENCY_LEVEL, LOGGING_LEVEL

from newOrder import newOrder
from custPayment import custPayment
from delivery import delivery
from order_status import orderStatus
from stockLevel import stockLevel
from popular_item import popularItems
from topbalance2 import topbalance

# @params dirname where the xact files reside
# @params file filename(should be an int) without extension
# @params cluster: an instance of the cluster
def readTransactions(dirname, file, cluster):
	timeStart = time.time()
	xactCount = 0
	session = cluster.connect(KEYSPACE)
	file = dirname + str(file) + '.txt'
	logger.info("reading from: " + str(file) + ".txt")
	try:
		with open(file, 'r') as xactfile:
			xactreader = itewrapper(csv.reader(xactfile, delimiter=','))
			while xactreader.hasnext():
				line = xactreader.next()
				xactType = line[0]
				logger.info("processing xactType:" +xactType)
				if xactType == 'N':
					handleNewOrder(line, xactreader, session)
				elif xactType == 'P':
					handlePayment(line, xactreader, session)
				elif xactType == 'D':
					handleDelivery(line, xactreader, session)
				elif xactType == 'O':
					handleOrderStatus(line, xactreader, session)
				elif xactType == 'S':
					handleStockStatus(line, xactreader, session)
				elif xactType == 'I':
					handlePopularItem(line, xactreader, session)
				elif xactType == 'T':
					handleTopBalance(line, xactreader, session)
				else:
					logger.warning("something went wrong.")
				xactCount+=1
	except Exception as e:
		logger.error("An Error has occured:")
		logger.error(e)
		exc_type, exc_value, exc_traceback = sys.exc_info()
		traceback.print_exc()
		timeEnd = time.time()
		return "transaction Stats: {0} transactions in {1} ms".format(xactCount, (timeEnd - timeStart)*1000.0)
	timeEnd = time.time()
	return "transaction Stats: {0} transactions in {1} ms".format(xactCount, (timeEnd - timeStart)*1000.0)


def handleNewOrder(line, xactreader, session):
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
	newOrder(custId, numItems, itemId, supplyWarehouse, qty, session)

def handlePayment(line, xactreader, session):
	custId = dict()
	custId['w_id'] = int(line[1])
	custId['d_id'] = int(line[2])
	custId['c_id'] = int(line[3])
	custPayment(custId, Decimal(line[4]), session)
def handleDelivery(line, xactreader, session):
	delivery(int(line[1]), int(line[2]), session)
def handleOrderStatus(line, xactreader, session):
	orderStatus(int(line[1]), int(line[2]), int(line[3]), session)
def handleStockStatus(line, xactreader, session):
	stockLevel(int(line[1]), int(line[2]), int(line[3]), int(line[4]), session)
def handlePopularItem(line, xactreader, session):
	popularItems(int(line[1]), int(line[2]), int(line[3]), session)
def handleTopBalance(line, xactreader, session):
	topbalance(session)

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
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=LOGGING_LEVEL)
	if len(sys.argv)<4:
		logger.info("Please specify a transaction input folder")
		logger.info("please specify client number, e.g., 2")
		logger.info("e.g. python driver.py <folder> <fileNum> <logfile>")
		exit()
	dirname = sys.argv[1]
	clientNum = int(sys.argv[2])
	logfile=sys.argv[3]
	logger.info("processing transactions: ")

	if not dirname.endswith("/"):
		dirname+="/"	
	cluster = Cluster()
	fh = logging.FileHandler(logfile)
	fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
	logger.addHandler(fh)
	logger.setLevel(logging.INFO)
	logger.info("Client No. " + str(clientNum) + ": starting transactions")
	logger.setLevel(LOGGING_LEVEL)
	msg = readTransactions(dirname, clientNum , cluster)	
	logger.setLevel(logging.INFO)
	logger.info("Client No. " + str(clientNum) + ": " + msg)
	logger.info("Client No. " + str(clientNum) + ": ending transactions")
	logger.setLevel(LOGGING_LEVEL)
	exit()


