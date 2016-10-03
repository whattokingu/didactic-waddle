from cassandra import ConsistencyLevel

KEYSPACE = 'cs4224'
CONSISTENCY_LEVEL = ConsistencyLevel.ANY
PRINT_OUTPUT = False
MULTI_CLUSTER = False
CLUSTER_ADDRESSES = {'192.168.48.234', '192.168.48.234', '192.168.48.234'}
