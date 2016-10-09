from cassandra import ConsistencyLevel
import logging
KEYSPACE = 'cs4224'
CONSISTENCY_LEVEL = ConsistencyLevel.ANY
PRINT_OUTPUT = True
LOGGING_LEVEL = logging.WARNING