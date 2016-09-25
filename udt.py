# Defines the address UDT for Cassandra
class Address(object):
	def __init__(self, street_1, street_2, city, state, zipcode):
		self._street_1 = street_1
		self._street_2 = street_2
		self._city = city
		self._state = state
		self._zipcode = zipcode

	@property
	def street_1(self):
		return self._street_1

	@street_1.setter
	def street_1(self, new_street_1):
		self._street_1 = new_street_1

	@property
	def street_2(self):
		return self._street_2

	@street_2.setter
	def street_2(self, new_street_2):
		self._street_2 = new_street_2

	@property
	def city(self):
		return self._city

	@city.setter
	def city(self, new_city):
		self._city = new_city

	@property
	def state(self):
		return self._state

	@state.setter
	def state(self, new_state):
		self._state = new_state

	@property
	def zipcode(self):
		return self._zipcode

	@zipcode.setter
	def zipcode(self, new_zipcode):
		self._zipcode = new_zipcode

# Defines an order line item UDT for Cassandra
class OrderLine(object):
	def __init__(self, i_id, delivery_date, amount, supply_w_id, quantity, dist_info):
		self._i_id = i_id
		self._delivery_date = delivery_date
		self._amount = amount
		self._supply_w_id = supply_w_id
		self._quantity = quantity
		self._dist_info = dist_info

	@property
	def ol_i_id(self):
		return self._i_id

	@ol_i_id.setter
	def ol_i_id(self, i_id):
		self._i_id = i_id

	@property
	def ol_delivery_d(self):
		return self._delivery_date

	@ol_delivery_d.setter
	def ol_delivery_d(self, delivery_date):
		self._delivery_date = delivery_date

	@property
	def ol_amount(self):
		return self._amount

	@ol_amount.setter
	def ol_amount(self, amount):
		self._amount = amount

	@property
	def ol_supply_w_id(self):
		return self._supply_w_id

	@ol_supply_w_id.setter
	def ol_supply_w_id(self, supply_w_id):
		self._supply_w_id = supply_w_id

	@property
	def ol_quantity(self):
		return self._quantity

	@ol_quantity.setter
	def ol_quantity(self, quantity):
		self._quantity = quantity

	@property
	def ol_dist_info(self):
		return self._dist_info

	@ol_dist_info.setter
	def ol_dist_info(self, dist_info):
		self._dist_info = dist_info
