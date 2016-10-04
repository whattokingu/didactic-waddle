CREATE TYPE IF NOT EXISTS cs4224.address (
street_1 text,
street_2 text,
city text,
state text,
zipcode text
);

CREATE TYPE IF NOT EXISTS cs4224.order_line (
ol_i_id int,
ol_delivery_d timestamp,
ol_amount decimal,
ol_supply_w_id int,
ol_quantity decimal,
ol_dist_info varchar
);

CREATE TABLE IF NOT EXISTS cs4224.warehouse (
w_id int PRIMARY KEY,
w_name text,
w_address frozen<address>,
w_tax decimal,
w_ytd decimal
);

CREATE TABLE IF NOT EXISTS cs4224.district (
d_w_id int,
d_id int,
d_name text,
d_address frozen<address>,
d_tax decimal,
d_w_tax decimal,
d_ytd decimal,
d_next_o_id int,
primary key ((d_w_id, d_id))
);

CREATE TABLE IF NOT EXISTS cs4224.customer (
c_w_id int,
c_d_id int,
c_id int,
c_first text,
c_middle text,
c_last text,
c_address frozen<address>,
c_phone text,
c_since timestamp,
c_credit text,
c_credit_lim decimal,
c_discount decimal,
c_ytd_payment decimal,
c_balance decimal,
c_payment_cnt int,
c_delivery_cnt int,
c_data text,
primary key ((c_w_id, c_d_id), c_id, c_balance)
);

CREATE TABLE IF NOT EXISTS cs4224.item (
i_id int primary key,
i_name text,
i_price decimal,
i_im_id int,
i_data text
);

CREATE TABLE IF NOT EXISTS cs4224."order" (
o_w_id int,
o_d_id int,
o_id int,
o_c_id int,
o_carrier_id int,
o_ol_cnt int,
o_all_local boolean,
o_entry_d timestamp,
o_o_lines list<frozen<order_line>>,
primary key ((o_w_id, o_d_id), o_id))
with clustering order by (o_id desc);

CREATE INDEX order_o_c_id on cs4224."order" (o_c_id);

CREATE TABLE IF NOT EXISTS cs4224.stock (
s_w_id int,
s_i_id int,
s_price decimal,
s_name text,
s_quantity int,
s_ytd decimal,
s_order_cnt int,
s_remote_cnt int,
s_dist_01 text,
s_dist_02 text,
s_dist_03 text,
s_dist_04 text,
s_dist_05 text,
s_dist_06 text,
s_dist_07 text,
s_dist_08 text,
s_dist_09 text,
s_dist_10 text,
s_data text,
primary key (s_w_id, s_i_id)
);
