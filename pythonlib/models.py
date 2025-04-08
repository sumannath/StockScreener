import datetime

from peewee import *

from db import DatabaseSingleton


class BaseModel(Model):
    class Meta:
        database = DatabaseSingleton.get_db_instance()
        legacy_table_names = False


class StockMaster(BaseModel):
    symbol = CharField(primary_key=True, max_length=12)
    name = CharField(max_length=100)
    series = CharField(max_length=5)
    date_of_listing = DateField(null=True)
    paid_up_value = DecimalField(max_digits=25, decimal_places=6)
    market_lot = IntegerField()
    isin_number = CharField(max_length=12, unique=True)
    face_value = DecimalField(max_digits=25, decimal_places=6)
    ts_created = DateTimeField(default=datetime.datetime.now)
    ts_updated = DateTimeField(default=datetime.datetime.now)
    ts_deleted = DateTimeField(null=True)


class StockPrice(BaseModel):
    trading_date = DateField()
    biz_date = DateField()
    segment = CharField(max_length=5)
    source = CharField(max_length=10)
    fin_instr_type = CharField(max_length=5)
    fin_instr_id = IntegerField()
    isin_number = CharField(max_length=12)
    symbol = CharField(max_length=12)
    instr_series = CharField(max_length=4)
    instr_expiry_date = DateField(null=True)
    instr_actual_expiry_date = DateField(null=True)
    instr_strike_price = DecimalField(max_digits=25, decimal_places=6, null=True)
    instr_option_type = CharField(max_length=2, null=True)
    instr_name = CharField(max_length=50)
    open_price = DecimalField(max_digits=25, decimal_places=2)
    high_price = DecimalField(max_digits=25, decimal_places=2)
    low_price = DecimalField(max_digits=25, decimal_places=2)
    close_price = DecimalField(max_digits=25, decimal_places=2)
    last_traded_price = DecimalField(max_digits=25, decimal_places=2)
    prev_close_price = DecimalField(max_digits=25, decimal_places=2)
    underlying_asset_price = DecimalField(max_digits=25, decimal_places=2, null=True)
    settlement_price = DecimalField(max_digits=25, decimal_places=2)
    open_interest = DecimalField(max_digits=25, decimal_places=2, null=True)
    change_in_open_interest = DecimalField(max_digits=25, decimal_places=2, null=True)
    total_traded_volume = IntegerField()
    total_traded_value = DecimalField(max_digits=25, decimal_places=2)
    total_traded_txn = IntegerField()
    session_id = CharField(max_length=20)
    market_lot_size = IntegerField()
    remarks = CharField(max_length=150, null=True)
    ts_created = DateTimeField(default=datetime.datetime.now)
    ts_updated = DateTimeField(default=datetime.datetime.now)

    class Meta:
        primary_key = CompositeKey('trading_date', 'symbol', 'instr_series')
