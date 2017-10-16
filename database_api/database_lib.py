import logging
import mysql.connector

# Pre requisit:
# A database with name stockz_db
#     Table company_list
#     {ID(uint32), Symbol(string 16), Name(string 100), IPOyear(uint16),
#      Sector(string 100) , Industry(string 100)
################################################################################
# Table stock_data
# {Date{date}, Volume(uint64), Open(double64), Close(double64), High(double64),
#  Low(double64), DayVoluem(uint64), PE(double64), EPS(double64), RevenuePS(double64)}


class StockzDatabaseClient:
    def __init__(self):
        self.conn_ = mysql.connector.connect(
            user='root', password='fdsajkl;', database='stockz_db')
        self.symbol_id_map = {}
        self.__InitSymbolIdMap()

    def __del__(self):
        status = True
        status &= self.conn_.close()
        if not status:
            logging.warning('StockzDatabaseClient close failed')

    def __InitSymbolIdMap(self):
        cursor = self.conn_.cursor()
        #
        cursor.commit()
        cursor.close()

    def __GetIdBySymbol(self, symbol):
        return self.symbol_id_map.get(symbol.upper())

    # Return a list of all stock symbol in database
    def ReadStockList(self):
        cursor = self.conn_.cursor()
        cursor.execute('SELECT * FROM company_list')
        values = cursor.fetchall()
        cursor.commit()
        cursor.close()
        return valuse

    def ReadStockInfo(self, symbol):
        cursor = self.conn_.cursor()
        #
        cursor.commit()
        cursor.close()

    def UpdateStockList(self, symbol, company_info):
        cursor = self.conn_.cursor()
        cursor.execute('INSERT INTO company_list (id, name) values (%s, %s)', [
                       '1', 'Michael'])
        cursor.execute(
            'CREATE TABLE sd_%s (id varchar(20) primary key, name varchar(20))', [symbol])
        cursor.commit()
        cursor.close()

    def ReadStockData(self, symbol):
        cursor = self.conn_.cursor()
        #
        cursor.commit()
        cursor.close()

    def ReadStockDataByDate(self, symbol, date):
        cursor = self.conn_.cursor()
        #
        cursor.commit()
        cursor.close()

    def UpdateStockData(self, symbol, data):
        cursor = self.conn_.cursor()
        #
        cursor.commit()
        cursor.close()
