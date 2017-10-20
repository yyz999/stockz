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
        try:
            self.conn_.close()
        except:
            logging.warning('StockzDatabaseClient close failed')

    def __InitSymbolIdMap(self):
        cursor = self.conn_.cursor()
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS company_list ( '
            'id int unsigned not null auto_increment primary key,'
            'symbol char(16) not null,'
            'name char(200) not null,'
            'ipo_year int unsigned,'
            'sector char(200),'
            'industry char(200) )')
        cursor.execute('SELECT id,symbol FROM company_list')
        for (db_id, symbol) in cursor:
            self.symbol_id_map[symbol] = db_id
        cursor.close()

    def __GetIdBySymbol(self, symbol):
        return self.symbol_id_map.get(symbol.upper())

    # Return a list of all stock symbol in database
    def ReadStockList(self):
        ret = []
        for sym in self.symbol_id_map:
            ret.append(sym)
        return ret

    def ReadStockInfo(self, symbol):
        cursor = self.conn_.cursor()
        #
        cursor.close()

    # company_info = [name, ipo_year(int), sector, industry]
    def UpdateStockList(self, symbol, company_info):
        symbol = symbol.upper()
        cursor = self.conn_.cursor()
        if symbol in self.symbol_id_map:
            cursor.execute(
                'UPDATE company_list SET name="%s",ipo_year=%d,sector="%s",industry="%s" WHERE id=%d' % (
                    company_info[0], company_info[1], company_info[2], company_info[3], self.__GetIdBySymbol(symbol)))
        else:
            cursor.execute(
                'INSERT INTO company_list values (NULL, "%s", "%s", %d, "%s", "%s" )' % (
                    symbol, company_info[0], company_info[1], company_info[2], company_info[3]))
            cursor.execute(
                'SELECT id FROM company_list where symbol="%s"' % (symbol))
            self.symbol_id_map[symbol] = cursor.fetchall()[0][0]
        self.conn_.commit()
        cursor.close()

    def ReadStockData(self, symbol):
        symbol = symbol.upper()
        cursor = self.conn_.cursor()
        #
        cursor.close()

    def ReadStockDataByDate(self, symbol, date):
        symbol = symbol.upper()
        cursor = self.conn_.cursor()
        #
        cursor.close()

    def UpdateStockData(self, symbol, data):
        symbol = symbol.upper()
        cursor = self.conn_.cursor()
        #
        cursor.close()
