import logging
import mysql.connector
import datetime

# Pre requisit:
# A database with name stockz_db
################################################################################
# Table company_list
#   {id(uint32), symbol(string 16), name(string 100), ipo_year(uint16),
#    sector(string 100) , industry(string 100)
# Table stock_data
#   {date{date}, volume(uint64), open(double64), close(double64), high(double64),
#    low(double64), day_voluem(uint64), pe(double64), eps(double64),
#    revenuePS(double64), google_index(double64)}


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

    def __SymbolWasher(self, symbol):
        ret = symbol.upper()
        ret = ret.replace("^", "_V_")
        ret = ret.replace(".", "_O_")
        ret = ret.replace("~", "_L_")
        ret = ret.replace("$", "_S_")
        return ret

    def __GetIdBySymbol(self, symbol):
        symbol = self.__SymbolWasher(symbol)
        if symbol not in self.symbol_id_map:
            logging.warning('%s not in current databas.' % (symbol))
            print('Warning: %s not in current databas.' % (symbol))
            return None
        return self.symbol_id_map.get(symbol)

    # Return a list of all stock symbol in database
    def ReadStockList(self):
        ret = []
        for sym in self.symbol_id_map:
            ret.append(sym)
        return ret

    def ReadStockInfo(self, symbol):
        cursor = self.conn_.cursor()
        symbol = self.__SymbolWasher(symbol)
        if symbol not in self.symbol_id_map:
            logging.warning('Read invalid stock info')
            return None
        symbol_id = self.__GetIdBySymbol(symbol)
        cursor.execute('SELECT * from company_list where id=%d' % (symbol_id))
        ret = cursor.fetchall()[0]
        self.conn_.commit()
        cursor.close()
        return ret

    # company_info = [name, ipo_year(int), sector, industry]
    def UpdateStockList(self, symbol, company_info):
        symbol = self.__SymbolWasher(symbol)
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
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS stock_%s ( '
                'data_date date not null primary key,'
                'open_p double not null,'
                'close_p double not null,'
                'high_p double not null,'
                'low_p double not null,'
                'volume_p double unsigned not null,'
                'open_adj double,'
                'close_adj double,'
                'high_adj double,'
                'low_adj double,'
                'volume_adj double unsigned,'
                'volume bigint unsigned,'
                'ex_dividend double,'
                'split_ratio double,'
                'pe_ratio double,'
                'eps double,'
                'rps double,'
                'g_heat double )' % (symbol)
            )
        self.conn_.commit()
        cursor.close()

    def ReadStockData(self, symbol):
        symbol = self.__SymbolWasher(symbol)
        cursor = self.conn_.cursor()
        cursor.execute('SELECT * FROM stock_%s' % (symbol))
        ret = cursor.fetchall()
        self.conn_.commit()
        cursor.close()
        return ret

    def ReadStockDataByDate(self, symbol, date):
        symbol = self.__SymbolWasher(symbol)
        cursor = self.conn_.cursor()
        cursor.execute(
            'SELECT * FROM stock_%s where data_date="%s"' % (symbol, date))
        ret = cursor.fetchall()
        self.conn_.commit()
        cursor.close()
        return ret

    def ReadLatestDateBySymbol(self, symbol):
        symbol = self.__SymbolWasher(symbol)
        cursor = self.conn_.cursor()
        cursor.execute('SELECT MAX(data_date) FROM stock_%s' % (symbol))
        ret = cursor.fetchall()
        self.conn_.commit()
        cursor.close()
        return ret[0][0]

    def InsertStockData(
            self, symbol, data_date, open_p="NULL", close_p="NULL", high_p="NULL", low_p="NULL", volume_p="NULL",
            open_adj="NULL", close_adj="NULL", high_adj="NULL", low_adj="NULL",
            volume_adj="NULL", volume="NULL", ex_dividend="NULL", split_ratio="NULL",
            pe_ratio="NULL", eps="NULL", rps="NULL", g_heat="NULL"):
        symbol = self.__SymbolWasher(symbol)
        if symbol not in self.symbol_id_map:
            logging.warning('%s not in current databas.' % (symbol))
            print('Warning: %s not in current databas.' % (symbol))
            return False
        cursor = self.conn_.cursor()
        cursor.execute(
            'INSERT INTO stock_%s ( '
            'data_date, open_p, close_p, high_p, low_p, volume_p, '
            'open_adj, close_adj, high_adj, low_adj, volume_adj,'
            'volume, ex_dividend, split_ratio, pe_ratio, eps, rps, g_heat ) '
            'values("%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)' % (
                symbol, data_date, open_p, close_p, high_p, low_p, volume_p,
                open_adj, close_adj, high_adj, low_adj, volume_adj,
                volume, ex_dividend, split_ratio, pe_ratio, eps, rps, g_heat)
        )
        self.conn_.commit()
        cursor.close()

    def UpdateStockData(
            self, symbol, data_date,
            open_adj=None, close_adj=None, high_adj=None, low_adj=None, volume_adj=None,
            volume=None, ex_dividend=None, split_ratio=None,
            pe_ratio=None, eps=None, rps=None, g_heat=None):
        symbol = self.__SymbolWasher(symbol)
        if symbol not in self.symbol_id_map:
            logging.warning('%s not in current databas.' % (symbol))
            print('Warning: %s not in current databas.' % (symbol))
            return False
        s = ''
        if open_adj:
            s = s + 'open_adj=%s,' % (open_adj)
        if close_adj:
            s = s + 'close_adj=%s,' % (close_adj)
        if high_adj:
            s = s + 'high_adj=%s,' % (high_adj)
        if low_adj:
            s = s + 'low_adj=%s,' % (low_adj)
        if volume_adj:
            s = s + 'volume_adj=%s,' % (volume_adj)
        if volume:
            s = s + 'volume=%s,' % (volume)
        if ex_dividend:
            s = s + 'ex_dividend=%s,' % (ex_dividend)
        if split_ratio:
            s = s + 'split_ratio=%s,' % (split_ratio)
        if pe_ratio:
            s = s + 'pe_ratio=%s,' % (pe_ratio)
        if eps:
            s = s + 'eps=%s,' % (eps)
        if rps:
            s = s + 'rps=%s,' % (rps)
        if g_heat:
            s = s + 'openg_heat_adj=%s,' % (g_heat)
        if s == '':
            return True
        else:
            s = s[:-1]
        cursor = self.conn_.cursor()
        cursor.execute(
            'UPDATE stock_%s SET %s'
            'where date="%s" )' % (symbol, s, data_date)
        )
        self.conn_.commit()
        cursor.close()
