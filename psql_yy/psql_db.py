"""
Create by yy on 2019/9/21
I'm looking for a good job, can you provide for me?
Please contact me by email 1023767856@qq.com.
"""
import re

import psycopg2

from .lib.tool import Tool
import warnings


# class _PsqlDB(object):
#     """
#     Remembers configuration for the (db, helper) tuple.
#     """
#
#     def __init__(self, db):
#         self.db = db
#         self.connectors = {}


class PsqlDB(object):
    """
    Real postgresql connection class
    """

    def __init__(self, helper=None, **kwargs):
        """
        初始化
        Initialize
        :param helper:
        :param kwargs:
        """
        self.is_debug = True
        self.is_connection = False
        self.db = None
        self.is_debug_close = False
        self.cursor = None
        self.tool = Tool()
        # 初始化方法允许用户单独定义要访问的数据库
        self.username = kwargs.setdefault("username", "postgres")
        self.password = kwargs.setdefault("password", "root")
        self.host = kwargs.setdefault("host", "127.0.0.1")
        self.port = kwargs.setdefault("port", "5432")
        self.database = kwargs.setdefault("database", "postgres")
        self.table_prefix = kwargs.setdefault("table_prefix", "")
        if helper is not None:
            self.helper = helper
            self.init_helper(helper)
        else:
            self.helper = None

    def __del__(self):
        """
        默认关闭数据库，防止使用者忘记关闭造成资源泄露等问题
        Default close the connection with the postgresql
        prevent user from forgetting to close connection which
        maybe cause problems such as resource leaks.
        :return:
        """
        self.close()

    def close(self):
        """
        关闭数据库
        Close the database connection
        :return:
        """
        try:
            self.db.close()
        except Exception as e:
            if self.is_debug_close:
                print(e)

    def get_cursor(self):
        try:
            if not self.is_connection:
                self.init()
                self.is_connection = True
        except Exception as e:
            if self.is_debug:
                self.tool.debug("数据库连接失败：")
                self.tool.debug(e)
        return self.db.cursor()

    def init(self):
        """
        创建连接
        Create a database connection
        :return:
        """
        self.db = psycopg2.connect(database=self.database, user=self.username, password=self.password, host=self.host,
                                   port=self.port)

    def get_config(self, config):
        """
        获取配置
        Get config from config dict.
        The config example:
            POSTGRESQL_CONFIG = {
                "USERNAME": "postgres",
                "PASSWORD": "root",
                "HOST": "127.0.0.1",
                "PORT": "5432",
                "DATABASE": "postgres",
                "TABLE_PREFIX": ""
            }
        :param config:
        :return:
        """
        self.username = self.helper.config[config].setdefault("USERNAME", "postgres")
        self.password = self.helper.config[config].setdefault("PASSWORD", "root")
        self.host = self.helper.config[config].setdefault("HOST", "127.0.0.1")
        self.port = self.helper.config[config].setdefault("PORT", "5432")
        self.database = self.helper.config[config].setdefault("DATABASE", "postgres")
        self.table_prefix = self.helper.config[config].setdefault("TABLE_PREFIX", "")

    def init_db(self, config):
        """
        If you want to use this method,
        you must call function init_helper,
        otherwise it will give a warning.
        :param config:
        :return:
        """
        if self.helper is None:
            warnings.warn('Please call function init_helper first.')
            return None
        if config not in self.helper.config:
            warnings.warn('Please set postgresql connect info.')
            return None
        self.get_config(config)
        return self

    def __call__(self, *args, **kwargs):
        if len(args) < 1:
            warnings.warn('Please give a config name.')
            return self
        return self.init_db(config=args[0])

    def init_helper(self, helper, init_config=True):
        """This callback can be used to initialize an Helper application for the
        use with this database setup.  Never use a database in the context
        of an application not initialized that way or connections will
        leak.
        """
        self.helper = helper
        default_config = 'POSTGRESQL_CONFIG'
        if init_config:
            if default_config not in helper.config:
                warnings.warn('Please set postgresql connect info.')
                return self
            self.get_config(default_config)
        helper.psql = self

    def select(self, data, get_all=True, is_close_db=True):
        """
        查询方法
        Select method.
        :param data:
        :param get_all:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        data['table'] = self.table_prefix + data['table']
        sql = self.get_select_sql(data)
        try:
            unused = data['columns'][0]
            if data['columns'][0] == "*":
                try:
                    columns_data = self.get_columns(data['table'])
                except:
                    return {"error": "字段信息获取失败"}
            else:
                columns_data = tuple()
                for v in data['columns']:
                    tmp_tuple = ((v,),)
                    columns_data = columns_data + tmp_tuple
        except:
            try:
                columns_data = self.get_columns(data['table'])
            except:
                return {"error": "字段信息获取失败"}
        try:
            self.cursor.execute(sql)
            if get_all:
                results = self.cursor.fetchall()
            else:
                results = self.cursor.fetchone()
        except:
            results = {"error": "数据获取失败"}
        if is_close_db:
            self.close()
        if get_all:
            results_final = list()
            for v in results:
                length = len(v)
                content = dict()
                for k in range(length):
                    content[columns_data[k][0]] = v[k]
                results_final.append(content)
        else:
            results_final = dict()
            try:
                length = len(results)
            except:
                length = 0
            for k in range(length):
                results_final[columns_data[k][0]] = results[k]
        return results_final

    def insert(self, data, is_close_db=True):
        """
        数据存储
        Insert data
        :param data:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        sql = self.get_insert_sql(data)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            results = 1
        except Exception as e:
            self.db.rollback()
            self.tool.debug(e)
            self.tool.debug("Database insert error")
            results = 0
        if is_close_db:
            self.close()
        return results

    def insert_last_id(self, data, return_columns="id", is_close_db=True):
        """
        数据存储并返回此次的 自增id
        Insert data and it will return a id which insert now.
        :param data:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        sql = self.get_insert_sql(data)
        sql = sql + " RETURNING {id}".format(id=return_columns)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            results = self.cursor.fetchone()[0]
        except Exception as e:
            self.db.rollback()
            self.tool.debug(e)
            self.tool.debug("Database insert error")
            results = 0
        if is_close_db:
            self.close()
        return results

    def update(self, data, is_close_db=True):
        """
        更新数据
        Update data
        :param data:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        sql = self.get_update_sql(data)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            results = 1
        except:
            self.db.rollback()
            self.tool.debug("Database update error")
            results = 0
        if is_close_db:
            self.close()
        return results

    def delete(self, data, is_close_db=True):
        """
        删除数据
        Delete data
        :param data:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        sql = self.get_delete_sql(data)
        try:
            self.cursor.execute(sql)
            self.db.commit()
            results = 1
        except:
            self.db.rollback()
            self.tool.debug("Database delete error")
            results = 0
        if is_close_db:
            self.close()
        return results

    def get_select_sql(self, data, is_close_db=False):
        """
        构造 查询sql语句
        Construct query sql statement
        :param data:
        :param is_close_db:
        :return:
        """
        sql = "select "
        s = ""
        # spell condition
        try:
            for i in data['columns']:
                if i != data['columns'][-1]:
                    s = s + i + ","
                else:
                    s = s + i
        except:
            s = "*"
        sql = sql + s + " from " + data['table']
        # if there is a condition , we spell it
        try:
            unused = data['condition']
            sql = sql + " where "
            s = ""
            for i in data['condition']:
                s = s + i + " "
            sql = sql + s
        except:
            pass
        # if there is a order need , we spell it
        try:
            unused = data['order']
            sql = sql + " order by " + data['order'][0] + " " + data['order'][1]
        except:
            pass
        # if there is limit. we spell it
        try:
            unused = data['limit']
            sql = sql + " limit " + str(data['limit'][0]) + "," + str(data['limit'][1])
        except:
            pass
        if is_close_db:
            self.close()
        return sql

    def get_update_sql(self, data, is_close_db=False):
        """
        构造更新sql语句
        Construct update sql statement
        :param data:  data['set'] is a dict  data['condition'] is a list
        :param is_close_db:
        :return:
        """
        sql = "update "
        sql = sql + self.table_prefix + data['table'] + ' set '
        try:
            unused = data['set']
            s = ""
            m = 0
            for k, i in data['set'].items():
                if m == 0:
                    s = s + k + "=" + self.tool.escape(i)
                    m = m + 1
                else:
                    s = s + "," + k + "=" + self.tool.escape(i)
            sql = sql + s
        except:
            pass
        # if there is a condition , we spell it
        try:
            unused = data['condition']
            sql = sql + " where "
            s = ""
            for i in data['condition']:
                s = s + i + " "
            sql = sql + s
        except:
            pass
        if is_close_db:
            self.close()
        return sql

    def get_delete_sql(self, data):
        """
        构造删除sql语句
        Construct delete sql statement
        :param data:
        :return:
        """
        sql = 'delete'
        data['table'] = self.table_prefix + data['table']
        sql = sql + " from " + data['table']
        try:
            unused = data['condition']
            sql = sql + " where "
            s = ""
            for i in data['condition']:
                s = s + i + " "
            sql = sql + s
        except Exception as e:
            self.tool.debug("condition is err, err info: {error}".format(error=e))
        return sql

    def get_columns(self, table, num=1, is_close_db=False):
        """
        根据表名 获取对应表的字段信息
        Get columns according to table name
        :param table:
        :param num:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        sql = """
                select a.attname                                                       as rowname,
                format_type(a.atttypid, a.atttypmod)                            as type,
                d.adsrc                                                         as default
                from pg_class c,
                pg_attribute a
                 left join (select a.attname, ad.adsrc
                            from pg_class c,
                                 pg_attribute a,
                                 pg_attrdef ad
                            where relname = '{table_name}'
                              and ad.adrelid = c.oid
                              and adnum = a.attnum
                              and attrelid = c.oid) as d on a.attname = d.attname
                where c.relname = '{table_name}'
                and a.attrelid = c.oid
                and a.attnum > 0;
                """.format(table_name=self.table_prefix + table)
        try:
            self.cursor.execute(sql)
            if num == 1:
                results = self.cursor.fetchall()
            else:
                results = self.cursor.fetchone()
        except:
            results = 0
            self.tool.debug("It's error that get table columns")
        if is_close_db:
            self.close()
        return results

    def get_insert_sql(self, data, is_close_db=False, table_columns=False):
        """
        构造 查询sql语句
        Construct insert sql statement
        :param data:
        :param is_close_db:
        :param table_columns:
        :return:
        """
        # 构造插入查询语句，此函数传入参数data必须为dict()类型
        table = data['table']
        del data['table']
        s = "insert into " + self.table_prefix + table + "("
        columns = ""
        value = ""
        # get table's columns name
        if not table_columns:
            table_columns = self.get_columns(table)
        table_columns_dict = dict()
        str_dict = {"text": "text", "varchar": "varchar", "longtext": "longtext", "datetime": "datetime",
                    "char": "char", "character varying": "varchar", "character": "character"}
        try:
            for v in table_columns:
                if v[2] is not None and len(re.findall("nextval", v[2])) > 0:
                    continue
                table_columns_dict[v[0]] = v[1]
        except Exception as e:
            self.tool.debug(e)
        length = len(data)
        i = 1
        for k, v in data.items():
            if i != length:
                columns = columns + k + ","
                tmp_str = "%s," % self.tool.escape(str(v))
                value = value + tmp_str
            else:
                columns = columns + k
                tmp_str = "%s" % self.tool.escape(str(v))
                value = value + tmp_str
            if k in table_columns_dict:
                del table_columns_dict[k]
            i = i + 1
        for k, v in table_columns_dict.items():
            columns = columns + "," + str(k)
            table_columns_dict[k] = re.sub("\([\d]*.\)", "", table_columns_dict[k])
            if table_columns_dict[k] in str_dict:
                value = value + ",''"
            else:
                value = value + "," + "0"
        data = s + columns + ")values(" + value + ")"
        if is_close_db:
            self.close()
        return data

    def free(self, sql, is_close_db=True):
        """
        直接执行sql语句
        Execute sql statement.
        :param sql:
        :param is_close_db:
        :return:
        """
        self.cursor = self.get_cursor()
        data = 1
        try:
            self.cursor.execute(sql)
            if sql.lower().find("select ") != -1:
                data = self.cursor.fetchall()
        except Exception as e:
            self.db.rollback()
            if self.is_debug:
                self.tool.debug("原生语句执行出错，报错信息：")
                data = 0
                self.tool.debug(e)
        finally:
            if is_close_db:
                self.close()
        return data
