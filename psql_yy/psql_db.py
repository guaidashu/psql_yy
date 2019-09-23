"""
Create by yy on 2019/9/21
"""
import psycopg2

from psql_yy.libs.tool import Tool
import warnings


class _PsqlDB(object):
    """
    Remembers configuration for the (db, helper) tuple.
    """

    def __init__(self, db):
        self.db = db
        self.connectors = {}


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
            self.init_helper(helper)

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
        close the database connection
        :return:
        """
        try:
            self.db.close()
        except Exception as e:
            if self.is_debug_close:
                print(e)

    def getCursor(self):
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
        create a database connection
        :return:
        """
        self.db = psycopg2.connect(database=self.database, user=self.username, password=self.password, host=self.host,
                                   port=self.port)

    def init_helper(self, helper):
        """This callback can be used to initialize an Helper application for the
        use with this database setup.  Never use a database in the context
        of an application not initialized that way or connections will
        leak.
        """
        if 'POSTGRESQL_CONFIG' not in helper.config:
            warnings.warn('Please set postgresql connect info.')
            return self
        helper.psql = _PsqlDB(self)

    def free(self, sql, is_close_db=True):
        self.cursor = self.getCursor()
        data = 1
        try:
            self.cursor.execute(sql)
            if sql.lower().find("select ") != -1:
                data = self.cursor.fetchall()
        except Exception as e:
            if self.is_debug:
                self.tool.debug("原生语句执行出错，报错信息：")
                data = 0
                self.tool.debug(e)
        finally:
            if is_close_db:
                self.close()
        return data
