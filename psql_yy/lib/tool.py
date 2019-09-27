"""
Create by yy on 2019/9/22
"""
import datetime as _datetime
import sys

from . import SERVER_STATUS
from . import settings, converters
from ._compat import str_type, PY2

_py_version = sys.version_info[:2]
if PY2:
    pass
elif _py_version < (3, 6):
    # See http://bugs.python.org/issue24870
    _surrogateescape_table = [chr(i) if i < 0x80 else chr(i + 0xdc00) for i in range(256)]


    def _fast_surrogateescape(s):
        return s.decode('latin1').translate(_surrogateescape_table)
else:
    def _fast_surrogateescape(s):
        return s.decode('ascii', 'surrogateescape')

__all__ = ['Tool']


class Tool(object):
    def __init__(self, binary_prefix=False):
        self._binary_prefix = binary_prefix

    def __change_to_str(self, data, rowstr="<br>", count=4, origin_count=4):
        """
        This function can turn a data which you give into a str which you can look easily.
        Like a dict {"text": 1, "id":2, "reply":3}
        call the function changeToStr(text)
        you will look follow data:
        dict(3) =>{
          ["text"] => 1
          ["id"] => 2
          ["reply"] => 3
        }

        :param data: data you give, it can be any type.
        :param rowstr: it's a str. Enter, if you want to show data in web, it's default,
        or you want to show data in console, you should set rowstr="\n"
        :param count: It's a int. spacing num(I'm so sorry, I have forget the parameter's meaning, but you can try changing it.)
        :param origin_count: It's a int. spacing num(I'm so sorry, I have forget the parameter's meaning)
        :return: str
        """
        s = ""
        space1 = rowstr
        space2 = rowstr
        if count == 0:
            endstr = "}"
        else:
            endstr = "}"
        for i in range(count):
            space1 = space1 + " "
            if i >= origin_count:
                space2 = space2 + " "
        count = count + origin_count
        if isinstance(data, dict):
            length = len(data)
            s = s + "dict(" + str(length) + ") => {"
            for k, v in data.items():
                s = s + space1 + "['" + str(k) + "'] => " + self.__change_to_str(v, rowstr, count, origin_count)
            s = s + endstr if not length else s + space2 + endstr
        elif isinstance(data, tuple):
            length = len(data)
            s = s + "tuple(" + str(length) + ") => {"
            i = 0
            for v in data:
                s = s + space1 + "[" + str(i) + "] => " + self.__change_to_str(v, rowstr, count, origin_count)
                i = i + 1
            s = s + ")" if not length else s + space2 + ")"
        elif isinstance(data, list):
            length = len(data)
            s = s + "list(" + str(length) + ") => ["
            i = 0
            for v in data:
                s = s + space1 + "[" + str(i) + "] => " + self.__change_to_str(v, rowstr, count, origin_count)
                i = i + 1
            s = s + "]" if not length else s + space2 + "]"
        else:
            s = str(data)
        return s

    def debug(self, data="", is_set_time=settings.DEBUG_TIME):
        """
        :param is_set_time:
        :param data:
        :return: no return
        """
        if is_set_time:
            now_time = _datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print("[" + now_time + "]  " + self.__change_to_str(data, rowstr="\n"))
        else:
            print(self.__change_to_str(data, rowstr="\n"))

    def escape(self, obj, mapping=None):
        """Escape whatever value you pass to it.

        Non-standard, for internal use; do not use this in your applications.
        """
        if isinstance(obj, str_type):
            return "'" + self.escape_string(obj) + "'"
        if isinstance(obj, (bytes, bytearray)):
            ret = self._quote_bytes(obj)
            if self._binary_prefix:
                ret = "_binary" + ret
            return ret
        return converters.escape_item(obj, "utf-8", mapping=mapping)

    def escape_string(self, s):
        if SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES:
            return s.replace("'", "''")
        return converters.escape_string(s)

    def _quote_bytes(self, s):
        if SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES:
            return "'%s'" % (_fast_surrogateescape(s.replace(b"'", b"''")),)
        return converters.escape_bytes(s)
