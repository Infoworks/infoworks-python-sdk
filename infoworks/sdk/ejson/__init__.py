from base64 import b64decode, b64encode
import calendar
from collections import OrderedDict
from datetime import date, datetime
import json
import six

EJSON_KEYWORDS = ("$date", "$type", "$value", "$escape", "$binary")

try:
    from datetime import timezone
except ImportError:
    from datetime import tzinfo, timedelta


    class timezone(tzinfo):
        def __init__(self, utcoffset, name=None):
            self._utcoffset = utcoffset
            self._name = name

        def utcoffset(self, dt):
            return self._utcoffset

        def tzname(self, dt):
            return self._name

        def dst(self, dt):
            return timedelta(0)


    timezone.utc = timezone(timedelta(0), 'UTC')


class UnknownTypeError(ValueError):
    pass


class EJSONEncoder(json.JSONEncoder):

    def __init__(self, *args, **kwargs):
        self.custom_type_hooks = kwargs.pop('custom_type_hooks', ())
        json.JSONEncoder.__init__(self, *args, **kwargs)

    def encode(self, o):
        if self.check_circular:
            markers = {}
        else:
            markers = None

        def _encode(o):
            if isinstance(o, (list, tuple, dict)):
                if markers is not None:
                    marker_id = id(o)
                    if marker_id in markers:
                        raise ValueError("Circular reference detected")
                    markers[marker_id] = o
                try:
                    if isinstance(o, dict):
                        dict_cls = OrderedDict if isinstance(o, OrderedDict) else dict
                        if any(kw in o for kw in EJSON_KEYWORDS):
                            return {"$escape": dict_cls((k, _encode(v)) for k, v in o.items())}
                        return dict_cls((k, _encode(v)) for k, v in o.items())
                    else:
                        return [_encode(v) for v in o]
                finally:
                    if markers is not None:
                        del markers[marker_id]

            if isinstance(o, date):
                return {"$date": int(calendar.timegm(o.timetuple()) * 1000)}

            if isinstance(o, datetime):
                return {"$date": int(calendar.timegm(o.utctimetuple()) * 1000)}

            if six.PY3 and isinstance(o, six.binary_type):
                return {"$binary": b64encode(o).decode()}

            for dict_cls, name, f in self.custom_type_hooks:
                if isinstance(o, dict_cls):
                    return {"$type": name, "$value": f(o)}

            return o

        return json.JSONEncoder.encode(self, _encode(o))


class EJSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        self.custom_type_hooks = dict(kwargs.pop('custom_type_hooks', ()))
        json.JSONDecoder.__init__(self, *args, **kwargs)

    def _decode_escaped(self, o):
        if isinstance(o, dict):
            if self.object_pairs_hook is not None:
                return self.object_pairs_hook((k, self._decode(v)) for k, v in o.items())
            return {k: self._decode(v) for k, v in o.items()}
        return self._decode(o)

    def _decode(self, o):
        if isinstance(o, dict):
            if len(o) == 1:
                if "$escape" in o:
                    return self._decode_escaped(o['$escape'])
                if "$date" in o:
                    return self._decode_escaped(o['$date'])
                if "$binary" in o:
                    return b64decode(o['$binary'])
            if len(o) == 2 and "$type" in o and "$value" in o:
                try:
                    reviver = self.custom_type_hooks[o['$type']]
                except KeyError:
                    raise UnknownTypeError(o["$type"])

                return reviver(o["$value"])

            if self.object_pairs_hook is not None:
                return self.object_pairs_hook((k, self._decode(v)) for k, v in o.items())
            return {k: self._decode(v) for k, v in o.items()}

        if isinstance(o, (list, tuple)):
            return [self._decode(v) for v in o]

        return o

    def decode(self, s, *args, **kwargs):
        o = json.JSONDecoder.decode(self, s, *args, **kwargs)
        return self._decode(o)


def dumps(obj, *args, **kwargs):
    return json.dumps(obj, *args, cls=kwargs.pop('cls', EJSONEncoder), **kwargs)


def loads(obj, *args, **kwargs):
    return json.loads(obj, *args, cls=kwargs.pop('cls', EJSONDecoder), **kwargs)
