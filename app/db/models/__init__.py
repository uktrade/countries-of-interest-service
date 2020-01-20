from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import ClauseElement


db = SQLAlchemy()
sql_alchemy = db

# aliases
_sa = sql_alchemy
_col = db.Column
_text = db.Text
_int = db.Integer
_dt = db.DateTime
_bigint = _sa.BigInteger
_bool = db.Boolean
_num = db.Numeric
_array = _sa.ARRAY
_date = _sa.Date


class BaseModel(db.Model):
    __abstract__ = True

    def save(self):
        _sa.session.add(self)
        _sa.session.commit()

    @classmethod
    def get_schema(cls):
        if 'schema' in cls.__table_args__:
            return cls.__table_args__['schema']
        else:
            return 'public'

    @classmethod
    def get_fq_table_name(cls):
        return f'"{cls.get_schema()}"."{cls.__tablename__}"'

    @classmethod
    def get_or_create(cls, defaults=None, **kwargs):
        """
        Creates a new object or returns existing.

        Example:
            object, created = Model.get_or_create(a=1, b=2, defaults=dict(c=3))

        :param defaults: (dictionary) of fields that should be saved on new instance
        :param kwargs: fields to query for an object
        :return: (Object, boolean) (Object, created)
        """
        instance = _sa.session.query(cls).filter_by(**kwargs).first()
        if instance:
            return instance, False
        else:
            params = dict(
                (k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement)
            )
            params.update(defaults or {})
            instance = cls(**params)
            instance.save()
            return instance, True
