from flask_security import RoleMixin, UserMixin

from app.db.models import (
    _bool,
    _col,
    _dt,
    _int,
    _text,
    BaseModel,
    db,
)


class UserRole(BaseModel):
    __tablename__ = 'users_roles'
    __table_args__ = {'schema': 'admin'}

    id = _col(_int(), primary_key=True, autoincrement=True)
    user_id = _col(_int(), db.ForeignKey('admin.users.id'))
    role_id = _col(_int(), db.ForeignKey('admin.roles.id'))


class Role(BaseModel, RoleMixin):
    __tablename__ = 'roles'
    __table_args__ = {'schema': 'admin'}

    id = _col(_int(), primary_key=True, autoincrement=True)
    name = _col(_text(), unique=True)
    description = _col(_text())

    def __str__(self):
        return self.name


class User(BaseModel, UserMixin):
    __tablename__ = 'users'
    __table_args__ = {'schema': 'admin'}

    id = _col(_int(), primary_key=True)
    user_id = _col(_text(), unique=True)
    first_name = _col(_text())
    last_name = _col(_text())
    email = _col(_text())
    password = _col(_text())
    active = _col(_bool())
    last_login_at = _col(_dt())
    current_login_at = _col(_dt())
    last_login_ip = _col(_text())
    current_login_ip = _col(_text())
    login_count = _col(_int())

    roles = db.relationship('Role', secondary='admin.users_roles', backref='users',)

    def __str__(self):
        return self.email
