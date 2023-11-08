from flask import current_app as flask_app
from flask import request
from flask.views import View

from app.common.api.utils import to_web_dict


class PaginatedListView(View):
    camel_case_columns = True
    include_id_column = False

    def get_select_clause(self):
        raise NotImplementedError

    def get_from_clause(self):
        raise NotImplementedError

    def dispatch_request(self):
        orientation = request.args.get('orientation', 'tabular')
        pagination_size = flask_app.config['app']['pagination_size']
        next_id = request.args.get('next-id')

        pagination_clause = ''
        pagination_values = []

        if next_id is not None:
            pagination_clause = 'where id >= %s'
            pagination_values = [next_id]

        sql_query = f'''
            select id, {self.get_select_clause()}
            from {self.get_from_clause()}
            {pagination_clause}
            order by id
            limit {pagination_size} + 1
        '''

        df = flask_app.dbi.execute_query(sql_query, data=pagination_values, df=True)
        if len(df) == pagination_size + 1:
            next_ = '{}{}?'.format(request.host_url[:-1], request.path)
            next_ += 'orientation={}'.format(orientation)
            next_ += '&next-id={}'.format(df['id'].values[-1])
            df = df[:-1]
        else:
            next_ = None
        df = df if self.include_id_column else df.iloc[:, 1:]
        web_dict = to_web_dict(df, orientation, self.camel_case_columns)
        web_dict['next'] = next_
        return flask_app.make_response(web_dict)
