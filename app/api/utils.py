from flask import request


def to_camel_case(word):
    word = word.lstrip('_').rstrip('_')
    index = 1
    previous_letter = word[0]
    new_word = previous_letter
    while index < len(word):
        if word[index] == '_' and previous_letter == '_':
            pass
        else:
            new_word = new_word + word[index]
        previous_letter = word[index]
        index += 1
    word = new_word
    return word.split('_')[0] + ''.join(x.capitalize() or '_' for x in word.split('_')[1:])


def to_web_dict(df, orient='records'):
    if orient == 'records':
        return to_records_web_dict(df)
    elif orient == 'tabular':
        return to_tabular_web_dict(df)
    else:
        raise Exception('unrecognised orient: {}'.format(orient))


def to_records_web_dict(df):
    headers = [to_camel_case(c) for c in df.columns]
    df.columns = headers
    return {'results': df.to_dict(orient='records')}


def to_tabular_web_dict(df):
    headers = [to_camel_case(c) for c in df.columns]
    values = df.values.tolist()
    if len(headers) == 1:
        values = [v[0] for v in values]
    return {'headers': headers, 'values': values}


def response_orientation_decorator(view, *args, **kwargs):
    def wrapper(*args, **kwargs):
        orientation = request.args.get('orientation', 'tabular')
        return view(orientation, *args, **kwargs)

    wrapper.__name__ = view.__name__
    return wrapper
