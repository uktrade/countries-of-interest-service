def to_camel_case(word):
    return word.split('_')[0] + ''.join(x.capitalize() or '_' for x in word.split('_')[1:])

def to_web_dict(df):
    headers = [to_camel_case(c) for c in df.columns]
    values = df.values.tolist()
    if len(headers) == 1:
        values = [v[0] for v in values]
    return {
        'headers': headers,
        'values': values
    }
