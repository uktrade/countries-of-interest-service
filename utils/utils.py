def to_camel_case(word):
    return word.split('_')[0] + ''.join(x.capitalize() or '_' for x in word.split('_')[1:])

def to_web_dict(df):
    headers = [to_camel_case(c) for c in df.columns]
    values = df.values.tolist()
    return {
        'headers': headers,
        'values': values
    }
