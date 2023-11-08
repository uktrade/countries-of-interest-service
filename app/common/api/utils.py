import numpy as np


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


def to_web_dict(df, orient='records', camel_case=True):
    df = df.replace({np.nan: None})
    if orient == 'records':
        return to_records_web_dict(df, camel_case)
    elif orient == 'tabular':
        return to_tabular_web_dict(df, camel_case)
    else:
        raise Exception('unrecognised orient: {}'.format(orient))


def to_records_web_dict(df, camel_case=True):
    headers = [to_camel_case(c) if camel_case else c for c in df.columns]
    df.columns = headers
    return {'results': df.reindex(sorted(df.columns), axis=1).to_dict(orient='records')}


def to_tabular_web_dict(df, camel_case=True):
    headers = [to_camel_case(c) if camel_case else c for c in df.columns]
    values = df.values.tolist()
    if len(headers) == 1:
        values = [v[0] for v in values]
    return {'headers': headers, 'values': values}
