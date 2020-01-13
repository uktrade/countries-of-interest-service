import io

import pycountry

import spacy

from app.db import sql_alchemy
from app.db.db_utils import dsv_buffer_to_table
from app.utils import log

REPLACEMENTS = {
    'uk': 'united kingdom',
    'uae': 'united arab emirates',
    'ksa': 'saudi arabia',
    'usa': 'united states',
    'us': 'united states',
    'america': 'united states',
    'u.s.': 'united states',
    'czech republic': 'czechia',
    'holland': 'the netherlands',
    'amsterdam': 'the netherlands',
}

INTERESTED_INDICATORS = [
    'interested',
    'look',
    'excite',
    'keen',
    'sympathetic',
    'attract',
    'draw',
    'entice',
    'fascinated',
    'impressed',
    'inspire',
    'move',
    'predisposed',
    'prejudiced',
    'rouse',
    'stimulate',
    'take',
    'introduce',
    'meet',
    'consider',
    'discuss',
    'suggest',
    'plan',
    'like',
    'establish',
    'help',
    'envisage',
    'enquire',
    'support',
    'want',
    'research',
    'advice',
    'likely',
    'express',
    'wonder',
    'revisit',
    'explore',
    'expore',
    'start',
    'eager',
    'venture',
    'enter',
    'broaden',
    'wish',
    'penetrate',
    'enter',
    'grow',
    'widen',
    'expand',
]
EXPORTED_INDICATORS = [
    'carry',
    'deliver',
    'export',
    'send',
    'transport',
    'truck',
    'open',
    'present',
    'attended',
    'show',
    'commission',
    'proceed',
    'agree',
    'employ',
    'deal',
    'establish',
    'base',
    'relocate',
    'sell',
    'distribute',
    'ship',
    'showcase',
    'control',
    'materialise',
    'settle',
    'reopen',
    'redevelop',
    'reinforce',
    'demo',
    'oversee',
    'deepen',
    'direct',
    'participate',
    'train',
    'despatch',
    'release',
    'deploy',
    'enhance',
    'hold',
    'manufacture',
    'relaunch',
]

REFERENCE_COUNTRY_MAPPING = {
    'Bahamas': 'The Bahamas',
    'Bolivia, Plurinational State of': 'Bolivia',
    'Brunei Darussalam': 'Brunei',
    "Côte d'Ivoire": 'Ivory Coast',
    'Congo, The Democratic Republic of the': 'Congo (Democratic Republic)',
    'Cabo Verde': 'Cape Verde',
    'Falkland Islands (Malvinas)': 'Falkland Islands',
    'Micronesia, Federated States of': 'Micronesia',
    'Gambia': 'The Gambia',
    'Iran, Islamic Republic of': 'Iran',
    'Saint Kitts and Nevis': 'St Kitts and Nevis',
    'Korea, Republic of': 'South Korea',
    "Lao People's Democratic Republic": 'Laos',
    'Saint Lucia': 'St Lucia',
    'Saint Martin (French part)': 'Saint-Martin (French part)',
    'Moldova, Republic of': 'Moldova',
    'Myanmar': 'Myanmar (Burma)',
    'Pitcairn': 'Pitcairn, Henderson, Ducie and Oeno Islands',
    "Korea, Democratic People's Republic of": 'North Korea',
    'Palestine, State of': 'Occupied Palestinian Territories',
    'Russian Federation': 'Russia',
    'South Georgia and the South Sandwich Islands': 'South Georgia\
        and South Sandwich Islands',
    'Syrian Arab Republic': 'Syria',
    'Timor-Leste': 'East Timor',
    'Taiwan, Province of China': 'Taiwan',
    'Tanzania, United Republic of': 'Tanzania',
    'Holy See (Vatican City State)': 'Vatican City',
    'Saint Vincent and the Grenadines': 'St Vincent',
    'Venezuela, Bolivarian Republic of': 'Venezuela',
    'Virgin Islands, British': 'British Virgin Islands',
    'Virgin Islands, U.S.': 'United States Virgin Islands',
    'Viet Nam': 'Vietnam',
}


def _load_model():
    """
    Checks if the model is allocated within the project and downloads them if not.
    :return:
    """
    model = 'en_core_web_lg'
    spacy.cli.download(model)
    return spacy.load(model)


def _get_place_contexts(doc):
    places = [
        (ent, ent.label_) for ent in doc.ents if ent.label_ in ('GPE', 'LOC', 'NORP')
    ]
    segment_nb, token_segments, token_pos = 1, [], []
    for token in doc:
        if (
            len(token_pos) > 2
            and token.pos_ in ('PROPN', 'NOUN')
            and token_pos[-1] in ('CCONJ', 'PUNCT')
            and token_pos[-2] in ('PROPN', 'NOUN')
        ):
            segment_nb -= 1
        token_segments.append(segment_nb)
        if token.pos_ in ('CCONJ', 'PUNCT'):
            segment_nb += 1
        token_pos.append(token.pos_)
    tuple_list = []
    if places:
        for place, label in places:
            place_token = place[0]
            verb_list = []
            neg = False
            for ancestor in place_token.ancestors:
                if token_segments[ancestor.i] != token_segments[place_token.i]:
                    continue
                if ancestor.pos_ in ('VERB', 'AUX', 'ADJ'):
                    context = ancestor.lemma_
                    for token in ancestor.children:
                        if token.dep_ == 'neg':
                            context = context + ' ' + token.lemma_
                            neg = True
                    verb_list.append(context.replace('"', ''))
            tuple_list.append((str(place), label, verb_list, neg))
    return tuple_list


def _analyse_interaction(interaction_doc):
    place_contexts = _get_place_contexts(interaction_doc)
    places = []
    for place, label, verb_list, neg in place_contexts:
        action = None
        mapped_place = None
        if place.lower() in (
            'the',
            'north',
            'east',
            'south',
            'west',
            'middle',
            'st',
            '#',
            'sao',
            'new',
        ):
            continue
        if set(INTERESTED_INDICATORS) & set(
            [verb.replace('not ', '') for verb in verb_list]
        ):
            action = 'interested'
            if neg:
                action = 'not ' + action
        elif set(EXPORTED_INDICATORS) & set(
            [verb.replace('not ', '') for verb in verb_list]
        ):
            action = 'exported'
            if neg:
                action = 'not ' + action
        if label == 'GPE':
            mapped_place = REPLACEMENTS.get(place.lower(), None)
            try:
                mapped_place = pycountry.countries.search_fuzzy(mapped_place or place)[
                    0
                ].name
                mapped_place = REFERENCE_COUNTRY_MAPPING.get(mapped_place, mapped_place)
            except Exception:
                pass
        if label == 'NORP':
            try:
                # TODO: get countries from NORP
                continue
            except Exception:
                continue
        places.append((place, mapped_place, action, label, verb_list, neg))
    return places


@log('Step 1/1 - process interactions')
def process_interactions(
    input_table, input_schema, output_schema, output_table, batch_size=1000
):
    nlp = _load_model()
    raw_connection = sql_alchemy.engine.raw_connection()
    cursor = raw_connection.cursor(name='fetch_interactions')
    cursor.execute(
        f'''
            SELECT id, notes FROM "{input_schema}"."{input_table}"
            WHERE id > (SELECT coalesce(max(id), 0)
            FROM "{output_schema}"."{output_table}")
            ORDER BY id, created_on
        '''
    )

    def _analysed_interaction_chunks():
        batch_count = 0
        while True:
            chunk = io.StringIO()
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            print(
                "uploading events"
                f"{f'{batch_count*batch_size}-{batch_count*batch_size+len(rows)}'}"
            )
            for row in rows:
                id = row[0]
                interaction = row[1]
                interaction_doc = nlp(interaction)
                places = _analyse_interaction(interaction_doc)
                for place in places:
                    place, mapped_place, action, label, verb_list, neg = place
                    line = (
                        ','.join(
                            [
                                f'${id}$',
                                f"${place.replace('$','')}$",
                                ''
                                if not mapped_place
                                else f"${mapped_place.replace('$','')}$",
                                '' if not action else f'${action}$',
                                f'${label}$',
                                f'''${{{','.join(verb_list)}}}$''',
                                f'${neg}$',
                            ]
                        )
                        + '\n'
                    )
                    chunk.write(line)
            chunk.seek(0)
            yield chunk
            batch_count += 1

    for chunk in _analysed_interaction_chunks():
        dsv_buffer_to_table(
            chunk,
            table=output_table,
            schema=output_schema,
            sep=',',
            null='',
            has_header=False,
            columns=[
                'id',
                'place',
                'standardized_place',
                'action',
                'type',
                'context',
                'negation',
            ],
            quote='$',
        )

    cursor.close()
