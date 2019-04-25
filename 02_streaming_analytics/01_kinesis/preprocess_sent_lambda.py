import base64
import json

print('Loading function')

# define processing functions

def get_bull_score(sent_score):
    if sent_score > 0:
        return sent_score
    return 0

def get_bear_score(sent_score):
    if sent_score < 0:
        return sent_score
    return 0

def lambda_handler(event, context):
    output = []

    for record in event['records']:
        # get data from input data
        payload = base64.b64decode(record['data'])
        data = json.loads(payload)
        
        # Do custom processing on the record payload here
        created_at = data.get('created_at')
        symbol = data.get('symbol')
        sent_score = data.get('sent_score')
        
        bull_score = get_bull_score(sent_score)
        bear_score = get_bear_score(sent_score)

        payload = {
            'created_at': created_at,
            'symbol': symbol,
            'bull_score': bull_score,
            'bear_score': bear_score
        }

        # to to encode data in a weird way so that we can deliver
        transormed_data = base64.b64encode(json.dumps(payload).encode('utf-8')).decode('utf-8')
    
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': transormed_data
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))
    print({'records': output})

    return {'records': output}