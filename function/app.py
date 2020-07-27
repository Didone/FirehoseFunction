import base64
import json
import os
import logging

logging.basicConfig()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(getattr(logging, os.environ.get('LOG_LEVEL','INFO')))
LOGGER.debug(__name__)

OK='Ok'
NOK='Dropped'

def lambda_handler(event, context):
    """ Data transformation """
    LOGGER.debug(event)
    LOGGER.debug(context)
    for record in event['records']:
        try:
            payload = _decode(record['data'])
            LOGGER.debug(payload)
            data = {
                'id': payload['data']['id'],
                'chave': payload['data']['chave'],
                'valor': payload['data']['valor'],
                'op': payload['metadata']['operation'],
                'ts': payload['metadata']['timestamp']
            }
            LOGGER.debug(data)
            record['data'] = _encode(data)
            record['result'] = OK
            LOGGER.info(f"""{OK}:{record['recordId']}""")
        except Exception as err:
            LOGGER.error(err)
            LOGGER.warning(f"""{NOK}:{record['recordId']}""")
            record['result'] = NOK
    return event

def _decode(message):
    """ Decode base64 to JSON"""
    base64_bytes = message.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    message_text = message_bytes.decode('ascii')
    LOGGER.debug(message_text)
    return json.loads(message_text)

def _encode(json_object):
    """ Encode JSON to base64"""
    message_bytes = json.dumps(json_object).encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_text = base64_bytes.decode('ascii')
    LOGGER.debug(base64_text)
    return base64_text
