import json
import logging
import base64
import copy

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_firehose_data(record):
    '''
    Parses and decodes the lambda event for kinesis stream data.

    :param str record: singleton segment event record
    :return: kinesis stream decoded data
    :rtype: str
    '''
    try:
        return base64.b64decode(record['data']).decode('utf-8')
    except (TypeError, KeyError, base64.binascii.Error, UnicodeDecodeError) as e:
        logger.exception(e)


def transform_records(records):
    try:
        output = []
        for record in records:
            data = get_firehose_data(record)
            payload = json.loads(data)
            logger.info('EVENT DATA: {}'.format(json.dumps(payload, indent=4)))

            properties = payload['properties']
            # right now data has multiple commands as array 
            exploded_events = explode_payload_list(properties, 'commands')
            # aggregate the separate documents together as one string with linebreaks
            record_data_with_linebreaks = ''
            # create separate events for each of the commands
            for i, cmd_event in enumerate(exploded_events):
                logger.info('COMMAND EVENT {0}: {1}'.format(i + 1, json.dumps(cmd_event, indent=4)))
                # overwrite the properties field of og payload
                new_payload = copy.deepcopy(payload)
                new_payload['properties'] = cmd_event
                # unnest properties portion of this dict
                new_payload = unnest_dict_key(new_payload, 'properties')

                logger.info('OUTPUT RECORD: {}'.format(json.dumps(new_payload, indent=4)))
                str_data_with_linebreak = json.dumps(new_payload) + '\n'
                record_data_with_linebreaks += str_data_with_linebreak

            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(
                    record_data_with_linebreaks.encode('utf-8')
                ).decode('utf-8')
            }
            output.append(output_record)
        logger.info('SUCCESS: Processed {0} Records. Created {1} Records'.format(
            len(records), len(output)))
        return output
    except Exception:
        logger.exception('LAMBDA EXCEPTION')

def explode_payload_list(payload, field_name, prefix=None):
    # takes a field name and returns duplicates of payload to flatten out list value
    # of field_name
    output_payloads = []
    array_field = payload[field_name]
    # duplicate payload a bunch of times with these fields as top-level fields
    for record in array_field:
        new_payload = copy.deepcopy(payload)
        # remove field from payload
        del new_payload[field_name]
        for k, v in record.items():
            if prefix is not None:
                k = '{0}_{1}'.format(prefix, k)
            new_payload[k] = v
        output_payloads.append(new_payload)
    return output_payloads

def unnest_dict_key(payload, dict_key):
    # unnests dictionary like 
    #     {'field1': 1, 'field2': {'a': 1, 'b': 2}}
    #      to => {'field1': 1, 'field2_a': 1, 'field2_b': 2}
    new_payload = copy.deepcopy(payload)
    if not isinstance(payload[dict_key], dict):
        return new_payload
    # otherwise we need to unpack dict keys
    # first remove nested value from original dict
    del new_payload[dict_key]
    sub_dict = payload[dict_key]
    for k, v in sub_dict.items():
        # check if field name is already used at top level of dict
        # if it is then rename to be {dict_key}_{k} 
        if k in new_payload:
            k = '{0}_{1}'.format(dict_key, k)
        new_payload[k] = v
    return new_payload


def handler(event, context):
    try:
        output = transform_records(event['records'])
        return {'records': output}
    except (TypeError, KeyError) as e:
        logger.exception(e)
