
import json
import base64

test_event_file = './data/input/test_s3_event.json'

with open(test_event_file) as f:
	payload = json.load(f)
	output_record = {
		'recordId': 'mYs3rEcOrDiD',
		'result': 'Ok',
		'data': base64.b64encode(
			json.dumps(payload).encode('utf-8')+b'\n'
		).decode('utf-8')
	}
	event = {'records': [output_record]}
	print(json.dumps(event, indent=4))
