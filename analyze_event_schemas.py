
import json
import os
from genson import SchemaBuilder


def convert_to_list_or_ignore(val):
	try: 
        # have to do this "eval" garbage since the arrays they stringify aren't actually
        # valid json (b.c. of single quotes seemingly)
		is_list = eval('isinstance({}, list)'.format(val))
		#is_list = isinstance(json.loads(val), list)
		if is_list:
			return eval(val)
            #return(json.loads(val))
		else:
			return val
	except:
		return val 

def make_sanitized_event(event):
	updated_event = {}
	for k, v in event.items():
		if isinstance(v, dict):
			updated_event[k] = unpack_lists(v)
		else:
			updated_event[k] = convert_to_list_or_ignore(v)
	return updated_event


def build_schemas(events):
	# make a dict of all the examples of each event by event type field
	events_by_type = {}
	for event in events:
		if event['event'] not in events_by_type:
			events_by_type[event['event']] = [event]
		else:
			events_by_type[event['event']].append(event)
	#
	# for each separate type send all the examples to the schema builder to generate a separate schema
	event_schemas = {}
	for event_type in events_by_type:
		builder = SchemaBuilder()
		events = events_by_type[event_type]
		for event in events:
			builder.add_object(event)
		# add the event type's schema
		event_schemas[event_type] = builder.to_schema()
		#
	return event_schemas

def get_schema_list_by_field(schemas):
	"""
	consumes the output of `build_schemas` -- this identifies for each distinct field how many different 
	schemas the field appears in
    .
	e.g.: context_library_name: ['DocumentUsageReport', 'AppLaunchReport', ...]

	"""
	# get all the distinct keys as a set
	distinct_fields = set()
	for schema in schemas.values():
		for k in schema['properties']:
			distinct_fields.add(k)
	# now find all the schemas each field appears in (want to identify common fields)
	schemas_by_field = {}
	for field in distinct_fields:
		for schema_name, schema in schemas.items():
			if field in schema['properties']:
				if field not in schemas_by_field:
					schemas_by_field[field] = [schema_name]
				else:
					schemas_by_field[field].append(schema_name)
	#
	return schemas_by_field

def read_events(filepath):
    events = []
    with open(filepath) as f:
        for line in f:
            event = json.loads(line)
            event = make_sanitized_event(event)
            events.append(event)
    return events

def write_schema_summaries(schemas, schema_output_dir):
    # write out the different schemas (and parse the "list" elements of the events)
    for schema_name, schema in schemas.items():
        schema_filename = schema_name + '.json'
        schema_filepath = os.path.join(schema_output_dir, schema_filename)
        with open(schema_filepath, 'w') as f:
            f.write(json.dumps(schema, indent=4))

def write_field_reports(schemas, outfile):
    # write out some info about each of the fields in the presonus events and which event types it shows up in
    with open(outfile, 'w') as f: 
        schemas_by_field = get_schema_list_by_field(schemas)
        for field in sorted(schemas_by_field.keys()):
        	f.write(field + '\n')
        	f.write("  in event types: {}\n\n".format(schemas_by_field[field]))

def main():
    events_file = './data/input/events.jl'
    schema_output_dir = './data/sample_output/schemas/'
    field_report_output_file = './data/sample_output/field_reports.txt'

    events = read_events(events_file)
    schemas = build_schemas(events)
    write_schema_summaries(schemas, schema_output_dir)
    write_field_reports(schemas, field_report_output_file)
    # also pretty-print events by type
    for schema_type in schemas:
        event_type_file = './data/sample_output/examples/{}_events_pretty.json'.format(schema_type)
        with open(event_type_file, 'w') as f:
            filtered_events = [e for e in events if e['event'] == schema_type]
            f.write(json.dumps(filtered_events, indent=4))



if __name__ == '__main__':
    main()

