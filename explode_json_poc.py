
import json
import itertools

# NOTE: could totally generalize all of this as helper function to transform any original 
# json from segment that has array fields to key off of messageid

# maybe this is sketchy since it *depends* on using segment to generate that field, but we could use a hash or something 
# in the future if e.g. this was all written directly to kineses and we don't get the automatic unique identifiers made by segment

def load_events(input_file_pretty):
	with open(input_file_pretty) as f:
		events_data = json.load(f)
		return events_data

def get_event_commands(event):
	# could make generic and just take *args for the keys whos values line up this way
	# should do validation to make sure all are of equal length before zipping in that case
	msg_id = event['messageid']
	commands = event['properties_commands_command']
	counts = event['properties_commands_count']
	invokers = event['properties_commands_invoker']
	commands_output = []
	for command, count, invoker in zip(commands, counts, invokers):
		command_name, command_value = tuple(command.split('|'))
		command_record = {
		    'messageid': msg_id,
		    'commandName': command_name,
		    'commandValue': command_value,
		    'count': count,
		    'invoker': invoker
		}
		commands_output.append(command_record)

	return commands_output


def main():
	input_file_pretty = './data/sample_output/examples/CommandUsageReport_events_pretty.json'
	command_summary_fl = './data/output/transformed/command_report_summary.json'
	command_detail_fl = './data/output/transformed/command_report_detail.json'
	events = load_events(input_file_pretty)
	detail_records = []
	summary_records = []
	for event in events:
		commands = get_event_commands(event)
		detail_records += commands
		# delete keys from event
		for k in ('properties_commands_command', 'properties_commands_count', 'properties_commands_invoker'):
			del event[k]
		# keep the modified event
		summary_records.append(event)

	with open(command_summary_fl, 'w') as f:
		f.write(json.dumps(summary_records, indent=4))

	with open(command_detail_fl, 'w') as f:
		f.write(json.dumps(detail_records, indent=4))


if __name__ == '__main__':
	main()
