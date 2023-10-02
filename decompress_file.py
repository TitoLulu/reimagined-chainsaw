import bz2
import csv
import json
import os


def open_file(file_path, output_folder):
    """
    Opens and processes a BZIP2-compressed JSON file containing events data.

    Args:
        file_path (str): The path to the BZIP2-compressed JSON file.
        output_folder (str): The folder where CSV files will be written.

    Returns:
        None
    """
    distinct_types = set()
    type_data = {}

    with bz2.open(file_path, "rt") as f:
        for line in f:
            event = json.loads(line.strip())
            event_type = event.get("type", None)

            if event_type:
                distinct_types.add(event_type)

                if event_type not in type_data:
                    type_data[event_type] = []

                type_data[event_type].append(event)

    for event_type, data in type_data.items():
        output_filename = os.path.join(output_folder, f"{event_type}.csv")

        with open(output_filename, "w", newline="") as output_file:
            csv_writer = csv.DictWriter(output_file, fieldnames=data[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(data)


file_path = os.path.join(os.getcwd(), "events.jsonl.bz2")
output_folder = os.path.join(os.getcwd(), "reimagined_chainsaw/seeds")
open_file(file_path=file_path, output_folder=output_folder)
