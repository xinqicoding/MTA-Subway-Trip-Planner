import sys
import awspyml


if __name__ == "__main__":
    try:
        data_fn = "trainRecordFinal.csv"
        target = None
        if len(sys.argv) > 2:
            target = sys.argv[2]
    except:
	print(__doc__)
        sys.exit(-1)
    schema = awspyml.SchemaGuesser().from_file(data_fn, target_variable=target)
    print(schema.as_json_string())