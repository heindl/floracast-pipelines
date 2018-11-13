TIME=$(date '+%s')

python generate_records.py \
         --runner DataflowRunner \
         --bucket "floracast-datamining" \
         --temp_location="gs://floracast-datamining/temp" \
         --staging_location="gs://floracast-datamining/staging" \
         --job_name "floracast-fetch-$TIME" \
         --setup_file /Users/m/Documents/pyflora/setup.py
