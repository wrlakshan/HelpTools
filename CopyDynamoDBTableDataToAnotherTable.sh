#!/bin/bash

# Source and destination table names
SOURCE_TABLE="source-table-name"
DESTINATION_TABLE="destination-table-name"
REGION="eu-north-1"  # Change this to your AWS region

# Scan the source table
scan_result=$(aws dynamodb scan --table-name $SOURCE_TABLE --region $REGION)

# Extract items from scan result
items=$(echo $scan_result | jq -c '.Items')

# Batch write items to the destination table
batch_write() {
  batch_items=$1
  request_items=$(jq -c --arg dest_table "$DESTINATION_TABLE" '{ ($dest_table): [ .[] | { PutRequest: { Item: . } } ] }' <<< "$batch_items")
  result=$(aws dynamodb batch-write-item --request-items "$request_items" --region $REGION)
  unprocessed=$(echo $result | jq '.UnprocessedItems | length')
  echo "$unprocessed"
}

# Split items into batches of 25 (max for batch-write-item)
batch_size=25
total_items=$(echo $items | jq 'length')
num_batches=$(( (total_items + batch_size - 1) / batch_size ))
progress_bar_width=50  # Adjust the width of the progress bar here

# Initialize progress variables
progress_count=0
progress_bar=""

# Print initial progress bar
printf "[%-${progress_bar_width}s] %d%%\r" "$progress_bar" 0

# Iterate over batches
for ((i = 0; i < total_items; i += batch_size)); do
  batch=$(echo $items | jq ".[$i:$((i + batch_size))]")
  unprocessed=$(batch_write "$batch")
  progress_count=$((progress_count + 1))

  if [[ $unprocessed -eq 0 ]]; then
    # Update progress bar
    percentage=$((progress_count * 100 / num_batches))
    completed=$((percentage * progress_bar_width / 100))
    remaining=$((progress_bar_width - completed))
    progress_bar=$(printf "%-${completed}s" "#" | tr ' ' '#')

    # Print updated progress bar
    printf "[%-${progress_bar_width}s] %d%%\r" "$progress_bar" "$percentage"
  else
    echo "Error: Some items were not processed."
    break
  fi
done

echo ""  # Print a new line after the progress bar completes
