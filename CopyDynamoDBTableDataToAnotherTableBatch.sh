#!/bin/bash

# Source and destination table names
SOURCE_TABLE="source-table-name"
DESTINATION_TABLE="destination-table-name"
REGION="eu-north-1"  # Change this to your AWS region
BATCH_SIZE=25  # Max items per batch-write-item request
SCAN_LIMIT=100  # Number of items to scan in each request

# Function to perform batch write to the destination table
batch_write() {
  local batch_items=$1
  local request_items
  request_items=$(jq -c --arg dest_table "$DESTINATION_TABLE" '{ ($dest_table): [ .[] | { PutRequest: { Item: . } } ] }' <<< "$batch_items")
  local result
  result=$(aws dynamodb batch-write-item --request-items "$request_items" --region $REGION)
  local unprocessed
  unprocessed=$(echo "$result" | jq '.UnprocessedItems | length')
  echo "$unprocessed"
}

# Initialize variables
last_evaluated_key=""
total_items=0
progress_count=0
progress_bar_width=50  # Adjust the width of the progress bar here

# Print initial progress bar
printf "[%-${progress_bar_width}s] %d%%\r" "" 0

# Loop to scan and process items
while : ; do
  # Scan the source table
  if [[ -z "$last_evaluated_key" ]]; then
    scan_result=$(aws dynamodb scan --table-name "$SOURCE_TABLE" --limit $SCAN_LIMIT --region "$REGION")
  else
    scan_result=$(aws dynamodb scan --table-name "$SOURCE_TABLE" --limit $SCAN_LIMIT --region "$REGION" --exclusive-start-key "$last_evaluated_key")
  fi
  
  # Extract items and last evaluated key
  items=$(echo "$scan_result" | jq -c '.Items')
  last_evaluated_key=$(echo "$scan_result" | jq -r '.LastEvaluatedKey // empty')

  # Update total items count
  batch_items=$(echo "$items" | jq 'length')
  total_items=$((total_items + batch_items))

  # Split and batch write items
  for ((i = 0; i < batch_items; i += BATCH_SIZE)); do
    batch=$(echo "$items" | jq ".[$i:$((i + BATCH_SIZE))]")
    unprocessed=$(batch_write "$batch")
    progress_count=$((progress_count + 1))

    if [[ $unprocessed -eq 0 ]]; then
      # Update progress bar
      percentage=$((progress_count * 100 / total_items))
      completed=$((percentage * progress_bar_width / 100))
      remaining=$((progress_bar_width - completed))
      progress_bar=$(printf "%-${completed}s" "#" | tr ' ' '#')

      # Print updated progress bar
      printf "[%-${progress_bar_width}s] %d%%\r" "$progress_bar" "$percentage"
    else
      echo "Error: Some items were not processed."
      break 2
    fi
  done

  # Break the loop if there are no more items to scan
  if [[ -z "$last_evaluated_key" ]]; then
    break
  fi
done

echo ""  # Print a new line after the progress bar completes
echo "Completed processing $total_items items."
