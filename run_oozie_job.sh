#!/bin/bash

# Check if numYears argument is provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <numYears>"
    echo "Example: $0 5"
    exit 1
fi

# Get numYears from command line argument
NUM_YEARS=$1

# Configuration - use relative paths and current directory
JOB_PROPERTIES="$(pwd)/job.properties"
TEMP_PROPERTIES="$(pwd)/job_temp.properties"
OUTPUT_LOG_FILE="$(pwd)/one.txt"

echo "Using job properties file: $JOB_PROPERTIES"

# Check if job.properties exists
if [ ! -f "$JOB_PROPERTIES" ]; then
    echo "Error: Job properties file not found at $JOB_PROPERTIES"
    echo "Please create a job.properties file in the current directory or specify the correct path."
    exit 1
fi

# Create a temporary properties file with the updated numYears
echo "Creating temporary properties file with numYears=$NUM_YEARS"
cp "$JOB_PROPERTIES" "$TEMP_PROPERTIES"

# Update or add the numYears property
if grep -q "^numYears=" "$TEMP_PROPERTIES"; then
    # If numYears exists, update it
    sed -i "s/^numYears=.*/numYears=$NUM_YEARS/" "$TEMP_PROPERTIES"
else
    # If numYears doesn't exist, add it
    echo "numYears=$NUM_YEARS" >> "$TEMP_PROPERTIES"
fi

# Ensure log file can be written
touch "$OUTPUT_LOG_FILE"

# Record start time and parameters
START_TIME=$(date +"%Y-%m-%d %H:%M:%S")
echo "===========================================" >> "$OUTPUT_LOG_FILE"
echo "JOB STARTED AT: $START_TIME" >> "$OUTPUT_LOG_FILE"
echo "NUM_YEARS: $NUM_YEARS" >> "$OUTPUT_LOG_FILE"
echo "===========================================" >> "$OUTPUT_LOG_FILE"
echo "" >> "$OUTPUT_LOG_FILE"

# Set EPOCH start time for duration calculation
EPOCH_START=$(date +%s)

# Run the Oozie job and capture job ID
echo "Starting Oozie workflow job with numYears=$NUM_YEARS..."
JOB_SUBMISSION=$(bin/oozie job -oozie http://localhost:11000/oozie -config "$TEMP_PROPERTIES" -run 2>&1)
echo "Job submission response: $JOB_SUBMISSION" | tee -a "$OUTPUT_LOG_FILE"

# Check if job submission was successful
if [[ "$JOB_SUBMISSION" == *"job:"* ]]; then
    JOB_ID=$(echo "$JOB_SUBMISSION" | grep "job:" | awk '{print $2}')
    echo "Job ID: $JOB_ID" >> "$OUTPUT_LOG_FILE"
    echo "" >> "$OUTPUT_LOG_FILE"

    # Wait for job to complete
    echo "Waiting for job to complete..."
    while true; do
        STATUS_INFO=$(bin/oozie job -info "$JOB_ID" 2>&1)
        
        if [[ "$STATUS_INFO" == *"Status"* ]]; then
            STATUS=$(echo "$STATUS_INFO" | grep "Status" | head -1 | awk '{print $3}')
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] Current status: $STATUS" | tee -a "$OUTPUT_LOG_FILE"
            
            if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "KILLED" || "$STATUS" == "FAILED" ]]; then
                break
            fi
        else
            echo "[$(date +"%Y-%m-%d %H:%M:%S")] Error getting status: $STATUS_INFO" | tee -a "$OUTPUT_LOG_FILE"
            STATUS="ERROR"
            break
        fi
        
        # Wait 30 seconds before checking again
        sleep 30
    done
else
    echo "Job submission failed: $JOB_SUBMISSION" | tee -a "$OUTPUT_LOG_FILE"
    STATUS="SUBMISSION_FAILED"
fi

# Record end time
END_TIME=$(date +"%Y-%m-%d %H:%M:%S")
EPOCH_END=$(date +%s)

# Calculate duration
DURATION=$((EPOCH_END - EPOCH_START))
HOURS=$((DURATION / 3600))
MINUTES=$(( (DURATION % 3600) / 60 ))
SECONDS=$((DURATION % 60))

echo "" >> "$OUTPUT_LOG_FILE"
echo "===========================================" >> "$OUTPUT_LOG_FILE"
echo "JOB COMPLETED AT: $END_TIME" >> "$OUTPUT_LOG_FILE"
echo "FINAL STATUS: $STATUS" >> "$OUTPUT_LOG_FILE"
echo "NUM_YEARS: $NUM_YEARS" >> "$OUTPUT_LOG_FILE" 
echo "TOTAL EXECUTION TIME: ${HOURS}h ${MINUTES}m ${SECONDS}s" >> "$OUTPUT_LOG_FILE"
echo "===========================================" >> "$OUTPUT_LOG_FILE"

# Print job logs to file if job ID exists and job failed
if [[ -n "$JOB_ID" && "$STATUS" == "FAILED" ]]; then
    echo "" >> "$OUTPUT_LOG_FILE"
    echo "JOB ERROR LOGS:" >> "$OUTPUT_LOG_FILE"
    bin/oozie job -log "$JOB_ID" >> "$OUTPUT_LOG_FILE" 2>&1
fi

# Clean up temporary properties file
rm "$TEMP_PROPERTIES"

echo "Job execution completed. Results saved to $OUTPUT_LOG_FILE"
