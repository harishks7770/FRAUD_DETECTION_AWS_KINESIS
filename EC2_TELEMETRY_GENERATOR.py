import json
import random
import time
import boto3
from faker import Faker

fake = Faker()


REGION = "ap-south-1"
STREAM_NAME = "FraudTransactionsStream"
TARGET_TPS = 115          # ~10 Million records/day
MAX_BATCH_SIZE = 500      # AWS Kinesis PutRecords hard limit
MAX_RETRIES = 3

# Uses boto3's default credential chain (IAM role / instance profile on the
# EC2 box). No access keys in source 
kinesis_client = boto3.client("kinesis", region_name=REGION)


def generate_transaction():
    """Generates synthetic transaction payload."""
    return {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "location": random.choice(["Chennai", "Mumbai", "New York", "London", "Delhi"]),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }


def put_records_with_retry(records):
    """Sends a batch to Kinesis, retrying only the records that actually
    failed (throttled shards) instead of dropping them."""
    remaining = records
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = kinesis_client.put_records(Records=remaining, StreamName=STREAM_NAME)
        except Exception as e:
            print(f"❌ Ingestion Error (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(attempt)  # small linear backoff
            continue

        failed_count = response.get("FailedRecordCount", 0)
        if failed_count == 0:
            return

        # Pull out just the records that failed so we don't resend the ones
        # that already succeeded.
        failed_records = [
            remaining[i] for i, r in enumerate(response["Records"]) if "ErrorCode" in r
        ]
        print(f"⚠️ {failed_count} records throttled (attempt {attempt}/{MAX_RETRIES}), retrying...")
        remaining = failed_records
        time.sleep(attempt)  # back off a bit more each retry

    if remaining:
        print(f"❌ Giving up on {len(remaining)} records after {MAX_RETRIES} attempts.")


def send_batch():
    records = []
    for _ in range(TARGET_TPS):
        data = generate_transaction()
        records.append({
            'Data': json.dumps(data),
            'PartitionKey': str(data["user_id"])
        })

    # Process in safe chunks of 500 max per API call
    for i in range(0, len(records), MAX_BATCH_SIZE):
        chunk = records[i:i + MAX_BATCH_SIZE]
        put_records_with_retry(chunk)


if __name__ == "__main__":
    print(f"🚀 Pumping {TARGET_TPS} TPS to Kinesis Stream: {STREAM_NAME} ({REGION})...")
    try:
        while True:
            start_time = time.time()
            send_batch()
            elapsed_time = time.time() - start_time

            # Dynamic sleep to maintain steady 1-second cadence
            sleep_duration = max(0.0, 1.0 - elapsed_time)
            time.sleep(sleep_duration)

    except KeyboardInterrupt:
        print("\n[*] Script stopped cleanly by user.")
