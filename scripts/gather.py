import boto3
import json
import concurrent.futures
import time
from botocore.config import Config

# --- CONFIGURATION ---
LAMBDA_NAME = 'ScatterWorker'      # Change this to the name of your deployed Lambda function
REGION = 'ap-southeast-1'          # Change this to your AWS region where the Lambda is deployed

# CURRENT AWS ACCOUNT QUOTA IS AT 10 CONCURRENT LAMBDAS. 
# SCRIPT WILL FAIL IF YOU EXCEED THIS. CHECK WITH THE CLOUD ENGINEER BEFORE INCREASING NUM_WORKERS.
NUM_WORKERS = 10                   # How many Lambdas to trigger simultaneously
WALKS_PER_WORKER = 100             # How many walks each Lambda should do

retry_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)

# Initialize the AWS client (Requires AWS credentials set up on your machine)
lambda_client = boto3.client('lambda', region_name=REGION, config=retry_config)

def trigger_worker(worker_id):
    """Invokes a single Lambda function and returns its list of walks."""
    print(f"Worker {worker_id} launched...")
    payload = {
        "worker_id": f"worker-{worker_id}", 
        "num_walks": WALKS_PER_WORKER
    }
    
    try:
        # Trigger the Lambda
        response = lambda_client.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType='RequestResponse', # Wait for the Lambda to finish
            Payload=json.dumps(payload)
        )
        
        # Unpack the nested JSON response
        response_payload = json.loads(response['Payload'].read())
        body = json.loads(response_payload['body'])
        
        # Return the list of {"value": X, "weight": Y} dictionaries
        return body.get('results', [])
        
    except Exception as e:
        print(f"Worker {worker_id} failed: {e}")
        return []

def main():
    print(f"Scattering {NUM_WORKERS} workers to AWS...")
    all_walk_results = []
    
    # Fire all Lambdas at the exact same time using multithreading
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(trigger_worker, i) for i in range(NUM_WORKERS)]
        time.sleep(0.1) # Small delay to ensure all workers are launched before we start gathering results
        for future in concurrent.futures.as_completed(futures):
            worker_results = future.result()
            all_walk_results.extend(worker_results)
            
    print(f"\nGathered {len(all_walk_results)} successful walks across {NUM_WORKERS} workers.")
    
    # --- FINAL HORVITZ-THOMPSON MATH ---
    if all_walk_results:
        # The Algorithm Engineer's math applied globally across all workers
        total_weighted = sum(r["value"] * r["weight"] for r in all_walk_results)
        total_weight = sum(r["weight"] for r in all_walk_results)
        
        if total_weight > 0:
            weighted_avg = total_weighted / total_weight
            print(f"\n==============================================")
            print(f"Final Estimated Avg Extended Price: ${weighted_avg:,.2f}")
            print(f"==============================================")
        else:
            print("Total weight was zero.")
    else:
        print("No successful walks returned. Check your AWS logs.")

if __name__ == '__main__':
    main()