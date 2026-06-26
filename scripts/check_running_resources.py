"""
Quick check — what's currently running and billing you?
Run this anytime you're unsure.
"""
import boto3

def check(region="us-east-1"):
    sm = boto3.client("sagemaker", region_name=region)

    endpoints = sm.list_endpoints()["Endpoints"]
    training = sm.list_training_jobs(StatusEquals="InProgress")["TrainingJobSummaries"]
    processing = sm.list_processing_jobs(StatusEquals="InProgress")["ProcessingJobSummaries"]
    hpo = sm.list_hyper_parameter_tuning_jobs(StatusEquals="InProgress")["HyperParameterTuningJobSummaries"]

    print("====== RUNNING RESOURCES (BILLING YOU) ======\n")

    if endpoints:
        print(f"🔴 ENDPOINTS ({len(endpoints)}):")
        for ep in endpoints:
            print(f"   - {ep['EndpointName']} ({ep['EndpointStatus']})")
    else:
        print("✅ No endpoints running.")

    if training:
        print(f"\n🟡 TRAINING JOBS ({len(training)}):")
        for j in training:
            print(f"   - {j['TrainingJobName']}")
    else:
        print("✅ No training jobs running.")

    if processing:
        print(f"\n🟡 PROCESSING JOBS ({len(processing)}):")
        for j in processing:
            print(f"   - {j['ProcessingJobName']}")
    else:
        print("✅ No processing jobs running.")

    if hpo:
        print(f"\n🟡 HPO JOBS ({len(hpo)}):")
        for j in hpo:
            print(f"   - {j['HyperParameterTuningJobName']}")
    else:
        print("✅ No HPO jobs running.")

    print("\n=============================================")

if __name__ == "__main__":
    check()
