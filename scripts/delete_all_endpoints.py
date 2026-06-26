"""
🔴 SAFETY SCRIPT — Run this before you log off. Every time. No exceptions.
Deletes all SageMaker endpoints, endpoint configs, and stops any running
training/processing/tuning jobs in your account.
"""
import boto3

def nuke_everything(region="us-east-1"):
    sm = boto3.client("sagemaker", region_name=region)

    # Delete endpoints
    endpoints = sm.list_endpoints()["Endpoints"]
    for ep in endpoints:
        name = ep["EndpointName"]
        print(f"🗑️  Deleting endpoint: {name}")
        sm.delete_endpoint(EndpointName=name)

    # Delete endpoint configs
    configs = sm.list_endpoint_configs()["EndpointConfigs"]
    for cfg in configs:
        name = cfg["EndpointConfigName"]
        print(f"🗑️  Deleting endpoint config: {name}")
        sm.delete_endpoint_config(EndpointConfigName=name)

    # Stop training jobs
    for job in sm.list_training_jobs(StatusEquals="InProgress")["TrainingJobSummaries"]:
        name = job["TrainingJobName"]
        print(f"⏹️  Stopping training job: {name}")
        sm.stop_training_job(TrainingJobName=name)

    # Stop processing jobs
    for job in sm.list_processing_jobs(StatusEquals="InProgress")["ProcessingJobSummaries"]:
        name = job["ProcessingJobName"]
        print(f"⏹️  Stopping processing job: {name}")
        sm.stop_processing_job(ProcessingJobName=name)

    # Stop HPO jobs
    for job in sm.list_hyper_parameter_tuning_jobs(StatusEquals="InProgress")["HyperParameterTuningJobSummaries"]:
        name = job["HyperParameterTuningJobName"]
        print(f"⏹️  Stopping HPO job: {name}")
        sm.stop_hyper_parameter_tuning_job(HyperParameterTuningJobName=name)

    if not endpoints:
        print("✅ No endpoints running. You're safe.")
    else:
        print(f"\n✅ Cleaned up {len(endpoints)} endpoint(s).")

if __name__ == "__main__":
    nuke_everything()
