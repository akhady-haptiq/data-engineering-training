import os
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt.cli import DbtCoreOperation

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "training_dbt")
DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "..", "training_dbt")
MODELS_DIR = os.path.join(PROJECT_DIR, "models")
SEEDS_DIR = os.path.join(PROJECT_DIR, "seeds")

@task
def discover_dbt_paths():
    logger = get_run_logger()
    models = []
    seeds = []

    # Recursively find all .sql files in models directory (including subdirectories)
    if os.path.exists(MODELS_DIR):
        for root, dirs, files in os.walk(MODELS_DIR):
            for name in files:
                if name.endswith(".sql"):
                    # Get model name without extension
                    model_name = os.path.splitext(name)[0]
                    models.append(model_name)
    
    # Find all .csv files in seeds directory
    if os.path.exists(SEEDS_DIR):
        for name in os.listdir(SEEDS_DIR):
            if name.endswith(".csv"):
                seed_name = os.path.splitext(name)[0]
                seeds.append(seed_name)
    
    logger.info(f"DBT Project Directory: {PROJECT_DIR}")
    logger.info(f"DBT Profiles Directory: {DBT_PROFILES_DIR}")
    logger.info(f"DBT Models Directory: {MODELS_DIR}")
    logger.info(f"DBT Seeds Directory: {SEEDS_DIR}")
    logger.info(f"Found {len(models)} models: {models}")
    logger.info(f"Found {len(seeds)} seeds: {seeds}")

    # Use space-separated format for dbt --select
    model_selection = " ".join(models) if models else "*"
    seed_selection = " ".join(seeds) if seeds else "*"

    return model_selection, seed_selection

@task
def run_dbt_seeds(model: str | None = None, full_refresh: bool = False):
    logger = get_run_logger()
    logger.info(f"Running DBT seeds for: {model}")
    seed_command = "dbt seed"
    
    if model:
        seed_command += f" --select {model}"
    if full_refresh:
        seed_command += " --full-refresh"
    
    logger.info(f"DBT Seed Command: {seed_command}")
    
    dbt_task = DbtCoreOperation(commands=[seed_command], project_dir=PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)

    result = dbt_task.run()
    return result

@task
def run_dbt_models(model_selection: str):
    logger = get_run_logger()
    logger.info(f"Running DBT models for: {model_selection}")

    if model_selection and model_selection != "*":
        command = f"dbt run --select {model_selection}"
    else:
        command = "dbt run"
    
    logger.info(f"DBT Run Command: {command}")
    dbt_task = DbtCoreOperation(commands=[command],
                                   project_dir=PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)

    result = dbt_task.run()
    return result

# Exercise Task Stub: Implement dbt tests
@task
def run_dbt_tests(model_selection: str | None = None):
    logger = get_run_logger()
    logger.info("(Exercise) dbt test task invoked with selection: %s", model_selection)

    if model_selection and model_selection != "*":
        command = f"dbt test --select {model_selection}"
    else:
        command = "dbt test"
    
    logger.info(f"DBT Test Command: {command}")
    
    dbt_task = DbtCoreOperation(commands=[command], project_dir=PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
    result = dbt_task.run()
    return result

@flow(name="prefect_dbt_testflow_run", log_prints=True)
def prefect_dbt_testflow_run(model_selection):
    logger = get_run_logger()
    logger.info("Starting DBT tests run flow...")
    
    run_dbt_tests(model_selection)

@flow(name="prefect_dbt_subflow_run", log_prints=True)
def prefect_dbt_subflow_run(model_selection):
    logger = get_run_logger()
    logger.info("Starting DBT models run subflow...")
    
    run_dbt_models(model_selection)

@flow(name="prefect_dbt_run", log_prints=True)
def prefect_dbt_flow_run(full_refresh: bool = True, seed_name: str | None = None):
    model_selection, seed_selection = discover_dbt_paths()
    
    if seed_name:
        run_dbt_seeds(seed_name, full_refresh)
    else:
        run_dbt_seeds(seed_selection)

    #run_dbt_models(model_selection)
    prefect_dbt_subflow_run(model_selection)

    # Exercise Task Stub: Implement dbt tests
    prefect_dbt_testflow_run(model_selection)

if __name__ == "__main__":
    prefect_dbt_flow_run()