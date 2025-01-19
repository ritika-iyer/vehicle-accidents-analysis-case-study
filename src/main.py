import argparse
from utils import (
    load_config,
    create_spark_session,
    call_analysis_methods,
    list_analysis_options,
)
from vehicle_accident_analysis.accident_analysis import VehicleCrashAnalysis
from constants import analysis_descriptions


def main():
    config_path = "config/config.yaml"

    parser = argparse.ArgumentParser(description="Vehicle Accident Analysis Case Study")

    parser.add_argument(
        "--analysis", help="select the specific car crash analysis", default="run_all"
    )
    parser.add_argument(
        "--list_analysis_options",
        action="store_true",
        help="List all available analysis options and their descriptions",
    )

    args = parser.parse_args()
    if args.list_analysis_options:
        list_analysis_options(analysis_descriptions)
        return
    required_analysis = args.analysis
    config = load_config(config_path)
    spark = create_spark_session(config)
    analysis = VehicleCrashAnalysis(config, spark)

    call_analysis_methods(analysis, required_analysis)


if __name__ == "__main__":
    main()
