import yaml
from pyspark.sql import SparkSession


def create_spark_session(config):
    """
    Create a Spark session using the provided configuration.

    This function initializes a Spark session with the settings from the configuration file.

    :param config: Dictionary containing configuration values.
    :type config: dict
    :return: A Spark session object.
    :rtype: pyspark.sql.SparkSession
    """
    spark = (
        SparkSession.builder.appName(config["spark_config"]["app_name"])
        .master(config["spark_config"]["master"])
        .config("spark.executor.memory", config["spark_config"]["executor_memory"])
        .config("spark.driver.memory", config["spark_config"]["driver_memory"])
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config(
            "spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"
        )
        .getOrCreate()
    )

    return spark


def load_config(config_path):
    """
    Load the contents from config YAML file into a Python dictionary.

    :param config_path: Path to the YAML configuration file.
    :type config_path: str
    :return: The configuration data as a Python dictionary.
    :rtype: dict
    """

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def load_csv_data(spark, input_path):
    """
    Load CSV data into a Spark DataFrame.

    This function reads a CSV file from the specified input path into a Spark DataFrame, with Header and InferSchema set True

    :param spark: The Spark session used to read the data.
    :type spark: pyspark.sql.SparkSession
    :param input_path: Path to the input CSV file.
    :type input_path: str
    :return: A Spark DataFrame containing the data from the CSV file.
    :rtype: pyspark.sql.DataFrame
    """
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    return df


def list_analysis_options(analysis_descriptions):
    """
    List all available accident analysis options for the --analysis parameter and their descriptions.

    :param analysis_descriptions: A dictionary containing the available analysis methods and their descriptions.
    :type analysis_descriptions: dict
    :return: None
    """
    print("Available Analysis Options:")
    print("Use the corresponding number for running the listed analysis")
    print("-----------------------------------------")
    for analysis, description in analysis_descriptions.items():
        print(f"{analysis}: {description}")
    print("-----------------------------------------")


def call_analysis_methods(analysis, required_analysis):
    """
    Function dynamically calls the appropriate analysis method based on the user's input.

    :param analysis: The analysis object containing the methods to be called.
    :type analysis: VehicleCrashAnalysis
    :param required_analysis: The name of the analysis to run.
    :type required_analysis: str
    :return: None
    """

    if required_analysis == "1":
        analysis.male_accident_deaths()

    elif required_analysis == "2":
        analysis.num_of_two_wheelers()

    elif required_analysis == "3":
        analysis.top_five_vehicles()

    elif required_analysis == "4":
        analysis.valid_drivers_license_hnr()

    elif required_analysis == "5":
        analysis.highest_no_female_accident_states()

    elif required_analysis == "6":
        analysis.top_veh_make_ids()

    elif required_analysis == "7":
        analysis.top_ethnicity()

    elif required_analysis == "8":
        analysis.top_alcohol_crash_zip()

    elif required_analysis == "9":
        analysis.no_damages_insurance()

    elif required_analysis == "10":
        analysis.top_5_vehicles_speeding()

    else:
        analysis.execute_all()
