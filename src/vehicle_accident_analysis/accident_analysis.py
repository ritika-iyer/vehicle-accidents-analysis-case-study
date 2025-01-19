import pyspark.sql.functions as F
from pyspark.sql import Window
from utils import load_csv_data
from constants import (
    valid_licenses,
    styles_to_exclude,
    unavailable_ethnicities,
    not_applicable,
)


class VehicleCrashAnalysis:
    """
    Class for analyzing vehicle crash data. This class contains methods to compute various statistics
    related to crashes, such as the number of accidents involving males, the number of two-wheeled vehicles
    involved in crashes, and other analyses based on the provided input data.

    Attributes:
        input_path (str): Path to the primary data containing person crash information.
        units_use_path (str): Path to the data regarding vehicle units.
        damages_path (str): Path to the crash damages data.
        charges_path (str): Path to the charges data.
        output_path_1 (str): Path to the output for the first analysis.
        output_path_2 (str): Path to the output for the second analysis.
        output_path_3 (str): Path to the output for the third analysis.
        output_path_4 (str): Path to the output for the fourth analysis.
        output_path_5 (str): Path to the output for the fifth analysis.
        output_path_6 (str): Path to the output for the sixth analysis.
        output_path_7 (str): Path to the output for the seventh analysis.
        output_path_8 (str): Path to the output for the eighth analysis.
        output_path_9 (str): Path to the output for the ninth analysis.
        output_path_10 (str): Path to the output for the tenth analysis.
        spark (SparkSession): Spark session object for processing data.
    """

    def __init__(self, config, spark):
        """
        Initializes the VehicleCrashAnalysis class by loading the paths for input and output data
        from config file and sets up the Spark session.

        :param config: A dictionary containing paths to various input and output files.
        :type config: dict
        :param spark: A SparkSession object used for data processing.
        :type spark: pyspark.sql.SparkSession
        """
        self.primary_path = config["primary_person_path"]
        self.units_use_path = config["units_use_path"]
        self.damages_path = config["damages_path"]
        self.charges_path = config["charges_path"]
        self.output_path_1 = config["analysis_1_output"]
        self.output_path_2 = config["analysis_2_output"]
        self.output_path_3 = config["analysis_3_output"]
        self.output_path_4 = config["analysis_4_output"]
        self.output_path_5 = config["analysis_5_output"]
        self.output_path_6 = config["analysis_6_output"]
        self.output_path_7 = config["analysis_7_output"]
        self.output_path_8 = config["analysis_8_output"]
        self.output_path_9 = config["analysis_9_output"]
        self.output_path_10 = config["analysis_10_output"]
        self.spark = spark

    def male_accident_deaths(self):
        """
        Analytics 1: Find the number of crashes (accidents) in which number of males killed are
        greater than 2

        This method filters the dataset for male persons involved in crashes, and then counts the
        number of crashes where the number of males involved exceeds two.

        :return: The count of accidents involving more than two males.
        :rtype: int

        """

        person_df = load_csv_data(self.spark, self.primary_path)

        person_df_1 = person_df.dropDuplicates()
        total_crashes_males = (
            person_df_1.filter(
                (F.col("PRSN_GNDR_ID") == "MALE")
                & (F.col("PRSN_INJRY_SEV_ID") == "KILLED")
            )
            .groupBy("CRASH_ID")
            .count()
        )

        # Filtering for crashes with >2 male deaths

        total_crashes_males = total_crashes_males.filter(
            F.col("count") > 2
        ).withColumnRenamed("count", "count_accident_male_deaths_over_two")

        total_crashes_males_count = total_crashes_males.count()

        total_crashes_males.show()
        print(
            f"Number of crashes (accidents) in which number of males killed are greater than 2: {total_crashes_males_count}"
        )
        total_crashes_males.write.parquet(self.output_path_1)
        return total_crashes_males_count

    def num_of_two_wheelers(self):
        """
        Analysis 2: How many two wheelers are booked for crashes?

        Filters the dataset for distinct vehicles with body style containing "MOTORCYCLE" and
        counts the vehicles involvedin crashes.

        :return: The number of motorcycles involved in crashes.
        :rtype: int

        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        result_df = units_df.filter(F.col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        result_df = result_df.filter(F.col("VIN").isNotNull())
        result_df = result_df.select("VIN").distinct()

        result_df.show()
        result = result_df.count()
        print(
            "How many two wheelers are booked for crashes?:",
            "\n",
            "(Considering only UNIQUE VINs as a two wheeler booked and removing nulls, even if same maybe VIN is involved in more than one crash (CRASH_ID))",
            f"{result}",
        )
        result_df.write.parquet(self.output_path_2)

        return result

    def top_five_vehicles(self):
        """
        Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

        Joins the vehicle and primary person datasets, filters for crashes where the driver was killed,
        airbags were not deployed, and the vehicle make is not "NA". Groups by vehicle make and returns
        the top 5 vehicle makes with the most fatal crashes.

        :return: A DataFrame containing the top 5 vehicle makes involved in fatal crashes.
        :rtype: list

        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        primary_person_df = load_csv_data(self.spark, self.primary_path)
        fatal_crashes_no_airbag = units_df.join(primary_person_df, ["CRASH_ID"])
        fatal_crashes_no_airbag = (
            fatal_crashes_no_airbag.filter(
                (F.col("PRSN_TYPE_ID") == "DRIVER")
                & (F.col("PRSN_INJRY_SEV_ID") == "KILLED")
                & (F.col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                & (F.col("VEH_MAKE_ID") != "NA")
            )
            .groupBy("VEH_MAKE_ID")
            .agg(F.count("*").alias("CRASH_COUNT"))
            .orderBy(F.col("CRASH_COUNT").desc())
            .limit(5)
        )
        distinct_vehicle_makes = fatal_crashes_no_airbag.select(
            "VEH_MAKE_ID"
        ).distinct()

        distinct_vehicle_makes_list = distinct_vehicle_makes.collect()
        final_list = []
        for row in distinct_vehicle_makes_list:
            final_list.append(row["VEH_MAKE_ID"])

        fatal_crashes_no_airbag.show()
        print(
            "----------------------------------------------------------------------------------------------------------------"
        )
        print(
            f"Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy: {final_list} "
        )

        fatal_crashes_no_airbag.write.parquet(self.output_path_3)
        print(
            "----------------------------------------------------------------------------------------------------------------"
        )
        return final_list

    def valid_drivers_license_hnr(self):
        """
        Analytics 4: Determine number of Vehicles with driver having valid licences involved in hit and run?

        Joins the vehicle and primary person datasets, filters for hit-and-run accidents where the driver has
        a valid license (either "DRIVER LICENSE" or "COMMERCIAL DRIVER LIC.") and counts the number of distinct
        vehicles (VIN) involved in such accidents. The count includes multiple instances of the same VIN if it
        appears in more than one crash.

        :return: The total number of vehicles with a driver having a valid license involved in hit-and-run accidents.
        :rtype: int

        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        primary_person_df = load_csv_data(self.spark, self.primary_path)

        valid_lic_hit_and_run = (
            units_df.join(primary_person_df, ["CRASH_ID"])
            .filter(
                (F.col("VEH_HNR_FL") == "Y")
                & (F.col("DRVR_LIC_TYPE_ID").isin(valid_licenses))
            )
            .select("CRASH_ID", "VEH_HNR_FL", "DRVR_LIC_TYPE_ID", "VIN")
        )
        print(
            "Counting each CRASH_ID x VIN as a unique entry of vehicles with driver having valid licences \
            involved in hit and run, so same VIN may have more than one CRASH_IDs but is counted each time"
        )
        result = valid_lic_hit_and_run.count()
        valid_lic_hit_and_run.show()
        print(
            "-----------------------------------------------------------------------------------"
        )
        print(
            f"Number of Vehicles with driver having valid licences involved in hit and run: \
            {result}"
        )
        print(
            "------------------------------------------------------------------------------------"
        )
        valid_lic_hit_and_run.write.parquet(self.output_path_4)
        return result

    def highest_no_female_accident_states(self):
        """
        Analysis 5: Which state has highest number of accidents in which females are not involved?

        Filters the dataset to exclude accidents where females are involved, groups by state, and finds
        the state with the highest number of such accidents.

        :return: The state with the highest number of accidents in which females are not involved.
        :rtype: str


        """

        primary_person_df = load_csv_data(self.spark, self.primary_path)

        highest_no_fem_acc_df = (
            primary_person_df.filter(F.col("PRSN_GNDR_ID") != "FEMALE")
            .groupBy("DRVR_LIC_STATE_ID")
            .count()
            .withColumnRenamed("count", "driver_count")
            .orderBy(F.col("driver_count").desc())
            .limit(1)
        )
        highest_no_fem_acc = highest_no_fem_acc_df.first()["DRVR_LIC_STATE_ID"]
        highest_no_fem_acc_df.write.parquet(self.output_path_5)
        highest_no_fem_acc_df.show()

        print(
            "----------------------------------------------------------------------------"
        )
        print(
            f"State which has highest number of accidents in which females are not involved: \
              {highest_no_fem_acc}"
        )
        print(
            "-----------------------------------------------------------------------------"
        )
        return highest_no_fem_acc

    def top_veh_make_ids(self):
        """
        Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including
        death

        Filters dataset for vehicles with valid `VEH_MAKE_ID`, calculates the total casualties
        (sum of injuries and deaths) and groups by `VEH_MAKE_ID` to find the total number of casualties
        for each vehicle make. Returns the 3rd to 5th vehicle makes based on the largest number of casualties.

        :return: A DataFrame containing the 3rd to 5th vehicle makes contributing to the largest number of casualties.
        :rtype: pyspark.sql.DataFrame
        """

        units_df = load_csv_data(self.spark, self.units_use_path)

        vehicle_casualties = units_df.filter(
            F.col("VEH_MAKE_ID").isNotNull() & (F.col("VEH_MAKE_ID") != "NA")
        ).withColumn("TOT_CASUALITIES", F.col("TOT_INJRY_CNT") + F.col("DEATH_CNT"))
        vehicle_casualties = (
            vehicle_casualties.groupBy("VEH_MAKE_ID")
            .agg(F.sum("TOT_CASUALITIES").alias("TOTAL_CASUALTIES_COUNT"))
            .orderBy(F.col("TOTAL_CASUALTIES_COUNT").desc())
        )

        top_5 = vehicle_casualties.limit(5)
        top_2 = vehicle_casualties.limit(2)
        ranked_3_to_5 = top_5.subtract(top_2)
        ranked_3_to_5_list = [row.VEH_MAKE_ID for row in ranked_3_to_5.collect()]
        ranked_3_to_5.write.parquet(self.output_path_6)
        ranked_3_to_5.show()
        print(
            f"Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death: \
            {ranked_3_to_5_list}"
        )
        return ranked_3_to_5

    def top_ethnicity(self):
        """
        Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique
        body style

        Joins the vehicle and primary person datasets, filters out rows with unavailable or unknown body styles
        and ethnicities, then calculates the ethnicity count for each body style. Ranks ethnicities within each
        body style based on the count and returns the top ethnic group for each body style.

        :return: A DataFrame containing the top ethnic group for each vehicle body style.
        :rtype: pyspark.sql.DataFrame

        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        person_df = load_csv_data(self.spark, self.primary_path)

        ethnicity_counts = (
            units_df.join(person_df, on=["CRASH_ID"])
            .filter(~F.col("VEH_BODY_STYL_ID").isin(styles_to_exclude))
            .filter(~F.col("PRSN_ETHNICITY_ID").isin(unavailable_ethnicities))
            .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            .agg(F.count("*").alias("ethnicity_count"))
        )
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(
            F.col("ethnicity_count").desc()
        )
        ranked_ethnicities = ethnicity_counts.withColumn(
            "rank", F.rank().over(window_spec)
        )

        top_ethnicities_per_body_style = ranked_ethnicities.filter(F.col("rank") == 1)
        top_ethnicities_per_body_style.show()
        print("Above are the top ethnic user group of each unique body style")
        print("\n")

        top_ethnicities_per_body_style.write.parquet(self.output_path_7)

        return top_ethnicities_per_body_style

    def top_alcohol_crash_zip(self):
        """
        Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes
        with alcohols as the contributing factor to a crash (Use Driver Zip Code)

        Joins the units_use and primary_person datasets, filters the crashes where alcohol or drinking is listed as a
        contributing factor in any of the fields (`CONTRIB_FACTR_1_ID`, `CONTRIB_FACTR_2_ID`, or `CONTRIB_FACTR_P1_ID`).
        Groups results by driver's ZIP code and returns the top 5 ZIP codes with the highest number of such crashes.

        :return: DataFrame containing the top 5 ZIP codes with the highest number of crashes with alcohol as
        the contributing factor
        :rtype: pyspark.sql.DataFrame


        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        person_df = load_csv_data(self.spark, self.primary_path)

        alcohol_crashes = (
            units_df.join(person_df, ["CRASH_ID"])
            .dropna(subset=["DRVR_ZIP"])
            .filter(
                F.col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")
                | F.col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")
                | F.col("CONTRIB_FACTR_P1_ID").contains("ALCOHOL")
                | F.col("CONTRIB_FACTR_1_ID").contains("DRINKING")
                | F.col("CONTRIB_FACTR_2_ID").contains("DRINKING")
                | F.col("CONTRIB_FACTR_P1_ID").contains("DRINKING")
            )
            .groupBy("DRVR_ZIP")
            .agg(F.count("*").alias("crash_count"))
            .orderBy(F.col("crash_count").desc())
            .limit(5)
        )
        alcohol_crashes.show()
        print(
            f"Above are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)"
        )

        alcohol_crashes.write.parquet(self.output_path_8)
        return alcohol_crashes

    def no_damages_insurance(self):
        """
        Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed
        and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

        This method filters crashes that meet the following conditions:
        - No property damage is recorded (`DAMAGED_PROPERTY` is "NONE" or contains "NO DAMAGE").
        - The damage scale (`VEH_DMAG_SCL_1_ID` or `VEH_DMAG_SCL_2_ID`) exceeds "DAMAGED 4"
        - Vehicle involved in the crash is covered by insurance (`FIN_RESP_TYPE_ID` contains "INSURANCE").

        Returns the count of such crashes.

        :return: The count of distinct crash IDs that meet the criteria for no property damage,
                damage level above 4, and insurance.
        :rtype: int

        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        damages_df = load_csv_data(self.spark, self.damages_path)

        # Crashes must meet all criteria

        crashes_with_damage = (
            damages_df.join(units_df, ["CRASH_ID"])
            .filter(
                # either one of damage scales must meet the conditions
                (
                    (F.col("VEH_DMAG_SCL_1_ID") > "DAMAGED 4")
                    & (~F.col("VEH_DMAG_SCL_1_ID").isin(not_applicable))
                )
                | (
                    (F.col("VEH_DMAG_SCL_2_ID") > "DAMAGED 4")
                    & (~F.col("VEH_DMAG_SCL_2_ID").isin(not_applicable))
                )
            )
            .filter(
                (F.col("DAMAGED_PROPERTY") == "NONE")
                | (F.col("DAMAGED_PROPERTY").contains("NO DAMAGE"))
            )
            .filter(F.col("FIN_RESP_TYPE_ID").contains("INSURANCE"))
            .select("CRASH_ID")
            .distinct()
        )
        crashes_with_damage.write.parquet(self.output_path_9)
        result = crashes_with_damage.count()
        crashes_with_damage.show()
        print(
            f" Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance: {result}"
        )

        return result

    def top_5_vehicles_speeding(self):
        """
        Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with
        highest number of offences (to be deduced from the data)

        This analysis joins data from the units, charges, and primary person datasets to filter for crashes
        where :
        - the charge is for speeding
        - the driver holds a valid license
        - the tvehicles are of top 10 most frequent colors
        - in states with the top 25 highest number of offenses.

        The result is a list of top 5 vehicle makes with the highest number of speeding offenses.

        :return: DataFrame containing the top 5 vehicle makes with the highest number of speeding offenses.
        :rtype: pyspark.sql.DataFrame
        """

        units_df = load_csv_data(self.spark, self.units_use_path)
        charges_df = load_csv_data(self.spark, self.charges_path)
        person_df = load_csv_data(self.spark, self.primary_path)

        colours_df = (
            units_df.filter(F.col("VEH_COLOR_ID") != "NA")
            .groupBy("VEH_COLOR_ID")
            .agg(F.count("*").alias("colour_freq"))
            .orderBy(F.col("colour_freq").desc())
            .limit(10)
        )
        top_10_colors = [row.VEH_COLOR_ID for row in colours_df.collect()]
        top_25_highest_offenses_states_df = (
            units_df.filter(F.col("VEH_LIC_STATE_ID").cast("int").isNull())
            .groupBy("VEH_LIC_STATE_ID")
            .agg(F.count("*").alias("num_of_offenses"))
            .orderBy(F.col("num_of_offenses").desc())
            .limit(25)
        )
        top_25_highest_offenses_states = [
            row.VEH_LIC_STATE_ID for row in top_25_highest_offenses_states_df.collect()
        ]
        valid_license = ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
        top_5_vehicles_with_speeding_charge = (
            charges_df.join(person_df, ["CRASH_ID"])
            .join(units_df, ["CRASH_ID"])
            .filter(
                (F.col("CHARGE").contains("SPEED"))
                & (F.col("DRVR_LIC_TYPE_ID").isin(valid_license))
                & (F.col("VEH_COLOR_ID").isin(top_10_colors))
                & (F.col("VEH_LIC_STATE_ID").isin(top_25_highest_offenses_states))
            )
            .groupBy("VEH_MAKE_ID")
            .agg(F.count("*").alias("num_of_speeding_offenses"))
            .orderBy(F.col("num_of_speeding_offenses").desc())
            .limit(5)
        )

        top_5_vehicles_with_speeding_charge.show()

        print(
            "Above are the Top 5 Vehicle Makes where drivers are charged with speeding related offences (with given conditions)"
        )

        top_5_vehicles_with_speeding_charge.write.parquet(self.output_path_10)
        return top_5_vehicles_with_speeding_charge

    def execute_all(self):
        """
        Executes all the analysis methods of the `VehicleCrashAnalysis` class sequentially.

        This method runs all the individual analysis methods in the `VehicleCrashAnalysis` class
        for processing and computing various statistics related to vehicle crash data.

        :return: None
        :rtype: None

        """
        VehicleCrashAnalysis.male_accident_deaths(self)
        VehicleCrashAnalysis.num_of_two_wheelers(self)
        VehicleCrashAnalysis.top_five_vehicles(self)
        VehicleCrashAnalysis.valid_drivers_license_hnr(self)
        VehicleCrashAnalysis.highest_no_female_accident_states(self)
        VehicleCrashAnalysis.top_veh_make_ids(self)
        VehicleCrashAnalysis.top_ethnicity(self)
        VehicleCrashAnalysis.top_alcohol_crash_zip(self)
        VehicleCrashAnalysis.no_damages_insurance(self)
        VehicleCrashAnalysis.top_5_vehicles_speeding(self)
