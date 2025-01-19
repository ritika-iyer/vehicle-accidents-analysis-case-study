# Vehicle Accident Analysis: Case Study

This is a Pyspark Application built for the analysis of vehicle accidents data in the US and computing several statistics concerning these accidents and the demographics involved.

This application is config-driven, modularized with classes, functions and follows coding best-practices. 

## Pre-requisites
Listed here the pre-requisites for running this application:

- Python 3.10 +
- Apache Spark 3.5.4 (Will also require compatible Java and Hadoop installations and Environment Variables set up if running locally)

### Other Dependencies
All required Python packages are listed in `requirements.txt`:

- py4j==0.10.9.7
- pyspark==3.5.4
- PyYAML==6.0.2


## Installation Guide
The following must be set up for running this application:

1. Install Python (Application tested with version 3.13.1)

2. Install Apache Spark (Application tested with version 3.5.4)

3. Install dependencies from requirements.txt



## Analytics

The application allows to perform the following list of analyses: 	

1. Analysis 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
2. Analysis 2: How many two wheelers are booked for crashes? 
3. Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4. Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5. Analysis 5: Which state has highest number of accidents in which females are not involved? 
6. Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7. Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8. Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9. Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10. Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

## Expected Output:
1.	Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
2.	Code should be properly organized in folders as a project.
3.	Input data sources and output should be config driven
4.	Code should be strictly developed using Data Frame APIs (Do not use Spark SQL)
5.	Share the entire project as zip or link to project in GitHub repo.


`Note`: Application was tested on Windows [Version 10.0.22631.4751]

## Inputs Data Sources

The input data is present in the Data folder.

These are the input datasets:
- `Charges_use.csv`: data on charges filed in accidents
- `Damages_use.csv`: data on damages to vehicles and property
- `Endorse_use.csv`: data on driver endorsement 
- `Primary_Person_use.csv`: data on primary persons in the accidents
- `Units_use.csv`: data on the vehicles in the accidents
- `Restrict_use.csv`: data on driver restrictions

## Config Set Up

Under the project folder, navigate to config/config.yaml.
You will find the option to configure the input data paths as well as the output paths for each analysis method.

In addition to this, you may also configure the app-name of the sparksession that will be created for the data processing, as well the executor core and driver memory. Default values are already set in the config file.


## Running the Application using spark-submit command:

For information on the command to be run and the custom arguments set for running the application, you may use the following command:

`spark-submit --conf spark.pyspark.python=python --conf spark.pyspark.driver.python=python src/main.py --help`

For listing all the available analysis methods you can use --list_analysis_options :

`spark-submit --conf spark.pyspark.python=python --conf spark.pyspark.driver.python=python src/main.py --list_analysis_options`

For running each individual analysis from the available options, use --analysis:

For instance, if you want to run the analysis:
1: Find the number of crashes (accidents) in which number of males killed are greater than 2?

Use Command:

`spark-submit --conf spark.pyspark.python=python --conf spark.pyspark.driver.python=python src/main.py --analysis 1`

For Running all the available analysis options sequentially, you can omit using the additional arguments, by default it will run all methods:

`spark-submit --conf spark.pyspark.python=python --conf spark.pyspark.driver.python=python src/main.py`

`Note`: If files from previous runs already exist in the output paths given, please move the files elsewhere or delete them.
If you wish to delete them, you may use the following powershell command (for windows):
```
if (Test-Path "./outputs/") {
    Remove-Item "./outputs/" -Recurse -Force
}
```

## Outputs

Outputs of each analysis are stored in the outputs folder. You may re-configure these paths using the config file.

## Folder Structure
Below is the folder structure of the project. Please note that the actual output files generated while testing have not been uploaded to the repository, but once generated by running the previously stated commands, the code structure will be as below.

```
vehicle_accident_case_study/
├── config/
│   └── config.yaml
├── Data/                           
│   ├── Charges_use.csv            
│   ├── Damages_use.csv            
│   ├── Endorse_use.csv            
│   ├── Primary_Person_use.csv     
│   ├── Restrict_use.csv           
│   └── Units_use.csv   
├── outputs/
│   ├── no_of_acc_male_deaths/                        
│   ├── no_of_two_wheelers/                        
│   ├── top_five_vehicles/                        
│   ├── valid_drivers_license_hnr/                        
│   ├── highest_no_female_accident_states/                        
│   ├── top_veh_make_ids/                       
│   ├── top_ethnicity/                        
│   ├── top_alcohol_crash_zip/                       
│   ├── no_damages_insurance/                      
│   └── top_5_vehicles_speeding/ 
├── outputs_screenshots/
│    └── ..                                         # screenshots of all analysis runs as tested on 2025-01-20 (Windows OS)
├── src/
│   ├── vehicle_accident_analysis/
│   │   └── accident_analysis.py
│   ├── constants.py
│   ├── main.py
│   └── utils.py
├── BCG_Case_Study_CarCrash_Updated_Questions.docx
├── Data Dictionary.xlsx
├── requirements.txt
└── README.md


```

