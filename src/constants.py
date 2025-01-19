analysis_descriptions = {
    "1": "Find the number of crashes (accidents) in which number of males killed are greater than 2?",
    "2": "How many two wheelers are booked for crashes?",
    "3": "Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.",
    "4": "Determine number of Vehicles with driver having valid licences involved in hit and run?",
    "5": "Which state has highest number of accidents in which females are not involved?",
    "6": "Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death",
    "7": "For all the body styles involved in crashes, mention the top ethnic user group of each unique body style",
    "8": "Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)",
    "9": "Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance",
    "10": "Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)",
}
valid_licenses = ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
styles_to_exclude = [
    "NA",
    "UNKNOWN",
    "NOT REPORTED",
    "OTHER  (EXPLAIN IN NARRATIVE)",
]
unavailable_ethnicities = ["NA", "UNKNOWN", "OTHER"]
not_applicable = ["NA", "NO DAMAGE", "INVALID VALUE"]
