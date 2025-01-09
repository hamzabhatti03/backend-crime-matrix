"""SCRIPT FOR ALL VALIDATION FUNCTIONS"""
import re
from datetime import datetime
from Utilities import configs

"""District Validation"""
def validating_district(district):
    # Convert the input to a string to handle both int and str types
    district_str = str(district)

    # Check if the district is a numeric string (or int) and is between 1 and 40
    if re.match(r'^\d+$', district_str):
        district_num = int(district_str)
        if 1 <= district_num <= 40:
            return True
    return False


"""Date Validation"""
def validating_date(date_str):
    try:
        return datetime.strptime(date_str, configs.YM_DATE)
    except ValueError:
        return None