"""Static reference data: GB regions and fuel groupings."""

# (region_id, short_name, country, is_aggregate)
# 1-14 are DNO regions; 15-17 are country roll-ups returned by the API.
REGIONS = [
    (1, "North Scotland", "Scotland", False),
    (2, "South Scotland", "Scotland", False),
    (3, "North West England", "England", False),
    (4, "North East England", "England", False),
    (5, "Yorkshire", "England", False),
    (6, "North Wales & Merseyside", "Wales", False),
    (7, "South Wales", "Wales", False),
    (8, "West Midlands", "England", False),
    (9, "East Midlands", "England", False),
    (10, "East England", "England", False),
    (11, "South West England", "England", False),
    (12, "South England", "England", False),
    (13, "London", "England", False),
    (14, "South East England", "England", False),
    (15, "England", "England", True),
    (16, "Scotland", "Scotland", True),
    (17, "Wales", "Wales", True),
]

FUELS = ["biomass", "coal", "gas", "hydro", "imports", "nuclear", "oil", "other", "solar", "wind"]

# Biomass is excluded from "renewable" because its carbon accounting is contested.
RENEWABLE_FUELS = ["wind", "solar", "hydro"]
LOW_CARBON_FUELS = RENEWABLE_FUELS + ["nuclear"]
FOSSIL_FUELS = ["gas", "coal", "oil"]
