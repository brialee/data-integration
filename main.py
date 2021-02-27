import pandas as PD


# The columns we're interested in keeping
covid_cols = ['date', 'county', 'state', 'cases', 'deaths',]
census_cols = ['state', 'county', 'TotalPop', 'IncomePerCap', 'Poverty',]

# DataFrames from CSV with only the columns we need
covid_df = PD.read_csv('COVID_county_data.csv', usecols=covid_cols)
acs_df = PD.read_csv('acs2017_census_tract_data.csv', usecols=census_cols)


'''
Question​: Show your aggregated county-level data rows for the following counties: Loudon County Virginia, Washington County Oregon, Harlan County Kentucky, Malheur County Oregon
'''

# Aggregate census data to state | county | total population | mean poverty | mean income per capita
# With one row per county
census_agg_data = acs_df.groupby(['state', 'county']).agg(
    total_population= PD.NamedAgg(column='TotalPop', aggfunc='sum'),
    mean_poverty = PD.NamedAgg(column='Poverty', aggfunc='mean'),
    mean_incomepercap = PD.NamedAgg(column='IncomePerCap', aggfunc='mean')
).reset_index()

print(
    census_agg_data.loc[(census_agg_data['state'] == "Oregon") & (census_agg_data['county'].isin([ "Washington", "Malheur"]))], end="\n"*4
)

print(
    census_agg_data.loc[(census_agg_data['state'] == "Virginia") & (census_agg_data['county'] == "Loudoun")], end="\n"*4
)

print(
    census_agg_data.loc[(census_agg_data['state'] == "Kentucky") & (census_agg_data['county'] == "Harlan")], end="\n"*4
)

'''
Question​: Show your simplified COVID data for the counties listed above.
'''

# Limit data to Dec. 2020
covid_december_2020_df = covid_df[
    (covid_df['date'] >= '2020-12-01') & (covid_df['date'] <= '2020-12-31')
]

# Aggregate into state | county | total deaths | total cases
deaths_by_counties_dec = covid_december_2020_df.groupby(['state', 'county']).agg(
    total_deaths = PD.NamedAgg(column='deaths', aggfunc='sum'),
    total_cases = PD.NamedAgg(column='cases', aggfunc='sum')
).reset_index()

# Aggregate into state | county | total deaths | total cases
deaths_by_counties_historic = covid_df.groupby(['state', 'county']).agg(
    total_deaths = PD.NamedAgg(column='deaths', aggfunc='sum'),
    total_cases = PD.NamedAgg(column='cases', aggfunc='sum')
).reset_index()

print(
    deaths_by_counties_dec.loc[(deaths_by_counties_dec['state'] == "Oregon") & (deaths_by_counties_dec['county'].isin([ "Washington", "Malheur"]))], end="\n"*4
)

print(
    deaths_by_counties_dec.loc[(deaths_by_counties_dec['state'] == "Virginia") & (deaths_by_counties_dec['county'] == "Loudoun")], end="\n"*4
)

print(
    deaths_by_counties_dec.loc[(deaths_by_counties_dec['state'] == "Kentucky") & (deaths_by_counties_dec['county'] == "Harlan")], end="\n"*4
)



'''
Question​: List your integrated data for all counties in the State of Oregon.
'''
# Merge dataset on matching state and county columns
merged_df = PD.merge(deaths_by_counties_dec, census_agg_data, on=['state', 'county'])
merged_historic_df = PD.merge(deaths_by_counties_historic, census_agg_data, on=['state', 'county'])

# For adding 'cases per 100k residents' column
def adj_cases(row):
    return (row['total_cases'] * 100000) / row['total_population']

# For adding 'deaths per 100k residents' column
def adj_deaths(row):
    return (row['total_deaths'] * 100000) / row['total_population']

# Add the new columns to both datasets
merged_df['cases per 100k'] = merged_df.apply(lambda row: adj_cases(row), axis=1)
merged_df['deaths per 100k'] = merged_df.apply(lambda row: adj_deaths(row), axis=1)
merged_historic_df['cases per 100k'] = merged_historic_df.apply(lambda row: adj_cases(row), axis=1)
merged_historic_df['deaths per 100k'] = merged_historic_df.apply(lambda row: adj_deaths(row), axis=1)

print(
    merged_df.loc[(merged_df['state'] == "Oregon") & (merged_df['county'].isin([ "Washington", "Malheur"]))], end="\n"*4
)

print(
    merged_df.loc[(merged_df['state'] == "Virginia") & (merged_df['county'] == "Loudoun")], end="\n"*4
)

print(
    merged_df.loc[(merged_df['state'] == "Kentucky") & (merged_df['county'] == "Harlan")], end="\n"*4
)


def pearson_correlation(df, k1, k2):
    return df[k1].corr(df[k2])

def should_make_plot(df, k1, k2):
    pc = pearson_correlation(df, k1, k2)
    return pc > 0.5 and pc < -0.5

'''
Analysis
Across all of the counties in the entire USA
a. COVID total cases vs. % population in poverty -
b. COVID total deaths vs. % population in poverty -
c. COVID total cases vs. Per Capita Income level -
d. COVID total cases vs. Per Capita Income level -
e. COVID cases during December 2020 vs. % population in poverty -
f. COVID deaths during December 2020 vs. % population in poverty -
g. COVID cases during December 2020 vs. Per Capita Income level -
h. COVID cases during December 2020 vs. Per Capita Income level -


Across all of the counties in the State of Oregon
a. COVID total cases vs. % population in poverty
b. COVID total deaths vs. % population in poverty
c. COVID total cases vs. Per Capita Income level
d. COVID total cases vs. Per Capita Income level
e. COVID cases during December 2020 vs. % population in poverty -
f. COVID deaths during December 2020 vs. % population in poverty -
g. COVID cases during December 2020 vs. Per Capita Income level -
h. COVID cases during December 2020 vs. Per Capita Income level -
'''


# Dec 2020 for all counties in USA :: NOTE all are false
# e. COVID cases during December 2020 vs. % population in poverty
#print(should_make_plot(merged_df, 'cases per 100k', 'mean_poverty'))

# f. COVID deaths during December 2020 vs. % population in poverty
#print(should_make_plot(merged_df, 'deaths per 100k', 'mean_poverty'))

# g. COVID cases during December 2020 vs. Per Capita Income level
#print(should_make_plot(merged_df, 'cases per 100k', 'mean_incomepercap'))

# h. COVID cases during December 2020 vs. Per Capita Income level
#print(should_make_plot(merged_df, 'deaths per 100k', 'mean_incomepercap'))


# Dec 2020 for all counties in Oregon :: NOTE all are false
#OR_only_2020 = merged_df.loc[(merged_df['state'] == "Oregon")]

# e. COVID cases during December 2020 vs. % population in poverty
#print(should_make_plot(OR_only_2020, 'cases per 100k', 'mean_poverty'))

# f. COVID deaths during December 2020 vs. % population in poverty
#print(should_make_plot(OR_only_2020, 'deaths per 100k', 'mean_poverty'))

# g. COVID cases during December 2020 vs. Per Capita Income level
#print(should_make_plot(OR_only_2020, 'cases per 100k', 'mean_incomepercap'))

# h. COVID cases during December 2020 vs. Per Capita Income level
#print(should_make_plot(OR_only_2020, 'deaths per 100k', 'mean_incomepercap'))


# Historic all counties in USA :: NOTE all are false
# a. COVID total cases vs. % population in poverty
#print(should_make_plot(merged_historic_df, 'cases per 100k', 'mean_poverty'))

# b. COVID total deaths vs. % population in poverty
#print(should_make_plot(merged_historic_df, 'deaths per 100k', 'mean_poverty'))

# c. COVID total cases vs. Per Capita Income level
#print(should_make_plot(merged_historic_df, 'cases per 100k', 'mean_incomepercap'))

# d. COVID total cases vs. Per Capita Income level
#print(should_make_plot(merged_historic_df, 'deaths per 100k', 'mean_incomepercap'))


# Historic across all of the counties in the State of Oregon:: NOTE all are false
#OR_only_historic = merged_historic_df.loc[(merged_df['state'] == "Oregon")]

# a. COVID total cases vs. % population in poverty
#print(should_make_plot(OR_only_historic, 'cases per 100k', 'mean_poverty'))

# b. COVID total deaths vs. % population in poverty
#print(should_make_plot(OR_only_historic, 'deaths per 100k', 'mean_poverty'))

# c. COVID total cases vs. Per Capita Income level
#print(should_make_plot(OR_only_historic, 'cases per 100k', 'mean_poverty'))

# d. COVID total cases vs. Per Capita Income level
#print(should_make_plot(OR_only_historic, 'cases per 100k', 'mean_poverty'))