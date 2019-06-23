# -*- coding: utf-8 -*-
"""Script with methods for cleaning up the Philips and WW encounterIds.

In WardWatcher (WW) the encounterId is called 'CIS Patient ID'.

There are a number of known issues with the Philips encounterIds, which
have been identified by Josh Inoue (UHB).
The causes or error are described in 'Philips encounterId Read Me.txt'
and the erroneous/duplicate Ids are tracked in 'Philips encounterId Issue List (New).xlsx'

Issues with the WW CIS Patient ID are fewer and are currently adjusted manually in this script. 
In the future they will be logged and autmoatically corrected.
"""

## Note: the 'c engine' is more robust for pandas.read_csv() but it does not allow 
## 	 'skip_footer', so the footer (final five lines) must be deleted manually
##	 from the file 'encounter_summary.rpt'
import pandas as pd
import numpy as np

def df_summary(df, verbose=True):
	""" Print a summary of a dataframe.

	If verbose=False just prints head of the dataframe.
	"""
	if verbose:
		print("This dataframe has %d rows." %len(df))
		print("The datatypes are:", '\n', df.dtypes)
	print('\n', df.head(), '\n')


def clean_icnarc_cis_ids(icnarc_cis_id_file, erroneous_ids, verbose=True):
	""" Replace CIS Patient IDs which point to the wrong Philips encounterId.

	Erroneous IDs are tracked in sheet 'WW' of 'Philips encounterId Issue List (New).xlsx'
	"""
	icnarc_numbers = pd.read_csv(icnarc_cis_id_file)
	icnarc_numbers = icnarc_numbers[icnarc_numbers['Unit ID']!=14]  ## remove rows specific to cardiac 
	icnarc_numbers = icnarc_numbers.rename(index=str, columns={'CIS Patient ID':'CIS Patient ID Original'})

	known_errors = pd.read_excel(erroneous_ids, sheet_name='WW')	
	known_errors = known_errors[known_errors['Unit ID']!=14]  ## remove rows specific to cardiac 
	
	new_column = []
	for index, row in icnarc_numbers.iterrows():
		if row['ICNARC number'] in known_errors['ICNARC Number']:
			new_column.append(known_errors.loc[known_errors['ICNARC Number']==row['ICNARC number']]['Corrected encID']) 
		else:
			new_column.append(row['CIS Patient ID Original'])

	icnarc_numbers['CIS Patient ID'] = new_column
	if verbose:
		df_summary(icnarc_numbers, verbose=True)
	return icnarc_numbers

def clean_philips_encounterids(philips_extract, encounter_id_issues, verbose=True, log_error_type=False, date_columns=['inTime','outTime']):
	''' Cleans the encounterIds in a Philips extraction.

	encounterIds to replace are recorded in sheet 'encounterId' of 'Philips encounterId Issue List (New).xlsx'
	'''
	data = pd.read_csv(philips_extract, delimiter='\t', parse_dates=date_columns)
	data = data.rename(index=str, columns={'encounterId':'encounterId_CIS'})	
	if verbose:	
		print("\nDataframe containing encounter summaries from Philips:")
		df_summary(data)
		print("There are %d unique encounterIds." %len(data['encounterId_original'].unique()))

	known_errors = pd.read_excel(encounter_id_issues, sheet_name='encounterId')
	known_errors = known_errors[known_errors.clinicalUnitId!=8.0]  ## remove rows specific to cardiac
	data = data.merge(known_errors, how='left', on='encounterId_CIS')

	if verbose:
		print("\nDataframe containing known enounterId issues in Philip ICCA data:")	
		df_summary(known_errors, verbose=False)
		
	data = data.rename(index=str, columns={'encounterId_CIS':'encounterId_original'})
	data['encounterId_Adjusted'] = data['encounterId_Adjusted'].fillna(data['encounterId_original'])
	if log_error_type:
		data['error_type'] = data['Explanation'].fillna('NA')

	data = data.drop(['clinicalUnitId', 'Explanation'], axis=1)
	data = data.rename(index=str, columns={'encounterId_Adjusted':'encounterId'})
	data['encounterId'] = data['encounterId'].astype(int)

	return data

def join_icnarc_to_philips(philips_data, icnarc_numbers, verbose=True):
	''' Simple merge of Philips extract to ICNARC, joining on cleaned encounterId columns.'''

	icnarc_numbers['encounterId'] = icnarc_numbers['CIS Patient ID'] 
	merged_data = philips_data.merge(icnarc_numbers, on='encounterId').drop(['CIS Patient ID', 'Key'], axis=1)
	if verbose:
		print("\nDataframe containing Philip data joined to ICNARC Id data:")
		df_summary(merged_data)

	return merged_data

def validation(icnarc_numbers, philips_data, merged_data, view_non_unique_ids=False):

	print('\n\n')
	print("There are %d ICNARC numbers." %len(icnarc_numbers))
	print("There are %d ICNARC numbers with an associated CIS Patient ID." %len(icnarc_numbers.dropna(subset={'CIS Patient ID'})))
	print('\n')
	print("There are %d rows in the Philips data," %len(philips_data['CIS Patient ID']))
	print("And %d unique encounterIds." %len(philips_data['CIS Patient ID'].unique()))
	print('\n')
	print("In the merged data there are %d rows." %len(merged_data['CIS Patient ID']))
	print("And %d unique encounterIds." %len(merged_data['CIS Patient ID'].unique()))
	nans = len(merged_data['CIS Patient ID']) - len(merged_data.dropna(subset={'CIS Patient ID'})) 
	print("There are %d rows with missing CIS Patient ID." %nans)

	if view_non_unique_ids:

		groups = merged_data.groupby(['CIS Patient ID'], as_index=False).count() 

		gids = groups.loc[groups['age']==2]['CIS Patient ID']
		print(gids,'\n')
		for gid in gids:
			print(merged_data.loc[merged_data['CIS Patient ID']==gid])

	print(merged_data.columns)

def _get_err(x):
	err = x[x!='NA']
	if len(err)==0:
		return 'NA'
	else:
		return err.values[0]

def combine_non_unique_philips_encounters(merged_data, combine='concat', verbose=True):
	''' Combining data from ITU stays with multiple encounterIds in Philips.'''

	if combine=='concat':
		groups = merged_data.groupby(['encounterId'], as_index=False).agg({'ptCensusId': ['count', list],
										'age': 'min',
										'inTime':'min',
										'outTime':'max',
										'tNumber':'first',
										'encounterId_original':['count', list],
										'lengthOfStay (mins)':'sum',
										'gender':'last',
										'error_type': _get_err})
		groups.columns = list(map('_'.join, groups.columns.values)) ## remove multi-index
	elif combine=='simple':
		groups = merged_data.groupby(['encounterId'], as_index=False).agg({'ptCensusId': 'first',
										'age': 'min',
										'inTime':'min',
										'outTime':'max',
										'tNumber':'first',
										'encounterId_original':'first',
										'lengthOfStay (mins)':'sum',
										'gender':'first',
										'error_type': _get_err})

	if verbose:
		print('\n', groups.columns)
		print(groups.iloc[:, groups.columns.get_level_values(0).isin({'CIS Patient ID', 'age', 'lengthOfStay (mins)', ''})].head())

	return groups

def combine_non_unique_encounters(merged_data, combine='concat', verbose=True):
	''' Combining data from ITU stays with multiple encounterIds in Philips after joining to ICNARC extraction.'''

	if combine=='concat':
		groups = merged_data.groupby(['CIS Patient ID'], as_index=False).agg({'ptCensusId': ['count', list],
										'age': 'min',
										'inTime':'min',
										'outTime':'max',
										'tNumber':'first',
										'encounterId_original':['count', list],
										'lengthOfStay (mins)':'sum',
										'gender':'first',
										'Unit ID':'min',
										'ICNARC number':['count', list],
										'CIS Patient ID Original':['count', list],
										'CIS Episode ID':['count', list],
										'Readmission during this hospital stay':'first'})
		groups.columns = list(map('_'.join, groups.columns.values)) ## remove multi-index
	elif combine=='simple':
		groups = merged_data.groupby(['CIS Patient ID'], as_index=False).agg({'ptCensusId': 'first',
										'age': 'min',
										'inTime':'min',
										'outTime':'max',
										'tNumber':'first',
										'encounterId_original':'first',
										'lengthOfStay (mins)':'sum',
										'gender':'first',
										'Unit ID':'min',
										'ICNARC number': 'first',
										'CIS Patient ID Original': 'first',
										'CIS Episode ID': 'first',
										'Readmission during this hospital stay':'first'})

	
	if verbose:
		print('\n', groups.columns)
		print(groups.iloc[:, groups.columns.get_level_values(0).isin({'CIS Patient ID', 'age', 'lengthOfStay (mins)', ''})].head())

	return groups

############# We now want to link to ICNARC XML and clean up (document in standard python format) and save results.....

if __name__=="__main__":

	icnarc_numbers = clean_icnarc_cis_ids('../ICNARC 2015-2018 encounterIds and Readmissions.TXT', '../Philips encounterId Issue List (New).xlsx')

	data = clean_philips_encounterids('../encounter_summary (1).rpt', '../Philips encounterId Issue List (New).xlsx')

	merged = join_icnarc_to_philips(data, icnarc_numbers)

	validation(icnarc_numbers, data, merged)

	merged_data = combine_non_unique_encounters(merged)	
