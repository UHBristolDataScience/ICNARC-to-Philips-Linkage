# -*- coding: utf-8 -*-
"""Code for parsing ICNARC xml file.

The function parse_icnarc_xml() reads the standard xml file output by WardWatcher,
converts the CODE names to human-readable description using the CMP description file,
and then returns the data as a Pandas dataframe.

The data are currently all stored as strings, 
except for 'ICNARC number' and 'Unit ID' which are integers.

Note: This dataframe is wide. Each column is a variable in the CMP dataset. 
	So there are potentially 205 columns. But only those variables present in the xml 
	file are included. 
      Each row in the dataframe is a patient. 
	Entries are empty when variables are not recorded for a given patient.  
	The dataframe is likley to have many missing entries.
"""
import xml.etree.ElementTree as ET  
import pandas as pd
import numpy as np

def parse_icnarc_xml(xml_filename, cmp_filename, verbose=True):

	data = pd.DataFrame() ## dataframe to return

	## first read the xml into a dictionary
	_xml = dict()
	try:
		tree = ET.parse(xml_filename)
		root = tree.getroot()
		if verbose:
			print("Parsed xml file, now extracting data from tree:")
		for ci,child in enumerate(root):
			if ci%100==0 and verbose:
				print(ci)

			_xml[ci] = dict()
			for grandchild in child:
				_xml[ci][grandchild.tag.split('}')[-1]] = grandchild.text

	except:
		print("Warning: could not read xml file, returning empty dataframe.")
		return data

	## find which CMP codes are present in this data 
	##	(not all used codes appear for all patients, appears that 4 are never used):
	codes_in_use = set()
	for patient in _xml.keys():
		codes_in_use.update(_xml[patient].keys())

	## now convert this into a dataframe, including data detials form the CMP properties file:
	try:
		cmp = pd.read_excel(cmp_filename, sheet_name='CMP_Dataset')
	except:	
		print("Warning: could not read CMP properties file, returning empty dataframe.")
		return data
	
	for index, row in cmp.iterrows():
		code = row['CODE']
		if code in codes_in_use:
			description = row['Description']
			data[description] = [_xml[patient][code] if code in _xml[patient].keys() else np.nan for patient in _xml.keys()]

	return convert_unit_numbers(data)

def convert_unit_numbers(data):
	''' Maps the ICNARC CMP Number column to an integer that matches the 'Unit ID' from WW:

	H91 -> 1 (GICU)
	B16 -> 14 (CICU)
	'''
	data['Unit ID'] = [1 if i=='H91' else 14 for i in data['ICNARC CMP Number']]
	data['ICNARC number'] = [int(i) for i in data['ICNARC Number']]

	return data.drop(columns=['ICNARC Number'])

if __name__=="__main__":

	data = parse_icnarc_xml("../ICNARC_Dataset_2015-2018__clean_.xml", "../ICNARC CMP Dataset Properties.xlsx")
	print(len(data))
	print(data.head())
	data.to_csv('icnarc_dataframe.csv')
	print("Saved as icnarc_dataframe.csv")
