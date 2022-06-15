# This script was at first intended to fix the file dump.nt available
# @ https://www.cs.toronto.edu/~oktie/linkedmdb/ . 
# Apache Jena (v 3.13.1) cannot parse it indeed. 
# The main problem is that the file contains URIs with illegal characters,
# in particular ", {, }, and `, among the whole illegal set [<>"{}|^`\] .
# This script actually fixes all the ill-ed URIs in any N-Triples file,
# according to the grammar defined @ https://www.w3.org/TR/n-triples/#n-triples-grammar 
# (#x00-#x20) excluded .
# The fixing is obtained by substituting the illegal characters with their hexadecimal
# equivalent value. eg: ' " ' is substituted with ' %22 '
#
# Please execute by using "python3 URI_fixer.py"
#
# NOTE: Execution time could be unfeasible for fixing all the
# illegal character set, especially on large files.
# Please set the variable FIXING_REGEX accordingly to save execution time.
# Please comment if-cases in the nestedmost loop according to what you chose.

# @author Nicola Maino

import re

# Set with the characters set to fix
# Currently set for linkedmdb_1m 
FIXING_REGEX = r'["{}`]' # Use r'[<>"{}|^`\\]' for full conversion instead

# Script dirs
file_path = "C:\\Users\\Nicola Maino\\Documents\\RDF_DATASETS\\linkedmdb-latest-dump\\"
file_input_name = "linkedmdb-latest-dump.nt"
file_output_name = "linkedmdb-latest-dump-purified.nt"

# Opens the input file
fin = open(file_path + file_input_name, "rt")

# Opens oputput file (to convert)
fout = open(file_path + file_output_name, "wt")

# For each line in the input file
for line in fin:

	# Splits each lie in subject, predicate and object
	# space-separated
	subj_pred_obj = line.split(" ")
	
	new_line = "" # New fixed file line to be output

	for element in subj_pred_obj:

		# It is an URI (eg: not a literal)
		if element.startswith('<'): 

			# There actually are illegal charachers, otherwise skip
			if re.search(FIXING_REGEX, element): 

				# This is to replace multiple occurences of the same
				# illegal character into the same URI
				while re.search(FIXING_REGEX, element):

					# Replacement ...
					if re.search(r'"', element):
						element = re.sub(r'"', r'%22', element)
					# elif re.search(r'<', element):
					# 	element = re.sub(r'<', r'%3C', element)
					# elif re.search(r'>', element):
					# 	element = re.sub(r'>', r'%3E', element)					
					elif re.search(r'{', element):
						element = re.sub(r'{', r'%7B', element)
					elif re.search(r'}', element):
						element = re.sub(r'}', r'%7D', element)
					# elif re.search(r'\|', element):
					# 	element = re.sub(r'\|', r'%7C', element)
					# elif re.search(r'^', element):
					#	element = re.sub(r'^', r'%5E', element)
					elif re.search(r'`', element):
						element = re.sub(r'`', r'%60', element)
					# elif re.search(r'\\', element):
					# 	element = re.sub(r'\\', r'%5C', element)
					# else: pass

		# Concatenates back subject, predicate and object
		new_line = new_line + element	

	# Writes out the new, grammarly correct line
	fout.write(new_line)

# close input and output files
fin.close()
fout.close()