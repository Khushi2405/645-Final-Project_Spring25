import csv
import os

def clean_value(val):
    val = val.strip().replace('\t', '').replace('\n', '').replace('\r', '')
    return '' if val == '\\N' else val

def pad_or_truncate(val, size):
    return val.encode('utf-8')[:size].decode('utf-8', errors='ignore')

# Part 1: Clean title_basics.tsv
def process_titles(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:

        reader = csv.DictReader(infile, delimiter='\t')
        writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')

        writer.writerow(['movieId', 'title'])
        for row in reader:
            tconst = clean_value(row['tconst'])
            primary_title = clean_value(row['primaryTitle'])

            if len(tconst) > 9:
                continue

            movie_id = pad_or_truncate(tconst, 9)
            title = pad_or_truncate(primary_title, 30)

            writer.writerow([movie_id, title])

# Part 2: Clean title_principals.tsv
def process_principals(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:

        reader = csv.DictReader(infile, delimiter='\t')
        writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writerow(['movieId', 'personId','category'])
        for row in reader:
            tconst = clean_value(row['tconst'])
            nconst = clean_value(row['nconst'])
            category = clean_value(row['category'])

            if len(tconst) > 9 or len(nconst) > 10:
                continue

            tconst_fixed = pad_or_truncate(tconst, 9)
            nconst_fixed = pad_or_truncate(nconst, 10)
            category_fixed = pad_or_truncate(category, 20)

            writer.writerow([tconst_fixed, nconst_fixed, category_fixed])

# Part 3: Clean name_basics.tsv
def process_names(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:

        reader = csv.DictReader(infile, delimiter='\t')
        writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writerow(['personId', 'name'])
        for row in reader:
            nconst = clean_value(row['nconst'])
            primary_name = clean_value(row['primaryName'])

            if len(nconst) > 10:
                continue

            nconst_fixed = pad_or_truncate(nconst, 10)
            name_fixed = pad_or_truncate(primary_name, 105)

            writer.writerow([nconst_fixed, name_fixed])


# Run all processing steps
process_titles('title.basics.tsv', 'cleaned_movies.csv')
process_principals('title.principals.tsv', 'cleaned_workedon.csv')
process_names('name.basics.tsv', 'cleaned_people.csv')
