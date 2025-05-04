import csv
import zipfile
import xml.etree.ElementTree as ET

def read_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')  # Adjust delimiter if needed
        return [tuple(row) for row in reader]

def read_xlsx(file_path):
    with zipfile.ZipFile(file_path) as z:
        shared_strings = []
        sheet_data = []

        # Read shared strings
        with z.open('xl/sharedStrings.xml') as f:
            tree = ET.parse(f)
            root = tree.getroot()
            for si in root.findall('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si'):
                text = ''.join(t.text or '' for t in si.iter('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t'))
                shared_strings.append(text)

        # Read sheet1 data
        with z.open('xl/worksheets/sheet1.xml') as f:
            tree = ET.parse(f)
            root = tree.getroot()
            ns = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

            for row in root.findall('.//main:row', ns):
                current_row = []
                for cell in row.findall('main:c', ns):
                    cell_type = cell.attrib.get('t')
                    value_elem = cell.find('main:v', ns)
                    if value_elem is not None:
                        value = value_elem.text
                        if cell_type == 's':
                            value = shared_strings[int(value)]
                        current_row.append(value)
                    else:
                        current_row.append('')
                sheet_data.append(tuple(current_row))

        return sheet_data

def compare_as_sets(xlsx_rows, csv_rows):
    xlsx_set = set(xlsx_rows)
    csv_set = set(csv_rows)

    only_in_xlsx = xlsx_set - csv_set
    only_in_csv = csv_set - xlsx_set

    return only_in_xlsx, only_in_csv

# ---- USAGE ----
csv_data = read_csv('postgres_Bandit_BarbaraJames.csv')
xlsx_data = read_xlsx('query_output_Bandit_Barbara James.xlsx')

only_in_xlsx, only_in_csv = compare_as_sets(xlsx_data, csv_data)

print(f"Rows only in XLSX: {len(only_in_xlsx)}")
print(f"Rows only in CSV : {len(only_in_csv)}")
