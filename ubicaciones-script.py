import csv
import os

def getDepartment(id):
    departments = {
        3: 'CANELONES',
        7: 'FLORES',
        10: 'MALDONADO',
        11: 'PAYSANDU'
    }
    return departments.get(int(id), 'UNKNOWN')

with open('ubi_loc_dep.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=';')
    line_count = 0
    sql_sentence = ''
    directory = "inserts_ubicaciones"
    file_path = os.path.join(directory, f"inserts_ubicaciones.sql")
    result = ''
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        else:
            result += f"INSERT INTO UBICACION VALUES({row[5]}, {row[0]}, {row[1]}, '{getDepartment(row[0])}', '{row[2]}');\n"
            print(result)
            line_count += 1
    
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(file_path, 'w') as file:
        file.write(result)
        result = ''
    print(f'Processed {line_count} lines.')

 