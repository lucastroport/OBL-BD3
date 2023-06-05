import os

def depure(directory):
    fives = 5555
    eights = 88
    
    hogares_table = "obligatorio.hogares"
    viviendas_table = "obligatorio.viviendas"
    personas_table = "obligatorio.personas"
    
    table_columns = {
        hogares_table: ['NBI_EDUCAC', 'NBI_CANTID', 'NBI_HAC', 'NBI_CONFOR', 'NBI_VIV', 'HogCa01', 'HogSH01', 'HogSC01', 'HogHD00', 'HogHD01', 'HogCa01', 'HogCE06', 'HogCE09', 'HogCE10', 'HogCE12', 'HogCE13'],
        viviendas_table: ['VivVO01', 'VivVO03', 'CATEVIV', 'VivDV05', 'VivDV06', 'VivDV01', 'VivDV03', 'VivDV02'],
        personas_table: ['PERNA01','pobpcoac', 'PerDi01', 'PerDi05', 'PerDi02', 'PerPa01', 'PerMi02_1', 'PerMi05_1', 'PerEd03_r', 'PerEd03_1', 'PerEd03_2', 'PerEd05_r', 'PerEd06_r', 'PerFM01', 'PerFM01_1', 'PerFM02', 'PerFM02_1', 'PerFM04_2', 'niveledu_r', 'v66', 'PerFM01_r', 'PerFM02_r', 'PerER01_4']
    }
    
    result = ''
    for table in table_columns:
        for column in table_columns[table]:
            result += f"UPDATE {table} SET {column} = NULL WHERE {column} = {fives} OR {column} = {eights};\n"
        # Set the file path and name
        file_path = os.path.join(directory, f"{table}_depurated.sql")
        # Open the file in write mode
        with open(file_path, 'w') as file:
            file.write(result)
            result = ''


directory = "depurated_tables"
if not os.path.exists(directory):
    os.makedirs(directory)
    
depure(directory)