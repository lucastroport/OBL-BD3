
"""
Convert a DBF file to an SQLite table.

"""

try:
    import pymysql
except ImportError:
    pass

from dbfread import DBF

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = 'rootDbPassword'
DB_NAME = 'obligatorio'


def run_query(query=''):
    data = [DB_HOST, DB_USER, DB_PASS, DB_NAME]

    conn = pymysql.connect(host=DB_HOST, user=DB_USER,
                           passwd=DB_PASS, database=DB_NAME)
    cursor = conn.cursor()
    cursor.execute(query)

    if query.upper().startswith('SELECT'):
        data = cursor.fetchall()
    else:
        conn.commit()
        data = None

    cursor.close()
    conn.close()

    return data


def sql_create_table(db):
    columns = []
    fields = db.fields
    columns.append("id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY")

    for field in fields:
        tipo = field.type
        if tipo == "C":
            tmp = "%s VARCHAR(%s)" % (field.name, field.length + 1)
            columns.append(tmp)
        elif tipo == "N":
            tmp = "%s NUMERIC(%s, %s)" % (
                field.name, field.length, field.decimal_count)
            columns.append(tmp)
        elif tipo == "D":
            tmp = "%s DATE" % (field.name)
            columns.append(tmp)
        else:
            raise NotImplementedError('Type %s not implemented' % tipo)

    q = "CREATE TABLE IF NOT EXISTS %(names)s\n(\n%(columns)s\n);"
    q = q % dict(names=db.name.rstrip(".dbf"), columns=",\n".join(columns))

    return q


def sql_insert_into(db):
    """ OBS: El id se autoincrementa solo. """

    apost = "`"
    fields = db.fields
    names = apost + db.name.rstrip(".dbf") + apost
    columns = []
    params = []
    for campo in fields:
        columns.append(apost + campo.name + apost)
        params.append("%s")
    q = "INSERT INTO %s (%s) VALUES(%s);" % (
        names, ",\n".join(columns), ",\n".join(params))
    return q


# Conexion con el dbf.
db = DBF('Hogares.dbf', encoding='latin-1')
# Estructura de la tabla
sql = sql_create_table(db)

# Establecer una conexion a mysql y ejecutar el script
# print(sql)
run_query(sql)

# Conexion con mysql.
# data = [DB_HOST, DB_USER, DB_PASS, DB_NAME]
conn = pymysql.connect(host=DB_HOST, user=DB_USER,
                       passwd=DB_PASS, database=DB_NAME)
cursor = conn.cursor()  # Creamos un cursor para insertar los data.

sql = sql_insert_into(db)  # Estructura del INSERT INTO.

# data.
for record in db:
    args = tuple(record.values())
    print(record.items())
    cursor.execute(sql, args)


conn.commit()
