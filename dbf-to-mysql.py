
"""
Convert a DBF file to an SQLite table.

"""

try:
    import pymysql
except ImportError:
    pass

from dbfread import DBF, FieldParser
import datetime
import re

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = 'rootDbPassword'
DB_NAME = 'obligatorio'

# Este implementacion es necesaria para las fechas que estan en un formato incorrecto


class AsteriskFieldParser(FieldParser):
    def parseD(self, field, data):
        """Parse date field and return datetime.date or None"""
        try:
            return datetime.date(int(data[:4]), int(data[4:6]), int(data[6:8]))
        except ValueError:
            print(data)
            withoutAstersik = re.sub(rb'\*+$', b'', data)
            if withoutAstersik.strip(b' 0') == b'':
                # A record containing only spaces and/or zeros is
                # a NULL value.
                return None
            else:
                raise ValueError('invalid date {!r}'.format(data))


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
        fieldType = field.type
        if fieldType == "C":
            tmp = "%s VARCHAR(%s)" % (field.name, field.length + 1)
            columns.append(tmp)
        elif fieldType == "N":
            tmp = "%s NUMERIC(%s, %s)" % (
                field.name, field.length, field.decimal_count)
            columns.append(tmp)
        elif fieldType == "D":
            tmp = "%s DATE" % (field.name)
            columns.append(tmp)
        else:
            raise NotImplementedError('Type %s not implemented' % fieldType)

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
    for field in fields:
        columns.append(apost + field.name + apost)
        params.append("%s")
    q = "INSERT INTO %s (%s) VALUES(%s);" % (
        names, ",\n".join(columns), ",\n".join(params))
    return q


# Conexion con el dbf.
db = DBF('Personas.dbf', encoding='latin-1', parserclass=AsteriskFieldParser)
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

for record in db:
    dpto = int(record['DPTO'])
    if dpto in [3,7,10,11]:
        args = tuple(record.values())
        print(record)
        cursor.execute(sql, args)


conn.commit()
