''' Loading files to the database alternatively file by file '''

import os
import psycopg2

def create_and_populate_ecatables(folder):
    variable_code = folder[-2:]
    files = os.listdir(folder)
    conn = psycopg2.connect('dbname=weatherdata user=leontioso password=postgres host=localhost')
    cur = conn.cursor()
    cur.execute(f''' create table eca_blend_{variable_code} (staid integer, souid integer, date date, {variable_code} integer, q_{variable_code} integer check( q_{variable_code} in (0, 1, 9)));''')
    for file in files:
        if file not in {'elements.txt', 'stations.txt', 'sources.txt'}:
            cur.execute(f'''copy eca_blend_{variable_code} (staid, souid, date, {variable_code}, q_{variable_code})
                            from program 'sudo runuser -l leontioso  -c "tail -q -n +22 /media/leontioso/data2/WeatherData/WeatherData/ECA_blend_{variable_code}/{file}'
                            delimiter ',' ''')
    conn.commit()
    print(f'eca_blend_{variable_code} table created')



