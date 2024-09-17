from flask import Flask, render_template
import psycopg2
import requests
import zipfile
import os
import json
import io
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host='db',
        database='fias_report',
        user='postgres',
        password='postgres'
    )
    return conn

def insert_package_date_to_db(date_str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO date_version (version) VALUES (%s)', (date_str,))
    conn.commit()
    cursor.close()
    conn.close()


def fetch_and_extract_zip(url, extract_to):
    last_update_data_info = requests.get(url)

    last_update_data_info_json = json.loads(last_update_data_info.text)

    insert_package_date_to_db(last_update_data_info_json['Date'])

    link_to_file_with_updates = last_update_data_info_json['GarXMLDeltaURL']

    response = requests.get(link_to_file_with_updates)

    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(extract_to)

        print("Files extracted successfully.")
    else:
        print(f"Failed to download file: {response.status_code}")


def insert_object_levels_to_db(path):
    directory = Path(path)
    xml_files = list(directory.glob('AS_OBJECT_LEVELS*.XML'))
    conn = get_db_connection()
    cursor = conn.cursor()
    if xml_files:
        for xml_file in xml_files:
            filename = xml_file.name
            xml_file_path = f'{path}{filename}'
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            for obj_level in root.findall('OBJECTLEVEL'):
                level = obj_level.get('LEVEL')
                name = obj_level.get('NAME')
                if (int(level) < 9):
                    query = """INSERT INTO object_levels (level, name) SELECT %s, %s 
                    WHERE NOT EXISTS (SELECT 1 FROM object_levels WHERE level = %s AND name = %s);"""
                    cursor.execute(query, (level, name, level, name))
                    conn.commit()
    else:
        print("No XML files found with the specified prefix.")
    cursor.close()
    conn.close()

def insert_changes_info(path):
    directory = Path(path)
    xml_files = [file for file in directory.glob('AS_ADDR_OBJ*.XML') 
             if not file.match('AS_ADDR_OBJ_DIVISION*.XML') 
             and not file.match('AS_ADDR_OBJ_PARAMS*.XML')]
    conn = get_db_connection()
    cursor = conn.cursor()
    if xml_files:
        for xml_file in xml_files:
            filename = xml_file.name
            xml_file_path = f'{path}{filename}'
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            for obj_level in root.findall('OBJECT'):
                level = obj_level.get('LEVEL')
                typename = obj_level.get('TYPENAME')
                name = obj_level.get('NAME')
                is_active = obj_level.get('ISACTIVE')
                if (int(is_active) == 1 and int(level) < 9):
                    cursor.execute('INSERT INTO objects (level, typename, name) VALUES (%s, %s, %s)', (level, typename, name))
        conn.commit()
    else:
        print("No XML files found with the specified prefix.")
    cursor.close()
    conn.close()

def insert_changes_from_all_folders(path_to_folder):
    items = os.listdir(path_to_folder)

    folders = [f for f in items if os.path.isdir(os.path.join(path_to_folder, f))]

    folder_paths = [os.path.join(path_to_folder, f) + '/' for f in folders]

    for path in folder_paths:
        insert_changes_info(path)


def populate_database():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('DELETE FROM object_levels; DELETE FROM objects; DELETE FROM date_version')
    conn.commit()
    cur.close()
    conn.close()

    fetch_and_extract_zip('https://fias.nalog.ru/WebServices/Public/GetLastDownloadFileInfo', '/app/data')

    insert_object_levels_to_db('/app/data/')
    insert_changes_from_all_folders('/app/data/')


@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute('SELECT * FROM object_levels')
    object_levels = cur.fetchall()

    cur.execute('SELECT version FROM date_version')
    date_row = cur.fetchone()

    tables_data = []
    for level in object_levels:
        level_value = level[0]
        cur.execute('SELECT typename, name FROM objects WHERE level = %s ORDER BY name ASC', (level_value,))
        data = cur.fetchall()

        tables_data.append({
            'level': level[1],
            'data': [{'typename': row[0], 'name': row[1]} for row in data] 
        })

    cur.close()
    conn.close()

    return render_template('index.html', date=date_row, tables_data=tables_data)


if __name__ == '__main__':
    populate_database()
    app.run(host='0.0.0.0')