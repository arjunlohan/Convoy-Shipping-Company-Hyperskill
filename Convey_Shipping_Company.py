# Write your code here
# Write your code here
import pandas as pd
import re
import sqlite3
import json
from lxml import etree
import xml.etree.cElementTree as ET


def create_database(database_name):
    conn = sqlite3.connect("{}.s3db".format(database_name))
    return conn


def execute_query(conn, query, values=False):
    cursor_name = conn.cursor()
    if values:
        cursor_name.execute(query, values)
    else:
        cursor_name.execute(query)
    conn.commit()


def convert_to_num(x):
    x = re.sub("[^0-9]", "", x)
    try:
        x = int(x)
    except ValueError:
        x = float(x)
    return x


def count_dataframe_diff(x, y):
    count = 0
    for name in x.columns:
        try:
            y[name] = y[name].astype(str)
            z = x.compare(y)
            count += z[name].count()[1]
        except KeyError:
            pass
    return count


def convert_xlsx_to_csv(x):
    df = pd.read_excel(x, sheet_name="Vehicles", dtype=str)
    df.to_csv(str(x[:-5]) + ".csv", index=None, header=True)
    print("{} {} added to {}.csv".format(int(df.shape[0]),
                                         "line was" if int(df.shape[0]) <= 1 else "lines were",
                                         x[:-5]))
    return df, str(x[:-5]) + ".csv"


def convert_csv_to_checked_csv(x):
    df = pd.read_csv(x, dtype=str)
    df_2 = df.applymap(convert_to_num)
    df_2.to_csv(str(x[:-4]) + "[CHECKED].csv", index=None, header=True)
    values_corrected = count_dataframe_diff(df, df_2)
    print("{} {} corrected in {}[CHECKED].csv".format(int(values_corrected),
                                                      "cells were" if int(values_corrected) > 1 else "cell was",
                                                      x[:-4]))
    return df_2, str(x[:-4]) + "[CHECKED].csv"


def read_from_db(x):
    convoy_dict_json = {"convoy":[]}
    convoy_dict_xml = {"convoy":[]}
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    conn = sqlite3.connect(x)
    conn.row_factory = dict_factory
    c = conn.cursor()
    c.execute("""SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy WHERE score > 3""")
    data = c.fetchall()
    for row in data:
        convoy_dict_json["convoy"].append(row)
    c.execute("""SELECT vehicle_id, engine_capacity, fuel_consumption, maximum_load FROM convoy WHERE score <= 3""")
    data = c.fetchall()
    for row in data:
        convoy_dict_xml["convoy"].append(row)
    return convoy_dict_json, convoy_dict_xml


def score_calculator(engine_capacity, fuel_consumption, maximum_load):
    score = 0
    if (int(engine_capacity)/int(fuel_consumption))*100 >= 450:
        score += 2
    elif ((int(engine_capacity)*2)/int(fuel_consumption))*100 >= 450:
        score += 1
    else:
        pass
    if (450/100) * int(fuel_consumption) <= 230:
        score += 2
    else:
        score += 1
    if int(maximum_load) >= 20:
        score += 2
    return score

def csv_to_db(filename, df_2, intial_query, adding_query):
    conn = create_database(filename)
    conn.execute(intial_query)
    conn.commit()
    for index, row in df_2.iterrows():
        values = [row["vehicle_id"], row["engine_capacity"], row["fuel_consumption"], row["maximum_load"]]
        score = score_calculator(row["engine_capacity"], row["fuel_consumption"], row["maximum_load"])
        values.append(score)
        conn.execute(adding_query, values)
        conn.commit()
    print("{} {} inserted into {}.s3db".format(int(df_2.shape[0]),
                                               "record was" if int(df_2.shape[0]) <= 1 else "records were",
                                               filename))
    conn.close()
    return str(filename)+".s3db"


def db_to_xml(filename, db_data):
    vehicle_counter = 0
    #xml_string = ""
    for main_key in db_data.keys():
        #xml_string += "<" + str(main_key) + ">"
        root = etree.Element(main_key)
        #print(xml_string)
        for element in db_data.get(str(main_key)):
            #xml_string += "<vehicle>"
            root_2 = etree.SubElement(root, "vehicle")
            for key, value in element.items():
                #xml_string += "<" + str(key) + ">" + str(value) + "</" + str(key) + ">"
                etree.SubElement(root_2, key).text = str(value)
            vehicle_counter += 1
            #xml_string += "</vehicle>"
        #xml_string += "</" + str(main_key) + ">"
    #root = etree.fromstring(xml_string)
    #print(xml_string)
    tree = ET.ElementTree(root)
    tree.write("{}.xml".format(filename), short_empty_elements=False)
    print("{} {} saved into {}.xml".format(vehicle_counter, "vehicles were",
                                           filename))


def db_to_json(filename, dict_data):
    with open('{}.json'.format(filename[:-5]), 'w') as f:
        json.dump(dict_data, f)
    print("{} {} saved into {}.json".format(len(dict_data.get("convoy")), "vehicle was" if len(dict_data.get("convoy")) == 1 else "vehicles were", filename[:-5]))


def file_reader(filename):
    intial_query = """
        CREATE TABLE "convoy" (
        "vehicle_id" INTEGER PRIMARY KEY,
        "engine_capacity" INTEGER NOT NULL,
        "fuel_consumption" INTEGER NOT NULL,
        "maximum_load" INTEGER NOT NULL,
        "score" INTEGER NOT NULL
        )
        """
    adding_query = """
                        INSERT INTO "convoy" VALUES (?, ?, ?, ?, ?)
                        """

    if filename[-5:] == ".xlsx":
        df, csv_filename = convert_xlsx_to_csv(filename)
        df_2, checked_csv_filename = convert_csv_to_checked_csv(csv_filename)
        filename = filename[:-5]
        db_filename = csv_to_db(filename, df_2, intial_query, adding_query)
        data_json, data_xml = read_from_db(db_filename)
        db_to_json(db_filename, data_json)
        db_to_xml(filename, data_xml)
    if filename[-4:] == ".csv" and filename[-13:] != "[CHECKED].csv":
        df_2, checked_csv_filename = convert_csv_to_checked_csv(filename)
        filename = filename[:-4]
        db_filename = csv_to_db(filename, df_2, intial_query, adding_query)
        data_json, data_xml = read_from_db(db_filename)
        db_to_json(db_filename, data_json)
        db_to_xml(filename, data_xml)
    if filename[-13:] == "[CHECKED].csv":
        df_2 = pd.read_csv(filename, dtype=str)
        filename = filename[:-13]
        db_filename = csv_to_db(filename, df_2, intial_query, adding_query)
        data_json, data_xml = read_from_db(db_filename)
        db_to_json(db_filename, data_json)
        db_to_xml(filename, data_xml)
    if filename[-5:] == ".s3db":
        data_json, data_xml = read_from_db(filename)
        db_to_json(filename, data_json)
        filename=filename[:-5]
        db_to_xml(filename, data_xml)


user_filename = input("Input file name\n")
file_reader(user_filename)