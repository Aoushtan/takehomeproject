import configparser
import glob
import json
import pymongo
    
class Connection:
    def __init__(self, host="localhost", port=27017):
        self.client = pymongo.MongoClient(host, port)
        self.db = None
        self.collection = None
    
    def set_db(self, db):
        self.db = self.client[db]
        
    def set_collection(self, collection):
        self.collection = self.get_db().get_collection(collection)
        
    def get_db(self):
        return self.db
    
    def get_coll(self):
        return self.collection

class QueryConfiguration:
    def __init__(self):
        self.columns = []
        self.queries = {}
        self.group_by = []
        self.numeric_columns = []
    
    def set_columns(self,cols):
        self.columns = cols
    
    def set_queries(self, q):
        self.queries = q
        
    def set_group_by(self, g):
        self.group_by = g
            
    def set_numeric_columns(self, cols):
        self.numeric_columns = cols
        
    def get_columns(self):
        return self.columns
    
    def get_queries(self):
        return self.queries
    
    def get_group_by(self):
        return self.group_by
    
    def get_numeric_columns(self):
        return self.numeric_columns

#Utility functions
def read_json(json_file):
    print(f"Loading in {json_file}")
    with open(json_file) as file:
        data = json.load(file)
    return data

def read_ini(ini_file):
    config = configparser.ConfigParser()
    config.read(ini_file)
    print(f"Reading {ini_file}")
    data = {}
    for section in config.sections():
        data[section] = {}
        for option in config.options(section):
            data[section][option] = config.get(section, option)
    return data

def insert_into_collection(collection, data):
    if data:
        if isinstance(data, list):
            collection.insert_many(data)
        else:
            collection.insert_one(data)
    else:
        print("No data to insert, collection not created.")

def nested_dict_pairs_iterator(dict_obj):
    ''' This function accepts a nested dictionary as argument
        and iterate over all values of nested dictionaries
    '''
    # Iterate over all key-value pairs of dict argument
    for key, value in dict_obj.items():
        # Check if value is of dict type
        if isinstance(value, dict):
            # If value is dict then iterate over all its values
            for pair in  nested_dict_pairs_iterator(value):
                yield (key, *pair)
        else:
            # If value is not dict type then yield the value
            yield (key, value)

def print_string_options(strs):
    result = ""
    for s in strs:
        result += str(strs.index(s)+1) + " " + str(s) + " | "
    print(result)
    
def get_items_by_index(values, indexes):
    list_str = indexes.split(",")
    list_int = set(map(int,list_str))
    result = list()
    for item in list_int:
        result.append(values[item-1])
    return result

def parse_config(data):
    config = QueryConfiguration()
    columns = []
    mongo_queries = {}
    group_by = []
    for pair in nested_dict_pairs_iterator(data):
        section = pair[0]
        key = pair[1]
        value = pair[2]
        if section == "IncludeColumns":
            if int(value) == 1:
                columns.append(str(key))
        elif section == "ApplyFilters":
            if value != "":
                if str(key.split("_")[0]) in columns or str(key.split("_")[0]) + "_" + str(key.split("_")[1]) in columns:
                    num = value.isnumeric()
                    if "_equals" in key:
                        if num:
                            value = int(value)
                        mongo_queries[str(key.split("_equals")[0])] = { "$eq" :  value }
                    elif "_greater_than" in key:
                        if num:
                            value = int(value)
                        mongo_queries[str(key.split("_greater_than")[0])] = { "$gt" : value }
                    elif "_greater_equal" in key:
                        if num:
                            value = int(value)
                        mongo_queries[str(key.split("_greater_equal")[0])] = { "$gte" : value }
                    elif "_less_than" in key: 
                        if num:
                            value = int(value)
                        mongo_queries[str(key.split("_less_than")[0])] = { "$lt" : value }
                    elif "_less_equal" in key:
                        if num:
                            value = int(value)
                        mongo_queries[str(key.split("_less_equal")[0])] = { "$lte" : value }
                    elif "_in" in key:
                        if num:
                            value = [int(x) for x in value]
                            mongo_queries[str(key.split("_in")[0])] = { "$in" : value }
                        else:
                            in_list = value.split(",")
                            mongo_queries[str(key.split("_in")[0])] = { "$in" : in_list } 
                    elif "_not" in key:
                        if num:
                            value = int(value)
                        mongo_queries[str(key.split("_not")[0])] = { "$ne" : value }
                    elif "_notin" in key:
                        if num:
                            value = [int(x) for x in value]
                            mongo_queries[str(key.split("_notin")[0])] = { "$nin" : value }
                        else:
                            not_in_list = value.split(",")
                            mongo_queries[str(key.split("_notin")[0])] = { "$nin" : not_in_list }                 
        elif section == "GroupBy":
            if str(key.split("group_")[1]) in columns:
                if int(value) == 1:
                    group_by.append(key.split("group_")[1])
    config.set_columns(columns)
    config.set_queries(mongo_queries)
    config.set_group_by(group_by)
    return config

def clean_input(prompt, type_ = None, min_ = None, max_ = None, range_ = None):
    if min_ is not None and max_ is not None and max_ < min_:
        raise ValueError("min_ must be less than or equal to max_") 
    while True:
        choice = input(prompt)
        if type_ is not None:
            try:
                choice = type_(choice)
            except ValueError:
                print(f"Input type must be {type_.__name__}.")
                continue
        if max_ is not None and choice > max_:
            print(f"Input must be less than or equal to {max_}.")
        elif min_ is not None and choice < min_:
            print(f"Input must be greater than or equal to {min_}.")
        elif range_ is not None and choice not in range_:
            if isinstance(range_, range):
                print(f"Input must be between {range_.start} and {range_.stop}.")
            else:
                txt = "Input must be {0}."
                if len(range_) == 1:
                    print(txt.format(*range_))
                else:
                    expected = " or ".join((", ".join(str(x) for x in range_[:-1]),str(range_[-1])))
                    print(txt.format(expected))
        else:
            return choice

def select_collection(connection):
    try:
        print("Please select which Collection to use by typing the corresponding number and hitting enter.")
        collections = connection.get_db().list_collection_names()
        print_string_options(collections)
        col_index = clean_input("Enter the number for the collection to select: ",int,1,len(collections))-1
        connection.set_collection(collections[col_index])
        print("Selected " + connection.get_coll().name + " collection.")
    except ValueError:
        print("No collections exist in database, please run setup option first.")
        menu(connection)
    
def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

#Operation functions
def main():
    print("|___________Take home interview project___________|")
    print("|Developed by Austin Lemacks for BornTec interview|")
    example_mongo = Connection(host, port)
    example_mongo.set_db(db_name)
    print(f"Using {db_name} database on MongoDB instance at {host}:{port}.")
    menu(example_mongo)

def menu(connection):
    print("|___Option Menu___|")
    print("Please select an option from below by entering the corresponding number.")
    print("| 1 | Setup - Loads JSON files in the directory into the local MongoDB instance as collections for example data.")
    print("| 2 | Run Queries - Select a collection to query using an .ini file for filtering.")
    print("| 3 | View Data - View the first document of a selected collection.")
    print("| 4 | Exit - Closes application.")
    print("_________________")
    choice = clean_input("Enter corresponding number for option above: ", int, 1, 4)
    if choice == 1:
        setup(connection)
    elif choice == 2:
        pipeline(connection)
    elif choice == 3:
        view_document(connection)
    elif choice == 4:
        print("Quitting.")
        quit()
      
def setup(connection):
    print("Starting JSON to MongoDB load for test data.")
    existing_collections = connection.get_db().list_collection_names()
    json_files = [f for f in glob.glob("*.json")]
    for file in json_files:
        if str(file)[:-5] not in existing_collections:
            file_data = read_json(file)
            collection = connection.get_db()[str(file)[:-5]]
            insert_into_collection(collection, file_data)
        else:
            print(f"Skipping {file}, collection already exists in the database.")
    print("Collection setup complete.")
    menu(connection)
         
def pipeline(connection):
    config_data = query_input(connection)
    initial_data, config = retrieve_stage(connection, config_data)
    final_data = aggregate_stage(connection, config, initial_data)
    store_stage(connection, final_data)
    
def query_input(connection):
    select_collection(connection)
    print("Now, select which configuration to use by selecting an .ini file from the list below for " + connection.get_coll().name + ".")
    ini_files = [f for f in glob.glob(connection.get_coll().name + "*.ini")]
    print_string_options(ini_files)
    if ini_files:
        config_index = clean_input("Enter the number for the ini configuration you wish to use for " + connection.get_coll().name +":" ,int,1,len(ini_files))
        config_data = read_ini(ini_files[config_index-1])
        return config_data
    else:
        print("No .ini configuration exists for this collection, please select a new one.")
        pipeline(connection)

def retrieve_stage(connection, config_data):
    config = parse_config(config_data)
    collection = connection.get_coll()
    print(f"Querying {collection.name}")
    filtered_docs = []
    numeric_cols = []
    cursor = collection.find()
    print("Retrieval complete.")
    print("Mapping data.")
    inc_cols = config.get_columns()
    for doc in cursor:
        doc_dict = {}
        for key in inc_cols:
            doc_dict[key] = (doc.get(key))
            if is_number(doc.get(key)):
                numeric_cols.append(key)
        filtered_docs.append(doc_dict)
    config.set_numeric_columns(numeric_cols)
    print("Mapping complete.")
    return filtered_docs, config

def aggregate_stage(connection, config, initial_data):
    print("Aggregating data.")
    temp_collection = connection.get_db()["temp_pipline"]
    connection.set_collection(temp_collection.name)
    insert_into_collection(temp_collection, initial_data)
    aggregation = []
    matches = config.get_queries()
    match_fields = {}
    match = {"$match" : match_fields}
    for col in config.get_columns():
        if matches.get(col) is not None:
            match_fields[col] = matches.get(col)
    aggregation.append(match)
    concat = []
    all_fields = []
    g = config.get_group_by()
    group = {}
    if g:
        group["$group"] = {"_id" : "$" + g[0]}
    else:
        group["$group"] = {"_id" : 0}
    for col in config.get_columns():
        if col not in g:
            if col in config.get_numeric_columns():
                group["$group"]["mean_" + str(col)] = {"$avg" : "$" + col}
                group["$group"]["sum_" + str(col)] = {"$sum" : "$" + col}
                group["$group"]["med_calc_" + str(col)] = {"$push" : "$" + col}
                all_fields.append("mean_" + str(col))
                all_fields.append("sum_" + str(col))
            else:
                group["$group"]["unique_strings_" + str(col)] = {"$addToSet" : "$" + col}
                concat.append("$unique_strings_" + str(col))
    aggregation.append(group)
    project = {}
    project["$project"] = {}
    for col in all_fields:
        project["$project"][str(col)] = "$" + col
    for col in config.get_columns():
        if col not in g and col in config.get_numeric_columns():
            project["$project"]["median_" + str(col)] = {"$arrayElemAt" : ["$med_calc_" + str(col), {"$toInt" : {"$divide" : [{"$size" : "$med_calc_" + str(col)}, 2]}}]}
    project["$project"]["all_unique_strings"] = {"$concatArrays" : concat}
    aggregation.append(project)
    #print(aggregation)
    print("Aggregation complete.")
    try:
        final_data = temp_collection.aggregate(aggregation)
    except:
        print("Aggregation error occured, likely an issue with the .ini file configuration.")
        menu(connection)
    temp_collection.drop()
    return final_data

def store_stage(connection, data):
    name = clean_input("Please enter a name for the new collection: ",str)
    connection.set_collection(name)
    for doc in data:
        insert_into_collection(connection.get_coll(), doc)
    print(f"Data inserted into {name}.  To view a sample of the data select that option from the menu.")
    menu(connection)
    
def view_document(connection):
    select_collection(connection)
    collection = connection.get_coll()
    cursor = collection.aggregate([{"$sample" : {"size" : 5}}])
    print("Viewing a sample of up to 5 documents.")
    for doc in cursor:
        print(doc)
    menu(connection)
                        
if __name__ == "__main__":
    host = "localhost"
    port = 27017
    db_name = "takehome"
    main()