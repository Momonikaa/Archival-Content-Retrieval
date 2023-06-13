from flask import Flask, request
from whoosh import index
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import pandas as pd
from whoosh.fields import Schema, TEXT, ID, NUMERIC, DATETIME
from whoosh.index import create_in, open_dir
from whoosh.qparser import QueryParser
from whoosh import scoring
from tqdm import tqdm
import os
import csv
import os
from whoosh.query import And,  Or
import shutil
from pathlib import Path
from whoosh.index import create_in
from whoosh.fields import *
import pandas as pd
from whoosh.writing import AsyncWriter

app = Flask(__name__)
csv_dir = os.path.dirname(__file__)
index_dir = os.path.join(csv_dir, "indexes")
power_index_dir = os.path.join(csv_dir, "power_index")
schema=Schema()




@app.route('/add_columns/<filename>', methods=['POST'])
def add_columns(filename,encoding="utf8"):
    # Get the path of the current script (app.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Combine the script directory and filename
    file_path = os.path.join(script_dir, filename)
    
    # Read the file into a DataFrame
    df = pd.read_csv(file_path)
    
    # Remove spaces from column names
    df.columns = df.columns.str.replace(" ", "_")
    
    # Add "filename" column if it doesn't exist
    if 'filename' not in df.columns:
        df['filename'] = filename
    
    if 'ID' not in df.columns:
        df['ID'] = range(1, len(df) + 1)

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Get the header row
        
        # Remove spaces from column names
        headers = [header.replace(" ", "_") for header in headers]
    
    
    
    # Save the modified DataFrame back to the file
    df.to_csv(file_path, index=False)
    
    return f'Columns added successfully to file: {filename}'


def get_index(filename,encoding="utf8"):
    index_path = os.path.join(index_dir, filename)
    
    if not os.path.exists(index_path):
        os.makedirs(index_path)
        
    if not index.exists_in(index_path):
        index.create_in(index_path, schema)
        
    ix = index.open_dir(index_path)
    
    return ix

def update_schema(field_names):
    global schema
    fields = {}
    
    print(field_names)
    # Add the field names to the schema
    for field_name in field_names:
        sanitized_field_name = field_name.replace(" ", "_")
        fields[sanitized_field_name] = TEXT(stored=True)
        
        
    fields['filename'] = TEXT(stored=True)
    
    # Update the schema
    schema = Schema(**fields)

    print(schema)
    
    return schema

@app.route('/index/<filename>', methods=['POST'])
def index_metadata(filename,encoding="utf8"):
    file_path=os.path.join(csv_dir,filename)

    if not os.path.exists(file_path):
        return f'File {filename} not found!'
    
    
    

    with open(file_path,'r',encoding="utf8") as file:
        reader = csv.DictReader(file)
        field_names = reader.fieldnames
        
        if 'filename' not in field_names:
            field_names.append('filename')
        
    
    

        if not schema:
            update_schema(field_names)

        ix = get_index(filename)
        writer = AsyncWriter(ix)

        for row in reader:
            metadata = {}
            for field in field_names:
                sanitized_field = field.replace(" ", "_")
                metadata[sanitized_field] = row[field]
                
            metadata['filename'] = filename
            writer.add_document(**metadata)

    writer.commit()

    power_index_path = os.path.join(power_index_dir, "power_index")
    power_index = get_index(power_index_path)
    power_writer = AsyncWriter(power_index)

    with ix.searcher() as searcher:
        for field in field_names:
            query = QueryParser(field, schema=schema).parse("*")
            results = searcher.search(query)
            for result in results:
                result_fields = result.fields()
                result_fields['filename'] = filename
                power_writer.add_document(**result_fields)

    power_writer.commit()


    return f'Metadata indexed successfully for file {filename}!'
        

@app.route('/update/<fieldname>', methods=['PUT'])
def update_metadata(filename):
    file_path = os.path.join(os.path.dirname(__file__), filename)

    if not os.path.exists(file_path):
        return f'File {filename} not found!'
    

    update_data = request.get_json()

    if not isinstance(update_data, dict):
        return 'Invalid update data format. Please provide a JSON object.'
    
    ix=get_index(filename)
    writer=ix.writer()

    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        field_names = reader.fieldnames

        for row in reader:
            if row['ID']==str(update_data.get('ID')):
                metadata={}
                for field in field_names:
                    if field in update_data:
                        metadata= update_data[field]

                    else:
                        metadata[field]=row[field]

                    metadata['filename']=filename

                    writer.update_document(**metadata)

    writer.commit()

    power_index_path = os.path.join(power_index_dir, "power_index")
    power_index = get_index(power_index_path)
    power_writer = power_index.writer()


    with power_index.searcher() as searcher:
        query = QueryParser("ID", scehma=schema).parse(str(update_data.get('ID')))
        filename_query = QueryParser("filename", schema=schema).parse(filename)
        final_query = And([query, filename_query])
        results = searcher.searcg(final_query)

        for result in results:
            power_writer.delete_documnet(result.docnum)

    power_writer.commit()


    power_writer = power_index.writer()
    with ix.searcher() as searcher:
        for field in field_names:
            query = QueryParser("filename", schema=schema).parse(str(update_data.get('ID')))
            filename_query = QueryParser("filename", schema=schema).parse(filename)
            final_query=And([query, filename_query])
            results = searcher.search(final_query)
            for result in results:
                power_writer.add_document(**result.fields())

    power_writer.commit()

    return f'Metadata updated successfully for {filename}!'

@app.route('/delete/<filename>', methods=['DELETE'])
def delete_metadata(filename):
    id_to_delete = request.data.decode("utf-8").strip()

    ix = get_index(filename)
    writer = ix.writer()

    with writer.mergetype(index.MERGE_CLEAR) as writer:
        query = QueryParser('ID', schema=schema).parse(id_to_delete)
        writer.delete_by_query(query)

    writer.commit()


    return f'Metadata with ID {id_to_delete} deleted successfully from file {filename}!'



@app.route('/delete/<filename>', methods=['DELETE'])
def delete_index(filename):
    # Delete the index directory for the specific CSV file
    index_path = os.path.join(index_dir, filename)
    if os.path.exists(index_path):
        shutil.rmtree(index_path)
    else:
        return f'Index directory for file {filename} not found!'
    
    # Delete the corresponding documents from the power index
    power_index_path = os.path.join(power_index_dir, "power_index")
    power_index = get_index(power_index_path)
    power_writer = power_index.writer()
    
    with power_index.searcher() as searcher:
        filename_query = QueryParser("filename", schema=schema).parse(filename)
        results = searcher.search(filename_query)
        for result in results:
            power_writer.delete_document(result.docnum)
    
    power_writer.commit()
    
    return f'Index for file {filename} deleted successfully!'



@app.route('/search', methods=['GET'])
def search_metadata():
    query = request.args.get('query')
    filename = request.args.get('filename')

    ix = index.open_dir(power_index_dir, indexname="power_index")
    searcher = ix.searcher()

    if filename is not None:
        # Search within a specific file
        filename_query = QueryParser("filename", schema=schema).parse(filename)
        query_parser = QueryParser("content", schema=schema)
        parsed_query = query_parser.parse(query)

        final_query = And([filename_query, parsed_query])
    else:
        # Search across all files and fields
        fields = schema.names()
        field_queries = [QueryParser(field, schema=schema).parse(query) for field in fields]
        final_query = Or(field_queries)

    results = searcher.search(final_query)

    # Process and return the search results as needed
    return str(results)  # Return the search results as a string (modify as per your requirements)


if __name__ == '__main__':
    app.run()                                          

    
    



