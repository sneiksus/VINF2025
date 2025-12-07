import os
import csv
import lucene

# Initialize the JVM (Critical: only do this once in the application life cycle)
try:
    lucene.initVM()
except ValueError:
    pass 

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, TextField, Field
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause

# --- Configuration ---
DEFAULT_INDEX_DIR = "people_index"
DEFAULT_DATA_FILE = "output.tsv"

def build_index(data_path: str = DEFAULT_DATA_FILE, index_dir: str = DEFAULT_INDEX_DIR):
    print(f"Building Index from {data_path} into {index_dir}...")
    
    if not os.path.exists(data_path):
        print(f"Error: Data file not found at '{data_path}'.")
        return False

    try:
        path = Paths.get(index_dir)
        os.makedirs(index_dir, exist_ok=True)
        directory = FSDirectory.open(path)
        analyzer = StandardAnalyzer()
        
        config = IndexWriterConfig(analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        
        writer = IndexWriter(directory, config)

        with open(data_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            columns = reader.fieldnames 
            count = 0
            
            for row in reader:
                doc = Document()
                
                for col_name in columns:
                    value = row.get(col_name, '')
                    if value:
                        # Normalize field names for Lucene (lowercase, no spaces)
                        field_name = col_name.lower().replace(" ", "_")
                        doc.add(TextField(field_name, value, Field.Store.YES))
                
                writer.addDocument(doc)
                count += 1
                
            print(f"Successfully indexed {count} documents.")
            
    except Exception as e:
        print(f"An error occurred during index building: {e}")
        return False
    finally:
        if 'writer' in locals() and writer:
            writer.close()
        if 'directory' in locals() and directory:
            directory.close()
            
    return True


def search_index(query_str: str, index_dir: str = DEFAULT_INDEX_DIR):
    print(f"\n--- Searching index '{index_dir}' for: '{query_str}' ---")

    try:
        path = Paths.get(index_dir)
        if not os.path.exists(index_dir) or not DirectoryReader.indexExists(FSDirectory.open(path)):
             print("Error: Index does not exist. Run 'build_index' first.")
             return
    except Exception as e:
         print(f"Error accessing index directory: {e}")
         return

    directory = None
    reader = None
    try:
        directory = FSDirectory.open(path)
        reader = DirectoryReader.open(directory)
        searcher = IndexSearcher(reader)
        analyzer = StandardAnalyzer()

        # Dynamically discover fields from the first document
        fields = []
        if reader.maxDoc() > 0:
            first_doc = searcher.doc(0)
            fields = [f.name() for f in first_doc.getFields() if f.name() != "_version_"] 
        else:
            print("Index is empty.")
            return
        
        # Construct the query
        builder = BooleanQuery.Builder()
        for field in fields:
            try:
                parser = QueryParser(field, analyzer)
                field_query = parser.parse(query_str)
                builder.add(field_query, BooleanClause.Occur.SHOULD)
            except Exception:
                # Ignore fields that fail to parse (e.g., formatting issues)
                pass
        
        query = builder.build()
        
        # Execute Search
        hits = searcher.search(query, 10) # Top 10 results

        print(f"Found {hits.totalHits.value} matches.")
        print("-" * 60)

        if hits.totalHits.value == 0:
            print("No results found.")
        else:
            for hit in hits.scoreDocs:
                doc = searcher.doc(hit.doc)
                name = doc.get("name") or "N/A"
                description = doc.get("description") or "N/A"

                print(f"Name:        {name}") 
                print(f"Description: {description}")
                print(f"Score:       {hit.score:.4f}")
                print("-" * 60)

    except Exception as e:
        print(f"An error occurred during search execution: {e}")
    finally:
        if reader:
            reader.close()
        if directory:
            directory.close()