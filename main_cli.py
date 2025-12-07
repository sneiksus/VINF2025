import argparse
import sys
import os

try:
    from wiki_spark import run_spark_merge_logic
    spark_merge_available = True
except ImportError:
    print("Error: Could not import run_spark_merge_logic from wiki_spark.py. Merge operation will fail.")
    spark_merge_available = False
except Exception as e:
    print(f"Warning: An unexpected error occurred during Spark module import: {e}")
    spark_merge_available = False

try:
    from wiki_lucene import build_index, search_index
    lucene_available = True
except ImportError:
    print("Error: Could not import build_index or search_index from wiki_lucene.py. Lucene operations will fail.")
    lucene_available = False
except Exception as e:
    print(f"Warning: An unexpected error occurred during Lucene module import or initialization: {e}")
    lucene_available = False


def merge_command_handler(args):
    print("\n--- Starting Merge Operation ---")
    if not spark_merge_available:
        print("Error: Spark merge logic is not available. Cannot perform merge.")
        sys.exit(1)
    try:
        success = run_spark_merge_logic(
            nndb_path=args.nndb_path,
            wiki_dump_path=args.wiki_dump_path,
            output_path="output.tsv"
        )
        if success:
            print(f"Merge operation completed successfully")
            if not os.path.exists("output.tsv"):
                print(f"Warning: output.tsv was not created as expected after merge operation.")
        else:
            print("Merge operation failed during execution.")
            sys.exit(1)
    except Exception as e:
        print(f"Merge operation failed with an unexpected error: {e}")
        sys.exit(1)

def index_command_handler(args):
    print("\n--- Starting Index Build Operation ---")
    if not lucene_available:
        print("Error: Lucene is not available. Cannot build index.")
        sys.exit(1)
    try:
        if not os.path.exists("output.tsv"):
            print(f"Error: Data file not found at. Please run the 'merge' command first or specify the correct path.")
            sys.exit(1)
            
        success = build_index(data_path="output.tsv", index_dir="people_index")
        if success:
            print("Index build completed successfully.")
        else:
            print("Index build failed.")
            sys.exit(1)
    except Exception as e:
        print(f"Index build operation failed: {e}")
        sys.exit(1)

def search_command_handler(args):
    query = args.query
    print(f"\n--- Starting Search Operation for: '{query}' ---")
    if not lucene_available:
        print("Error: Lucene is not available. Cannot perform search.")
        sys.exit(1)
    try:
        search_index(query_str=query, index_dir="people_index")
        print("Search operation completed.")
    except Exception as e:
        print(f"Search operation failed: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="CLI")
    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')

    # --- Merge Command ---
    merge_parser = subparsers.add_parser('merge', help='Merge NNDB and Wiki dumps to create output.tsv')
    merge_parser.add_argument('--nndb-path', type=str, required=True, help='Path to the NNDB TSV file.')
    merge_parser.add_argument('--wiki-dump-path', type=str, required=True, help='Path to the Wikipedia XML dump file.')
    merge_parser.set_defaults(func=merge_command_handler)

    # --- Index Command ---
    index_parser = subparsers.add_parser('index', help='Build Lucene index from output.tsv')
    index_parser.set_defaults(func=index_command_handler)

    # --- Search Command ---
    search_parser = subparsers.add_parser('search', help='Search the Lucene index')
    search_parser.add_argument('query', type=str, help='The search query')
    search_parser.set_defaults(func=search_command_handler)

    args = parser.parse_args()
    
    if args.command == 'merge':
        if not args.nndb_path or not args.wiki_dump_path:
            print("Error: --nndb-path and --wiki-dump-path are required for the 'merge' command.")
            merge_parser.print_help()
            sys.exit(1)

    args.func(args) 

if __name__ == "__main__":
    main()
