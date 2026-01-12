#!/usr/bin/env python3
"""
Test DuckDB queries against leaderboard results.
This script helps you develop and validate leaderboard queries locally.
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Error: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)


# Example queries for CAR-bench leaderboard
EXAMPLE_QUERIES = {
    "overall_performance": """
        SELECT
            id,
            ROUND(pass_rate, 1) AS "Pass Rate (%)",
            ROUND(time_used, 1) AS "Time (s)",
            total_tasks AS "Total Tasks"
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY pass_rate DESC, time_used ASC) AS rn
            FROM (
                SELECT
                    results.participants.agent AS id,
                    res.pass_rate AS pass_rate,
                    res.time_used AS time_used,
                    SUM(res.max_score) OVER (PARTITION BY results.participants.agent) AS total_tasks
                FROM results
                CROSS JOIN UNNEST(results.results) AS r(res)
            )
        )
        WHERE rn = 1
        ORDER BY "Pass Rate (%)" DESC, "Time (s)" ASC;
    """,
    
    "performance_by_split": """
        SELECT
            results.participants.agent AS id,
            'base' AS split,
            ROUND(AVG(res.pass_at_k_scores_by_split.base."Pass@1" * 100), 1) AS "Avg Pass Rate (%)",
            ROUND(AVG(res.time_used), 1) AS "Avg Time (s)"
        FROM results
        CROSS JOIN UNNEST(results.results) AS r(res)
        GROUP BY results.participants.agent
        
        UNION ALL
        
        SELECT
            results.participants.agent AS id,
            'hallucination' AS split,
            ROUND(AVG(res.pass_at_k_scores_by_split.hallucination."Pass@1" * 100), 1) AS "Avg Pass Rate (%)",
            ROUND(AVG(res.time_used), 1) AS "Avg Time (s)"
        FROM results
        CROSS JOIN UNNEST(results.results) AS r(res)
        GROUP BY results.participants.agent
        
        UNION ALL
        
        SELECT
            results.participants.agent AS id,
            'disambiguation' AS split,
            ROUND(AVG(res.pass_at_k_scores_by_split.disambiguation."Pass@1" * 100), 1) AS "Avg Pass Rate (%)",
            ROUND(AVG(res.time_used), 1) AS "Avg Time (s)"
        FROM results
        CROSS JOIN UNNEST(results.results) AS r(res)
        GROUP BY results.participants.agent
        
        ORDER BY id, split;
    """,
    
    "task_success_rates": """
        WITH base_tasks AS (
            SELECT
                'base' AS split,
                dr.task_id,
                dr.reward
            FROM results
            CROSS JOIN UNNEST(results.results) AS r(res)
            CROSS JOIN UNNEST(res.detailed_results_by_split.base) AS d(dr)
        ),
        hallucination_tasks AS (
            SELECT
                'hallucination' AS split,
                dr.task_id,
                dr.reward
            FROM results
            CROSS JOIN UNNEST(results.results) AS r(res)
            CROSS JOIN UNNEST(res.detailed_results_by_split.hallucination) AS d(dr)
        ),
        disambiguation_tasks AS (
            SELECT
                'disambiguation' AS split,
                dr.task_id,
                dr.reward
            FROM results
            CROSS JOIN UNNEST(results.results) AS r(res)
            CROSS JOIN UNNEST(res.detailed_results_by_split.disambiguation) AS d(dr)
        ),
        all_tasks AS (
            SELECT * FROM base_tasks
            UNION ALL SELECT * FROM hallucination_tasks
            UNION ALL SELECT * FROM disambiguation_tasks
        )
        SELECT
            split,
            task_id,
            ROUND(AVG(reward), 2) AS "Success Rate",
            COUNT(*) AS "Attempts"
        FROM all_tasks
        GROUP BY split, task_id
        ORDER BY split, task_id
        LIMIT 20;
    """,
    
    "pass_at_k": """
        SELECT
            id,
            "Pass@1",
            "Pass@2"
        FROM (
            SELECT
                results.participants.agent AS id,
                ROUND(AVG(res.pass_at_k_scores."Pass@1" * 100), 1) AS "Pass@1",
                ROUND(AVG(res.pass_at_k_scores."Pass@2" * 100), 1) AS "Pass@2"
            FROM results
            CROSS JOIN UNNEST(results.results) AS r(res)
            GROUP BY results.participants.agent
        )
        ORDER BY "Pass@1" DESC;
    """
}


def load_results(results_dir: str = "results") -> duckdb.DuckDBPyConnection:
    """Load results JSON files into DuckDB."""
    conn = duckdb.connect(":memory:")
    
    # Check if results directory exists
    if not os.path.exists(results_dir):
        print(f"Error: Results directory '{results_dir}' not found")
        sys.exit(1)
    
    # Find all JSON files
    json_files = list(Path(results_dir).glob("*.json"))
    if not json_files:
        print(f"Warning: No JSON files found in '{results_dir}'")
        print("Creating temporary table with empty structure...")
        # Create empty table with expected structure
        conn.execute("""
            CREATE TABLE results (
                participants STRUCT(agent VARCHAR),
                results STRUCT(
                    pass_rate DOUBLE,
                    time_used DOUBLE,
                    max_score INTEGER,
                    task_rewards_by_split MAP(VARCHAR, MAP(VARCHAR, DOUBLE[])),
                    pass_at_k_scores STRUCT("Pass@1" DOUBLE, "Pass@2" DOUBLE)
                )[]
            )
        """)
        return conn
    
    # Load JSON files
    pattern = f"{results_dir}/*.json"
    try:
        conn.execute(f"""
            CREATE TABLE results AS
            SELECT * FROM read_json_auto('{pattern}')
        """)
        print(f"Loaded {len(json_files)} result file(s) from {results_dir}")
    except Exception as e:
        print(f"Error loading results: {e}")
        sys.exit(1)
    
    return conn


def run_query(conn: duckdb.DuckDBPyConnection, query: str, query_name: str = "Query"):
    """Run a DuckDB query and display results."""
    try:
        print(f"\n{'='*80}")
        print(f"{query_name}")
        print(f"{'='*80}\n")
        
        result = conn.execute(query).fetchdf()
        
        if result.empty:
            print("No results found.")
        else:
            print(result.to_string(index=False))
            print(f"\n({len(result)} row{'s' if len(result) != 1 else ''})")
        
        return result
    
    except Exception as e:
        print(f"Error running query: {e}")
        return None


def test_with_sample_data(conn: duckdb.DuckDBPyConnection, sample_file: str):
    """Test queries with a sample results.json file."""
    try:
        with open(sample_file) as f:
            sample_data = json.load(f)
        
        # Insert sample data
        conn.execute("DROP TABLE IF EXISTS results")
        conn.execute(f"CREATE TABLE results AS SELECT * FROM read_json_auto('{sample_file}')")
        
        print(f"Loaded sample data from {sample_file}")
        
        # Show structure
        print("\nResults structure:")
        result = conn.execute("DESCRIBE results").fetchdf()
        print(result.to_string(index=False))
        
        # Show sample
        print("\nSample data (first row):")
        result = conn.execute("SELECT * FROM results LIMIT 1").fetchdf()
        print(f"Columns: {', '.join(result.columns)}")
        print(f"Shape: {result.shape}")
        
    except Exception as e:
        print(f"Error loading sample data: {e}")
        sys.exit(1)


def interactive_mode(conn: duckdb.DuckDBPyConnection):
    """Interactive query mode."""
    print("\n" + "="*80)
    print("Interactive Query Mode")
    print("="*80)
    print("Enter DuckDB SQL queries. Type 'exit' or 'quit' to exit.")
    print("Type 'help' to see example queries.")
    print("="*80 + "\n")
    
    while True:
        try:
            query = input("duckdb> ").strip()
            
            if not query:
                continue
            
            if query.lower() in ('exit', 'quit', 'q'):
                break
            
            if query.lower() == 'help':
                print("\nAvailable example queries:")
                for name in EXAMPLE_QUERIES.keys():
                    print(f"  - {name}")
                print("\nUse: run <query_name> to execute an example")
                continue
            
            if query.lower().startswith('run '):
                query_name = query[4:].strip()
                if query_name in EXAMPLE_QUERIES:
                    run_query(conn, EXAMPLE_QUERIES[query_name], query_name)
                else:
                    print(f"Unknown query: {query_name}")
                continue
            
            # Run custom query
            result = conn.execute(query).fetchdf()
            if result.empty:
                print("No results.")
            else:
                print(result.to_string(index=False))
                print(f"\n({len(result)} rows)")
        
        except KeyboardInterrupt:
            print("\n")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Test DuckDB queries against leaderboard results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test with results directory
  python test_queries.py --results results/
  
  # Test with sample results.json
  python test_queries.py --sample ../output/results.json
  
  # Run specific example query
  python test_queries.py --results results/ --query overall_performance
  
  # Run custom query from file
  python test_queries.py --results results/ --file my_query.sql
  
  # Interactive mode
  python test_queries.py --results results/ --interactive
        """
    )
    
    parser.add_argument("--results", default="results", help="Path to results directory")
    parser.add_argument("--sample", help="Path to sample results.json file")
    parser.add_argument("--query", choices=EXAMPLE_QUERIES.keys(), help="Run example query")
    parser.add_argument("--file", help="Run query from SQL file")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive query mode")
    parser.add_argument("--list-queries", action="store_true", help="List available example queries")
    
    args = parser.parse_args()
    
    # List queries
    if args.list_queries:
        print("Available example queries:")
        for name, query in EXAMPLE_QUERIES.items():
            print(f"\n{name}:")
            print(query.strip())
        return
    
    # Load data
    if args.sample:
        conn = load_results(args.results)
        test_with_sample_data(conn, args.sample)
    else:
        conn = load_results(args.results)
    
    # Run specific query
    if args.query:
        run_query(conn, EXAMPLE_QUERIES[args.query], args.query)
    
    # Run query from file
    if args.file:
        with open(args.file) as f:
            query = f.read()
        run_query(conn, query, f"Query from {args.file}")
    
    # Interactive mode
    if args.interactive:
        interactive_mode(conn)
    
    # Run all example queries if no specific action
    if not args.query and not args.file and not args.interactive:
        print("\nRunning all example queries...\n")
        for name, query in EXAMPLE_QUERIES.items():
            run_query(conn, query, name)


if __name__ == "__main__":
    main()
