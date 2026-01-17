#!/usr/bin/env python3
"""
Convert all parquet files to CSV
"""

import os
import sys
from pathlib import Path
import pandas as pd


def convert_parquet_to_csv(input_dir: str = "stage", output_dir: str = None):
    """
    Convert all parquet files in input_dir to CSV files
    
    Args:
        input_dir: Directory containing parquet files (default: stage)
        output_dir: Directory for CSV files (default: same as input with _csv suffix)
    """
    input_path = Path(input_dir)
    
    if output_dir is None:
        output_path = Path(f"{input_dir}_csv")
    else:
        output_path = Path(output_dir)
    
    # Find all parquet files
    parquet_files = list(input_path.rglob("*.parquet"))
    
    if not parquet_files:
        print(f"❌ No parquet files found in {input_path}")
        return
    
    print(f"🔄 Converting {len(parquet_files)} parquet files to CSV...")
    print(f"   Input:  {input_path}")
    print(f"   Output: {output_path}")
    print("=" * 50)
    
    converted = 0
    for parquet_file in parquet_files:
        try:
            # Read parquet
            df = pd.read_parquet(parquet_file)
            
            # Create output path preserving subdirectory structure
            relative_path = parquet_file.relative_to(input_path)
            csv_file = output_path / relative_path.with_suffix(".csv")
            
            # Create output directory if needed
            csv_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as CSV
            df.to_csv(csv_file, index=False)
            
            print(f"✅ {relative_path} -> {csv_file.name} ({len(df)} rows)")
            converted += 1
            
        except Exception as e:
            print(f"❌ Error converting {parquet_file}: {e}")
    
    print("=" * 50)
    print(f"✅ Converted {converted}/{len(parquet_files)} files")
    print(f"📁 CSV files saved to: {output_path}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert parquet files to CSV")
    parser.add_argument("--input", "-i", default="stage", help="Input directory (default: stage)")
    parser.add_argument("--output", "-o", default=None, help="Output directory (default: input_csv)")
    
    args = parser.parse_args()
    
    convert_parquet_to_csv(args.input, args.output)


if __name__ == "__main__":
    main()