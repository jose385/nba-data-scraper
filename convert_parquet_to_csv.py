#!/usr/bin/env python3
"""
Convert NBA Parquet Files to CSV
Converts all .parquet files in a directory to .csv format
"""

import argparse
import os
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

def convert_parquet_to_csv(input_dir, output_dir=None, file_pattern=None):
    """
    Convert all parquet files in directory to CSV
    
    Args:
        input_dir: Directory containing .parquet files
        output_dir: Directory to save .csv files (default: same as input_dir)  
        file_pattern: Optional pattern to filter files (e.g., 'nba_game_info')
    """
    
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"❌ Input directory does not exist: {input_dir}")
        return False
    
    # Set output directory
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = input_path
    
    # Find all parquet files
    if file_pattern:
        parquet_files = list(input_path.glob(f'*{file_pattern}*.parquet'))
    else:
        parquet_files = list(input_path.glob('*.parquet'))
    
    if not parquet_files:
        print(f"⚠️ No parquet files found in {input_dir}")
        if file_pattern:
            print(f"   (searching for pattern: *{file_pattern}*.parquet)")
        return False
    
    print(f"🔄 Converting {len(parquet_files)} parquet files to CSV...")
    print(f"📁 Input:  {input_path}")
    print(f"📁 Output: {output_path}")
    print()
    
    converted = 0
    failed = 0
    
    for parquet_file in sorted(parquet_files):
        try:
            # Create CSV filename
            csv_filename = parquet_file.stem + '.csv'
            csv_path = output_path / csv_filename
            
            # Read parquet file
            print(f"🔄 Converting {parquet_file.name}...", end=" ")
            df = pd.read_parquet(parquet_file)
            
            # Write CSV file
            df.to_csv(csv_path, index=False)
            
            # Get file sizes
            parquet_size = parquet_file.stat().st_size / 1024  # KB
            csv_size = csv_path.stat().st_size / 1024  # KB
            
            print(f"✅ {len(df)} rows ({parquet_size:.1f}KB → {csv_size:.1f}KB)")
            converted += 1
            
        except Exception as e:
            print(f"❌ Failed: {e}")
            failed += 1
    
    print(f"\n📊 Conversion Summary:")
    print(f"   ✅ Converted: {converted} files")
    print(f"   ❌ Failed: {failed} files")
    print(f"   📁 CSV files saved to: {output_path}")
    
    return failed == 0

def preview_parquet_files(input_dir, max_files=5):
    """Preview what parquet files are available"""
    
    input_path = Path(input_dir)
    
    if not input_path.exists():
        print(f"❌ Directory does not exist: {input_dir}")
        return
    
    parquet_files = list(input_path.glob('*.parquet'))
    
    if not parquet_files:
        print(f"⚠️ No parquet files found in {input_dir}")
        return
    
    print(f"📋 Found {len(parquet_files)} parquet files in {input_dir}:")
    print()
    
    for i, parquet_file in enumerate(sorted(parquet_files)[:max_files]):
        try:
            df = pd.read_parquet(parquet_file)
            size_kb = parquet_file.stat().st_size / 1024
            
            print(f"📄 {parquet_file.name}")
            print(f"   📊 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
            print(f"   💾 Size: {size_kb:.1f} KB")
            print(f"   🏷️ Columns: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            print()
            
        except Exception as e:
            print(f"❌ {parquet_file.name}: Error reading file ({e})")
    
    if len(parquet_files) > max_files:
        print(f"... and {len(parquet_files) - max_files} more files")

def convert_specific_nba_files(input_dir, output_dir=None):
    """Convert specific NBA data types to CSV"""
    
    nba_file_types = {
        'game_info': 'Game schedules and basic info',
        'plays': 'Play-by-play data with advanced metrics',
        'shot_chart': 'Shot location and defensive context',
        'box_scores': 'Player statistics and props data'
    }
    
    print("🏀 Converting NBA data files to CSV...")
    print("=" * 50)
    
    total_converted = 0
    
    for file_type, description in nba_file_types.items():
        print(f"\n📊 {description}")
        print(f"🔍 Looking for files matching: *{file_type}*.parquet")
        
        if convert_parquet_to_csv(input_dir, output_dir, file_pattern=file_type):
            print(f"✅ {file_type} files converted successfully")
            total_converted += 1
        else:
            print(f"⚠️ No {file_type} files found or conversion failed")
    
    print(f"\n🎉 NBA conversion complete!")
    print(f"📊 Successfully converted {total_converted}/{len(nba_file_types)} NBA data types")

def main():
    parser = argparse.ArgumentParser(description='Convert NBA parquet files to CSV format')
    parser.add_argument('--input-dir', '-i', default='stage', help='Input directory with .parquet files (default: stage)')
    parser.add_argument('--output-dir', '-o', help='Output directory for .csv files (default: same as input)')
    parser.add_argument('--pattern', '-p', help='Filter files by pattern (e.g., "game_info", "espn")')
    parser.add_argument('--preview', action='store_true', help='Preview parquet files without converting')
    parser.add_argument('--nba-files', action='store_true', help='Convert NBA-specific file types (game_info, plays, etc.)')
    
    args = parser.parse_args()
    
    print("🏀 NBA Parquet to CSV Converter")
    print("=" * 40)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    if args.preview:
        preview_parquet_files(args.input_dir)
        return
    
    if args.nba_files:
        convert_specific_nba_files(args.input_dir, args.output_dir)
        return
    
    # Convert all files or filtered files
    success = convert_parquet_to_csv(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        file_pattern=args.pattern
    )
    
    if success:
        print("\n🎉 All files converted successfully!")
        print("\n💡 Usage tips:")
        print("   📊 Open CSV files in Excel, Google Sheets, or any text editor")
        print("   🔍 Use Excel filters to analyze your NBA data")
        print("   📈 Import into BI tools for advanced analytics")
    else:
        print("\n⚠️ Some files failed to convert - check errors above")

if __name__ == "__main__":
    main()