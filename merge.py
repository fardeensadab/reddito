#!/usr/bin/env python3
"""
Merge Reddit JSON files into a single CSV dataset
Scans category folders and combines data with proper null/empty handling
"""

import json
import csv
import os
from pathlib import Path
from typing import List, Dict, Optional
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedditDataMerger:
    def __init__(self, data_dir: str = "data", output_file: str = "reddit_dataset.csv"):
        self.data_dir = Path(data_dir)
        self.output_file = Path(output_file)
        self.records = []
        self.auto_id = 1
        
        logger.info(f"Data directory: {self.data_dir.absolute()}")
        logger.info(f"Output file: {self.output_file.absolute()}")
    
    def _comments_to_json_string(self, comments: Optional[List]) -> str:
        """
        Convert comments list to JSON string representation
        Handles None and empty arrays
        """
        if comments is None:
            return "[]"
        if not comments or len(comments) == 0:
            return "[]"
        try:
            return json.dumps(comments, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Error converting comments to JSON: {e}")
            return "[]"
    
    def _handle_null_value(self, value: any, default: str = "") -> str:
        """
        Convert various null/empty/None values to string representation
        """
        if value is None:
            return default
        if isinstance(value, str):
            if value.strip() == "":
                return default
            return value.strip()
        if isinstance(value, (list, dict)):
            if len(value) == 0:
                return default
            return str(value)
        return str(value)
    
    def _process_json_file(self, filepath: Path, category: str) -> Optional[Dict]:
        """
        Process a single JSON file and extract data
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            record = {
                'auto_id': self.auto_id,
                'category': category,
                'title': self._handle_null_value(data.get('title')),
                'description': self._handle_null_value(data.get('description')),
                'votes': self._handle_null_value(data.get('votes')),
                'comments': self._comments_to_json_string(data.get('comments')),
                'url': self._handle_null_value(data.get('url'))
            }
            
            self.auto_id += 1
            return record
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {filepath}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing {filepath}: {e}")
            return None
    
    def _scan_category_folder(self, category_path: Path, category_name: str) -> int:
        """
        Scan a category folder for JSON files
        Returns the number of files processed
        """
        files_processed = 0
        
        if not category_path.is_dir():
            logger.warning(f"Category path is not a directory: {category_path}")
            return files_processed
        
        # Find all JSON files in the category folder
        json_files = sorted(category_path.glob('*.json'))
        
        # Filter out tracker files
        json_files = [f for f in json_files if f.name not in ['id_tracker.json', 'hash_tracker.json']]
        
        logger.info(f"Found {len(json_files)} JSON files in category '{category_name}'")
        
        for json_file in json_files:
            record = self._process_json_file(json_file, category_name)
            if record:
                self.records.append(record)
                files_processed += 1
                logger.debug(f"  Processed: {json_file.name}")
            else:
                logger.warning(f"  Failed to process: {json_file.name}")
        
        return files_processed
    
    def merge_data(self) -> int:
        """
        Scan all category folders and merge data
        Returns total number of records processed
        """
        if not self.data_dir.exists():
            logger.error(f"Data directory not found: {self.data_dir}")
            return 0
        
        logger.info(f"Scanning data directory: {self.data_dir}")
        
        total_processed = 0
        
        # Find all community folders (e.g., r_bangladesh)
        community_folders = [d for d in self.data_dir.iterdir() if d.is_dir()]
        
        for community_folder in sorted(community_folders):
            logger.info(f"\nScanning community: {community_folder.name}")
            
            # Check if this community has category subfolders
            category_folders = [d for d in community_folder.iterdir() if d.is_dir()]
            
            if category_folders:
                # Has category subfolders
                for category_folder in sorted(category_folders):
                    category_name = category_folder.name
                    processed = self._scan_category_folder(category_folder, category_name)
                    total_processed += processed
                    logger.info(f"  Category '{category_name}': {processed} records")
            else:
                # No category subfolders, treat community folder as containing JSON files
                category_name = community_folder.name
                processed = self._scan_category_folder(community_folder, category_name)
                total_processed += processed
                logger.info(f"  {category_name}: {processed} records")
        
        logger.info(f"\nTotal records collected: {total_processed}")
        return total_processed
    
    def write_csv(self) -> bool:
        """
        Write collected records to CSV file
        """
        if not self.records:
            logger.warning("No records to write!")
            return False
        
        try:
            fieldnames = ['auto_id', 'category', 'title', 'description', 'votes', 'comments', 'url']
            
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.records)
            
            logger.info(f"✓ CSV file written successfully: {self.output_file.absolute()}")
            logger.info(f"  Total records: {len(self.records)}")
            logger.info(f"  File size: {self.output_file.stat().st_size / 1024:.2f} KB")
            
            return True
            
        except Exception as e:
            logger.error(f"Error writing CSV file: {e}")
            return False
    
    def run(self) -> bool:
        """
        Main method to run the merge process
        """
        logger.info("="*80)
        logger.info("Reddit Data Merger")
        logger.info("="*80)
        
        # Merge data from all categories
        total = self.merge_data()
        
        if total == 0:
            logger.warning("No data to merge!")
            return False
        
        # Write to CSV
        success = self.write_csv()
        
        logger.info("="*80)
        if success:
            logger.info("✓ Merge process completed successfully!")
        else:
            logger.error("✗ Merge process failed!")
        logger.info("="*80)
        
        return success


def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Merge Reddit JSON files into a CSV dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python merge.py
  python merge.py --data-dir data --output reddit_dataset.csv
        """
    )
    
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Directory containing Reddit JSON files (default: data)'
    )
    
    parser.add_argument(
        '--output',
        default='reddit_dataset.csv',
        help='Output CSV filename (default: reddit_dataset.csv)'
    )
    
    args = parser.parse_args()
    
    # Create merger and run
    merger = RedditDataMerger(data_dir=args.data_dir, output_file=args.output)
    success = merger.run()
    
    return 0 if success else 1


if __name__ == '__main__':
    exit(main())
