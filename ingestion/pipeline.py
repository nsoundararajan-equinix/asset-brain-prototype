"""
Asset Brain Ingestion Pipeline

Main ingestion script that loads UxM-formatted test data into Spanner Graph.
Since our JSON data is already in UxM format, this focuses on efficient
batch loading and monitoring.
"""

import sys
import time
import os
import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from ingestion.spanner_client import SpannerGraphClient
from ingestion.data_loader import AssetDataLoader


class IngestionPipeline:
    """
    Main pipeline for ingesting Asset Brain test data into Spanner Graph.
    """
    
    def __init__(self, data_dir: str, project_id: str, instance_id: str, database_id: str, 
                 upsert_mode: str = 'smart', parallel_workers: int = 4, retry_attempts: int = 3,
                 enable_checkpoints: bool = True):
        self.data_loader = AssetDataLoader(data_dir)
        self.spanner_client = SpannerGraphClient(project_id, instance_id, database_id)
        self.upsert_mode = upsert_mode  # 'basic', 'smart', or 'insert_only'
        self.parallel_workers = parallel_workers  # Number of concurrent batches
        self.retry_attempts = retry_attempts  # Retry attempts for failed batches
        self.enable_checkpoints = enable_checkpoints
        self.checkpoint_file = f".checkpoint_{project_id}_{database_id}_{upsert_mode}.json"
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'entities_loaded': 0,
            'entities_updated': 0,
            'relationships_loaded': 0,
            'entity_batches_processed': 0,
            'relationship_batches_processed': 0,
            'entity_batches_failed': 0,
            'relationship_batches_failed': 0,
            'entity_batches_skipped': 0,
            'relationship_batches_skipped': 0,
            'total_retries': 0,
            'errors': 0,
            'warnings': 0
        }
        
    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load checkpoint data from disk."""
        if not self.enable_checkpoints or not os.path.exists(self.checkpoint_file):
            return {
                'completed_entity_batches': set(),
                'completed_relationship_batches': set(),
                'phase': 'entities',  # 'entities' or 'relationships' or 'complete'
                'timestamp': None
            }
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
                # Convert lists back to sets
                data['completed_entity_batches'] = set(data.get('completed_entity_batches', []))
                data['completed_relationship_batches'] = set(data.get('completed_relationship_batches', []))
                return data
        except Exception as e:
            print(f"   ⚠️ Could not load checkpoint: {e}")
            return {
                'completed_entity_batches': set(),
                'completed_relationship_batches': set(),
                'phase': 'entities',
                'timestamp': None
            }
    
    def _save_checkpoint(self, checkpoint_data: Dict[str, Any]):
        """Save checkpoint data to disk."""
        if not self.enable_checkpoints:
            return
            
        try:
            # Convert sets to lists for JSON serialization
            save_data = checkpoint_data.copy()
            save_data['completed_entity_batches'] = list(checkpoint_data['completed_entity_batches'])
            save_data['completed_relationship_batches'] = list(checkpoint_data['completed_relationship_batches'])
            save_data['timestamp'] = time.time()
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(save_data, f, indent=2)
        except Exception as e:
            print(f"   ⚠️ Could not save checkpoint: {e}")
    
    def _clear_checkpoint(self):
        """Clear checkpoint file after successful completion."""
        if self.enable_checkpoints and os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                print(f"   🗑️ Checkpoint cleared: {self.checkpoint_file}")
            except Exception as e:
                print(f"   ⚠️ Could not clear checkpoint: {e}")
        
    def _process_entity_batch_with_retry(self, batch_data: Tuple[int, List[Dict]], 
                                       checkpoint_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single entity batch with retry logic and transaction management.
        
        Args:
            batch_data: Tuple of (batch_number, entity_list)
            checkpoint_data: Checkpoint tracking data
            
        Returns:
            Processing result with metrics
        """
        batch_num, entity_batch = batch_data
        
        # Check if this batch was already completed
        if batch_num in checkpoint_data['completed_entity_batches']:
            return {
                'batch_num': batch_num,
                'inserted': 0,
                'updated': 0,
                'errors': 0,
                'warnings': 0,
                'skipped': True,
                'message': 'Already completed (checkpoint)'
            }
        
        for attempt in range(self.retry_attempts):
            client = None
            try:
                # Create a new client connection for this thread
                client = SpannerGraphClient(
                    self.spanner_client.project_id,
                    self.spanner_client.instance_id, 
                    self.spanner_client.database_id
                )
                client.connect()
                
                # Process batch with transaction
                if self.upsert_mode == 'smart':
                    result = client.batch_upsert_entities_smart(entity_batch)
                elif self.upsert_mode == 'insert_only':
                    result = client.batch_insert_entities(entity_batch)
                else:  # basic mode
                    result = client.batch_insert_entities(entity_batch)
                
                # Mark batch as completed in checkpoint
                checkpoint_data['completed_entity_batches'].add(batch_num)
                self._save_checkpoint(checkpoint_data)
                
                result['batch_num'] = batch_num
                result['attempt'] = attempt + 1
                return result
                
            except Exception as e:
                if attempt < self.retry_attempts - 1:
                    wait_time = (2 ** attempt)  # Exponential backoff
                    print(f"   ⚠️ Entity batch {batch_num} failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                    self.metrics['total_retries'] += 1
                else:
                    print(f"   ❌ Entity batch {batch_num} failed after {self.retry_attempts} attempts: {e}")
                    return {
                        'batch_num': batch_num,
                        'inserted': 0,
                        'updated': 0,
                        'errors': len(entity_batch),
                        'warnings': 0,
                        'failed': True,
                        'error_message': str(e)
                    }
            finally:
                if client:
                    client.disconnect()
        
        # Should never reach here, but for type safety
        return {
            'batch_num': batch_num,
            'inserted': 0,
            'updated': 0,
            'errors': len(entity_batch),
            'warnings': 0,
            'failed': True,
            'error_message': 'Unknown error'
        }
    
    def _process_relationship_batch_with_retry(self, batch_data: Tuple[int, List[Dict]], 
                                             checkpoint_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single relationship batch with retry logic and transaction management.
        
        Args:
            batch_data: Tuple of (batch_number, relationship_list)
            checkpoint_data: Checkpoint tracking data
            
        Returns:
            Processing result with metrics
        """
        batch_num, rel_batch = batch_data
        
        # Check if this batch was already completed
        if batch_num in checkpoint_data['completed_relationship_batches']:
            return {
                'batch_num': batch_num,
                'inserted': 0,
                'errors': 0,
                'warnings': 0,
                'skipped': True,
                'message': 'Already completed (checkpoint)'
            }
        
        for attempt in range(self.retry_attempts):
            client = None
            try:
                # Create a new client connection for this thread
                client = SpannerGraphClient(
                    self.spanner_client.project_id,
                    self.spanner_client.instance_id,
                    self.spanner_client.database_id
                )
                client.connect()
                
                result = client.batch_insert_relationships(rel_batch)
                
                # Mark batch as completed in checkpoint
                checkpoint_data['completed_relationship_batches'].add(batch_num)
                self._save_checkpoint(checkpoint_data)
                
                result['batch_num'] = batch_num
                result['attempt'] = attempt + 1
                return result
                
            except Exception as e:
                if attempt < self.retry_attempts - 1:
                    wait_time = (2 ** attempt)  # Exponential backoff
                    print(f"   ⚠️ Relationship batch {batch_num} failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                    self.metrics['total_retries'] += 1
                else:
                    print(f"   ❌ Relationship batch {batch_num} failed after {self.retry_attempts} attempts: {e}")
                    return {
                        'batch_num': batch_num,
                        'inserted': 0,
                        'errors': len(rel_batch),
                        'warnings': 0,
                        'failed': True,
                        'error_message': str(e)
                    }
            finally:
                if client:
                    client.disconnect()
        
        # Should never reach here, but for type safety
        return {
            'batch_num': batch_num,
            'inserted': 0,
            'errors': len(rel_batch),
            'warnings': 0,
            'failed': True,
            'error_message': 'Unknown error'
        }
        
    def run_ingestion(self) -> Dict[str, Any]:
        """
        Execute the complete ingestion pipeline with parallel processing and checkpoints.
        
        Phase 1: Load all entities in parallel batches
        Phase 2: Load all relationships in parallel batches
        
        Returns:
            Ingestion metrics and results
        """
        print("🚀 Starting Asset Brain Parallel Ingestion Pipeline")
        print(f"   Workers: {self.parallel_workers} | Mode: {self.upsert_mode} | Retries: {self.retry_attempts}")
        if self.enable_checkpoints:
            print(f"   Checkpoints: Enabled ({self.checkpoint_file})")
        print("=" * 70)
        
        self.metrics['start_time'] = time.time()
        
        # Load checkpoint data
        checkpoint_data = self._load_checkpoint()
        if checkpoint_data['timestamp']:
            print(f"   📋 Resuming from checkpoint (saved: {time.ctime(checkpoint_data['timestamp'])})")
            print(f"       Entity batches completed: {len(checkpoint_data['completed_entity_batches'])}")
            print(f"       Relationship batches completed: {len(checkpoint_data['completed_relationship_batches'])}")
        
        try:
            # Step 1: Validate data format
            print("\n1. Validating data format...")
            if not self.data_loader.validate_data_format():
                raise Exception("Data format validation failed")
                
            # Step 2: Connect to Spanner (main connection for schema)
            print("\n2. Connecting to Spanner Graph...")
            self.spanner_client.connect()
            
            # Step 3: Create schema (if needed)
            print("\n3. Setting up database schema...")
            self.spanner_client.create_schema()
            
            # Step 4: Get data statistics
            print("\n4. Analyzing data to be loaded...")
            stats = self.data_loader.get_data_stats()
            print(f"   Total entities: {stats['total_entities']:,}")
            print(f"   Total relationships: {stats['total_relationships']:,}")
            
            for filename, count in stats['entity_files'].items():
                print(f"   - {filename}: {count:,} entities")
                
            # Phase 1: Parallel Entity Loading
            if checkpoint_data['phase'] == 'entities':
                print(f"\n🚀 PHASE 1: Loading entities in parallel (mode: {self.upsert_mode})")
                entity_start = time.time()
                
                # Collect all entity batches
                entity_batches = list(enumerate(self.data_loader.load_entities(batch_size=500)))
                total_entity_batches = len(entity_batches)
                completed_from_checkpoint = len(checkpoint_data['completed_entity_batches'])
                
                print(f"   Processing {total_entity_batches} entity batches with {self.parallel_workers} workers...")
                if completed_from_checkpoint > 0:
                    print(f"   Skipping {completed_from_checkpoint} already completed batches from checkpoint...")
                
                # Process entity batches in parallel
                with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                    # Submit all entity batch jobs
                    future_to_batch = {
                        executor.submit(self._process_entity_batch_with_retry, batch_data, checkpoint_data): batch_data[0]
                        for batch_data in entity_batches
                    }
                    
                    # Process completed batches
                    completed_batches = 0
                    for future in as_completed(future_to_batch):
                        result = future.result()
                        completed_batches += 1
                        
                        if result.get('skipped', False):
                            self.metrics['entity_batches_skipped'] += 1
                        elif result.get('failed', False):
                            print(f"   ❌ Batch {result['batch_num']} failed: {result.get('error_message', 'Unknown error')}")
                            self.metrics['entity_batches_failed'] += 1
                        else:
                            self.metrics['entities_loaded'] += result.get('inserted', 0)
                            self.metrics['entities_updated'] += result.get('updated', 0)
                            
                        self.metrics['entity_batches_processed'] += 1
                        self.metrics['errors'] += result.get('errors', 0)
                        self.metrics['warnings'] += result.get('warnings', 0)
                        
                        # Progress updates
                        if completed_batches % max(1, total_entity_batches // 10) == 0:
                            progress = (completed_batches / total_entity_batches) * 100
                            print(f"   📊 Entity progress: {completed_batches}/{total_entity_batches} batches ({progress:.1f}%)")
                            
                entity_time = time.time() - entity_start
                print(f"   ✅ PHASE 1 Complete: {self.metrics['entities_loaded']:,} entities loaded")
                print(f"      Time: {entity_time:.2f}s | Processed: {self.metrics['entity_batches_processed']}")
                print(f"      Skipped: {self.metrics['entity_batches_skipped']} | Failed: {self.metrics['entity_batches_failed']}")
                if self.metrics['entities_updated'] > 0:
                    print(f"      Updates: {self.metrics['entities_updated']:,}")
                
                # Update checkpoint to relationships phase
                checkpoint_data['phase'] = 'relationships'
                self._save_checkpoint(checkpoint_data)
            else:
                print(f"\n⏭️ PHASE 1: Skipped (entities already completed)")
            
            # Phase 2: Parallel Relationship Loading
            if checkpoint_data['phase'] == 'relationships':
                print(f"\n🔗 PHASE 2: Loading relationships in parallel")
                rel_start = time.time()
                
                # Collect all relationship batches
                rel_batches = list(enumerate(self.data_loader.load_relationships(batch_size=2000)))
                total_rel_batches = len(rel_batches)
                completed_from_checkpoint = len(checkpoint_data['completed_relationship_batches'])
                
                print(f"   Processing {total_rel_batches} relationship batches with {self.parallel_workers} workers...")
                if completed_from_checkpoint > 0:
                    print(f"   Skipping {completed_from_checkpoint} already completed batches from checkpoint...")
                
                # Process relationship batches in parallel
                with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                    # Submit all relationship batch jobs
                    future_to_batch = {
                        executor.submit(self._process_relationship_batch_with_retry, batch_data, checkpoint_data): batch_data[0]
                        for batch_data in rel_batches
                    }
                    
                    # Process completed batches
                    completed_batches = 0
                    for future in as_completed(future_to_batch):
                        result = future.result()
                        completed_batches += 1
                        
                        if result.get('skipped', False):
                            self.metrics['relationship_batches_skipped'] += 1
                        elif result.get('failed', False):
                            print(f"   ❌ Batch {result['batch_num']} failed: {result.get('error_message', 'Unknown error')}")
                            self.metrics['relationship_batches_failed'] += 1
                        else:
                            self.metrics['relationships_loaded'] += result.get('inserted', 0)
                            
                        self.metrics['relationship_batches_processed'] += 1
                        self.metrics['errors'] += result.get('errors', 0)
                        self.metrics['warnings'] += result.get('warnings', 0)
                        
                        # Progress updates
                        if completed_batches % max(1, total_rel_batches // 10) == 0:
                            progress = (completed_batches / total_rel_batches) * 100
                            print(f"   📊 Relationship progress: {completed_batches}/{total_rel_batches} batches ({progress:.1f}%)")
                            
                rel_time = time.time() - rel_start
                print(f"   ✅ PHASE 2 Complete: {self.metrics['relationships_loaded']:,} relationships loaded")
                print(f"      Time: {rel_time:.2f}s | Processed: {self.metrics['relationship_batches_processed']}")
                print(f"      Skipped: {self.metrics['relationship_batches_skipped']} | Failed: {self.metrics['relationship_batches_failed']}")
                
                # Update checkpoint to complete
                checkpoint_data['phase'] = 'complete'
                self._save_checkpoint(checkpoint_data)
            else:
                print(f"\n⏭️ PHASE 2: Skipped (relationships already completed)")
            
            # Step 7: Verify ingestion
            print("\n7. Verifying ingestion results...")
            db_stats = self.spanner_client.get_stats()
            print(f"   Database entities: {db_stats['total_entities']:,}")
            print(f"   Database relationships: {db_stats['total_relationships']:,}")
            
            # Clear checkpoint on successful completion
            if checkpoint_data['phase'] == 'complete':
                self._clear_checkpoint()
            
        except Exception as e:
            print(f"\n❌ Ingestion failed: {e}")
            self.metrics['errors'] += 1
            raise
            
        finally:
            # Clean up main connection
            self.spanner_client.disconnect()
            self.metrics['end_time'] = time.time()
            
        return self._generate_report()
        
    def _generate_report(self) -> Dict[str, Any]:
        """Generate final ingestion report with parallel processing metrics."""
        total_time = self.metrics['end_time'] - self.metrics['start_time']
        
        print("\n" + "=" * 70)
        print("PARALLEL INGESTION PIPELINE RESULTS")
        print("=" * 70)
        print(f"🕒 Total time: {total_time:.2f}s")
        print(f"👥 Workers used: {self.parallel_workers}")
        print(f"🔄 Total retries: {self.metrics['total_retries']}")
        print()
        print("📊 ENTITIES:")
        print(f"   Loaded: {self.metrics['entities_loaded']:,}")
        if self.metrics['entities_updated'] > 0:
            print(f"   Updated: {self.metrics['entities_updated']:,}")
        print(f"   Batches processed: {self.metrics['entity_batches_processed']}")
        if self.metrics['entity_batches_skipped'] > 0:
            print(f"   Batches skipped: {self.metrics['entity_batches_skipped']}")
        if self.metrics['entity_batches_failed'] > 0:
            print(f"   Batches failed: {self.metrics['entity_batches_failed']}")
        print()
        print("🔗 RELATIONSHIPS:")
        print(f"   Loaded: {self.metrics['relationships_loaded']:,}")
        print(f"   Batches processed: {self.metrics['relationship_batches_processed']}")
        if self.metrics['relationship_batches_skipped'] > 0:
            print(f"   Batches skipped: {self.metrics['relationship_batches_skipped']}")
        if self.metrics['relationship_batches_failed'] > 0:
            print(f"   Batches failed: {self.metrics['relationship_batches_failed']}")
        print()
        print(f"❌ Errors: {self.metrics['errors']}")
        print(f"⚠️ Warnings: {self.metrics['warnings']}")
        
        if self.metrics['errors'] == 0:
            print("\n✅ Parallel ingestion completed successfully!")
        else:
            print("\n⚠️ Parallel ingestion completed with errors")
            
        print("=" * 70)
        
        return {
            'success': self.metrics['errors'] == 0,
            'total_time': total_time,
            'parallel_workers': self.parallel_workers,
            'total_retries': self.metrics['total_retries'],
            'entities_loaded': self.metrics['entities_loaded'],
            'entities_updated': self.metrics['entities_updated'],
            'entity_batches_processed': self.metrics['entity_batches_processed'],
            'entity_batches_skipped': self.metrics['entity_batches_skipped'],
            'entity_batches_failed': self.metrics['entity_batches_failed'],
            'relationships_loaded': self.metrics['relationships_loaded'],
            'relationship_batches_processed': self.metrics['relationship_batches_processed'],
            'relationship_batches_skipped': self.metrics['relationship_batches_skipped'],
            'relationship_batches_failed': self.metrics['relationship_batches_failed'],
            'errors': self.metrics['errors'],
            'warnings': self.metrics['warnings']
        }


def main():
    """Main entry point for parallel ingestion pipeline with command-line arguments."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Asset Brain Parallel Ingestion Pipeline')
    parser.add_argument('--mode', choices=['smart', 'basic', 'insert_only'], default='smart',
                      help='Upsert mode: smart (default), basic, or insert_only')
    parser.add_argument('--workers', type=int, default=4,
                      help='Number of parallel workers (default: 4)')
    parser.add_argument('--retries', type=int, default=3,
                      help='Number of retry attempts for failed batches (default: 3)')
    parser.add_argument('--data-dir', default='../data/test_data_final',
                      help='Data directory path (default: ../data/test_data_final)')
    parser.add_argument('--no-checkpoints', action='store_true',
                      help='Disable checkpoint system for resumable ingestion')
    
    args = parser.parse_args()
    
    # Configuration - Use real Spanner Graph (no emulator)
    DATA_DIR = args.data_dir
    PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT', 'your-project-id')
    INSTANCE_ID = os.getenv('SPANNER_INSTANCE_ID', 'your-instance-id')
    DATABASE_ID = os.getenv('SPANNER_DATABASE_ID', 'asset-brain-dev')
    
    print(f"🚀 Asset Brain Parallel Ingestion Configuration:")
    print(f"   Data Directory: {DATA_DIR}")
    print(f"   Upsert Mode: {args.mode}")
    print(f"   Parallel Workers: {args.workers}")
    print(f"   Retry Attempts: {args.retries}")
    print(f"   Checkpoints: {'Disabled' if args.no_checkpoints else 'Enabled'}")
    print(f"   Project ID: {PROJECT_ID}")
    print(f"   Instance ID: {INSTANCE_ID}")
    print(f"   Database ID: {DATABASE_ID}")
    print()
    
    if PROJECT_ID == 'your-project-id' or INSTANCE_ID == 'your-instance-id':
        print("⚠️ Please set environment variables:")
        print("   export GOOGLE_CLOUD_PROJECT=your-actual-project-id")
        print("   export SPANNER_INSTANCE_ID=your-actual-instance-id")
        print("   export SPANNER_DATABASE_ID=your-actual-database-id")
        return 1
    
    try:
        # Run parallel ingestion
        pipeline = IngestionPipeline(
            DATA_DIR, PROJECT_ID, INSTANCE_ID, DATABASE_ID, 
            upsert_mode=args.mode,
            parallel_workers=args.workers,
            retry_attempts=args.retries,
            enable_checkpoints=not args.no_checkpoints
        )
        results = pipeline.run_ingestion()
        
        return 0 if results['success'] else 1
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
