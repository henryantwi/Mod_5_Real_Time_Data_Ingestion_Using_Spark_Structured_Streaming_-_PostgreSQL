"""
Performance Metrics Tracker for Spark Streaming Pipeline
=========================================================
Tracks and logs performance metrics for the real-time data ingestion pipeline.

Metrics tracked:
- Batch processing time (latency)
- Records per batch (throughput)
- Validation success/failure rates
- Database write times
- End-to-end processing statistics
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from statistics import mean, median, stdev
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Metrics log directory
METRICS_LOG_DIR = os.environ.get("VALIDATION_LOG_DIR", "/opt/spark/work-dir/logs")
METRICS_FILE = os.path.join(METRICS_LOG_DIR, "performance_metrics.json")


@dataclass
class BatchMetrics:
    """Metrics for a single batch."""
    batch_id: int
    timestamp: str
    
    # Record counts
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    
    # Timing (in milliseconds)
    validation_time_ms: float = 0.0
    db_write_time_ms: float = 0.0
    total_processing_time_ms: float = 0.0
    
    # Calculated metrics
    validation_success_rate: float = 0.0
    records_per_second: float = 0.0


@dataclass
class AggregateMetrics:
    """Aggregate metrics across all batches."""
    start_time: str
    end_time: str
    total_batches: int = 0
    total_records_processed: int = 0
    total_valid_records: int = 0
    total_invalid_records: int = 0
    
    # Latency statistics (ms)
    avg_batch_latency_ms: float = 0.0
    min_batch_latency_ms: float = 0.0
    max_batch_latency_ms: float = 0.0
    median_batch_latency_ms: float = 0.0
    
    # Throughput statistics
    avg_records_per_batch: float = 0.0
    avg_records_per_second: float = 0.0
    peak_throughput_records_per_sec: float = 0.0
    
    # Validation statistics
    overall_validation_success_rate: float = 0.0
    
    # Database write statistics (ms)
    avg_db_write_time_ms: float = 0.0
    max_db_write_time_ms: float = 0.0


class PerformanceTracker:
    """
    Tracks performance metrics for the streaming pipeline.
    
    Usage:
        tracker = PerformanceTracker()
        
        # For each batch:
        tracker.start_batch(batch_id, record_count)
        # ... validation ...
        tracker.record_validation(valid_count, invalid_count)
        # ... db write ...
        tracker.record_db_write()
        tracker.end_batch()
        
        # Get summary:
        summary = tracker.get_summary()
    """
    
    def __init__(self):
        self.batch_metrics: List[BatchMetrics] = []
        self.current_batch: Optional[BatchMetrics] = None
        self._batch_start_time: float = 0
        self._validation_start_time: float = 0
        self._db_write_start_time: float = 0
        self._validation_end_time: float = 0
        self.start_time = datetime.now()
        
        # Ensure log directory exists
        os.makedirs(METRICS_LOG_DIR, exist_ok=True)
    
    def start_batch(self, batch_id: int, total_records: int) -> None:
        """Start tracking a new batch."""
        self._batch_start_time = time.perf_counter()
        self._validation_start_time = self._batch_start_time
        self.current_batch = BatchMetrics(
            batch_id=batch_id,
            timestamp=datetime.now().isoformat(),
            total_records=total_records
        )
    
    def record_validation(self, valid_count: int, invalid_count: int) -> None:
        """Record validation completion and stats."""
        if self.current_batch is None:
            return
        
        self._validation_end_time = time.perf_counter()
        self.current_batch.valid_records = valid_count
        self.current_batch.invalid_records = invalid_count
        self.current_batch.validation_time_ms = (
            (self._validation_end_time - self._validation_start_time) * 1000
        )
        
        # Calculate validation success rate
        total = valid_count + invalid_count
        if total > 0:
            self.current_batch.validation_success_rate = (valid_count / total) * 100
    
    def start_db_write(self) -> None:
        """Mark the start of database write operation."""
        self._db_write_start_time = time.perf_counter()
    
    def record_db_write(self) -> None:
        """Record database write completion."""
        if self.current_batch is None:
            return
        
        db_write_end = time.perf_counter()
        self.current_batch.db_write_time_ms = (
            (db_write_end - self._db_write_start_time) * 1000
        )
    
    def end_batch(self) -> BatchMetrics:
        """
        End batch tracking and calculate final metrics.
        Returns the completed batch metrics.
        """
        if self.current_batch is None:
            raise ValueError("No batch currently being tracked")
        
        batch_end_time = time.perf_counter()
        self.current_batch.total_processing_time_ms = (
            (batch_end_time - self._batch_start_time) * 1000
        )
        
        # Calculate throughput
        if self.current_batch.total_processing_time_ms > 0:
            self.current_batch.records_per_second = (
                self.current_batch.valid_records / 
                (self.current_batch.total_processing_time_ms / 1000)
            )
        
        # Store the batch metrics
        completed_batch = self.current_batch
        self.batch_metrics.append(completed_batch)
        self.current_batch = None
        
        # Log the batch metrics
        self._log_batch_metrics(completed_batch)
        
        return completed_batch
    
    def _log_batch_metrics(self, batch: BatchMetrics) -> None:
        """Log batch metrics to file."""
        try:
            with open(METRICS_FILE, 'a', encoding='utf-8') as f:
                f.write(json.dumps(asdict(batch)) + '\n')
        except Exception as e:
            logger.warning(f"Failed to write metrics to file: {e}")
    
    def get_summary(self) -> AggregateMetrics:
        """Calculate and return aggregate metrics across all batches."""
        if not self.batch_metrics:
            return AggregateMetrics(
                start_time=self.start_time.isoformat(),
                end_time=datetime.now().isoformat()
            )
        
        # Extract values for calculations
        latencies = [b.total_processing_time_ms for b in self.batch_metrics]
        throughputs = [b.records_per_second for b in self.batch_metrics]
        db_write_times = [b.db_write_time_ms for b in self.batch_metrics if b.db_write_time_ms > 0]
        
        total_valid = sum(b.valid_records for b in self.batch_metrics)
        total_invalid = sum(b.invalid_records for b in self.batch_metrics)
        total_records = total_valid + total_invalid
        
        summary = AggregateMetrics(
            start_time=self.start_time.isoformat(),
            end_time=datetime.now().isoformat(),
            total_batches=len(self.batch_metrics),
            total_records_processed=total_records,
            total_valid_records=total_valid,
            total_invalid_records=total_invalid,
            
            # Latency stats
            avg_batch_latency_ms=mean(latencies) if latencies else 0,
            min_batch_latency_ms=min(latencies) if latencies else 0,
            max_batch_latency_ms=max(latencies) if latencies else 0,
            median_batch_latency_ms=median(latencies) if latencies else 0,
            
            # Throughput stats
            avg_records_per_batch=total_records / len(self.batch_metrics) if self.batch_metrics else 0,
            avg_records_per_second=mean(throughputs) if throughputs else 0,
            peak_throughput_records_per_sec=max(throughputs) if throughputs else 0,
            
            # Validation stats
            overall_validation_success_rate=(total_valid / total_records * 100) if total_records > 0 else 0,
            
            # DB write stats
            avg_db_write_time_ms=mean(db_write_times) if db_write_times else 0,
            max_db_write_time_ms=max(db_write_times) if db_write_times else 0,
        )
        
        return summary
    
    def print_summary(self) -> None:
        """Print a formatted summary of performance metrics."""
        summary = self.get_summary()
        
        print("\n" + "=" * 70)
        print("PERFORMANCE METRICS SUMMARY")
        print("=" * 70)
        print(f"Tracking Period: {summary.start_time} to {summary.end_time}")
        print("-" * 70)
        
        print("\n📊 THROUGHPUT METRICS:")
        print(f"  Total Batches Processed:     {summary.total_batches}")
        print(f"  Total Records Processed:     {summary.total_records_processed}")
        print(f"  Average Records per Batch:   {summary.avg_records_per_batch:.1f}")
        print(f"  Average Throughput:          {summary.avg_records_per_second:.2f} records/sec")
        print(f"  Peak Throughput:             {summary.peak_throughput_records_per_sec:.2f} records/sec")
        
        print("\n⏱️ LATENCY METRICS:")
        print(f"  Average Batch Latency:       {summary.avg_batch_latency_ms:.2f} ms")
        print(f"  Minimum Batch Latency:       {summary.min_batch_latency_ms:.2f} ms")
        print(f"  Maximum Batch Latency:       {summary.max_batch_latency_ms:.2f} ms")
        print(f"  Median Batch Latency:        {summary.median_batch_latency_ms:.2f} ms")
        
        print("\n✅ VALIDATION METRICS:")
        print(f"  Total Valid Records:         {summary.total_valid_records}")
        print(f"  Total Invalid Records:       {summary.total_invalid_records}")
        print(f"  Validation Success Rate:     {summary.overall_validation_success_rate:.1f}%")
        
        print("\n💾 DATABASE WRITE METRICS:")
        print(f"  Average DB Write Time:       {summary.avg_db_write_time_ms:.2f} ms")
        print(f"  Maximum DB Write Time:       {summary.max_db_write_time_ms:.2f} ms")
        
        print("=" * 70 + "\n")
    
    def save_summary_to_file(self, filepath: Optional[str] = None) -> str:
        """
        Save the performance summary to a JSON file.
        Returns the filepath where summary was saved.
        """
        if filepath is None:
            filepath = os.path.join(METRICS_LOG_DIR, "performance_summary.json")
        
        summary = self.get_summary()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(asdict(summary), f, indent=2)
        
        return filepath


# Global tracker instance
_tracker: Optional[PerformanceTracker] = None


def get_tracker() -> PerformanceTracker:
    """Get or create the global performance tracker instance."""
    global _tracker
    if _tracker is None:
        _tracker = PerformanceTracker()
    return _tracker


def reset_tracker() -> PerformanceTracker:
    """Reset and return a new performance tracker instance."""
    global _tracker
    _tracker = PerformanceTracker()
    return _tracker
