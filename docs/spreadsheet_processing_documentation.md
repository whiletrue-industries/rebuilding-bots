# Asynchronous Spreadsheet Processing Documentation

## Overview

The asynchronous spreadsheet processing system provides background processing capabilities for Google Sheets data sources. This system is designed to work as part of the automated sync infrastructure, allowing spreadsheet operations to run in the background without blocking the main sync workflow.

## Key Features

- **Asynchronous Processing**: Background task processing using thread pools
- **Google Sheets Integration**: Leverages existing Google Sheets API infrastructure
- **Intermediate Storage**: Stores fetched data in Elasticsearch for later processing
- **Task Management**: Comprehensive task tracking and status monitoring
- **Error Handling**: Robust error handling and retry logic
- **Integration**: Seamless integration with existing sync workflow

## Architecture

### Core Components

1. **AsyncSpreadsheetProcessor**: Main orchestrator for async spreadsheet operations
2. **SpreadsheetFetcher**: Async fetcher for Google Sheets data
3. **TaskQueue**: Thread-based background task queue
4. **IntermediateStorage**: Elasticsearch storage for spreadsheet data
5. **ProcessingTask**: Task tracking and status management

### Data Flow

```
Google Sheets → Async Fetch → Background Processing → Elasticsearch Storage
                                      ↓
                              Task Queue Management
```

## Configuration

### Spreadsheet Source Configuration

Spreadsheet sources are configured in the sync configuration file (e.g., `specs/takanon/sync_config.yaml`):

```yaml
- id: "oral-knowledge-spreadsheet"
  name: "ידע שבעל פה (Oral Knowledge)"
  description: "ידע שבעל פה על עבודת הכנסת והתקנון"
  type: "spreadsheet"
  spreadsheet_config:
    url: "https://docs.google.com/spreadsheets/d/1fEgiCLNMQQZqBgQFlkABXgke8I2kI1i1XUvj8Yba9Ow/edit?gid=0#gid=0"
    sheet_name: "תושב״ע"
    range: "A1:Z1000"
    use_adc: true
  versioning_strategy: "timestamp"
  fetch_strategy: "async"  # Must be "async" for background processing
  fetch_interval: 3600  # 1 hour
  enabled: true
  priority: 1
  tags: ["משפטי", "ידע-בעל-פה", "google-spreadsheet"]
```

### Configuration Parameters

- **`fetch_strategy`**: Must be set to `"async"` for background processing
- **`fetch_interval`**: Interval in seconds between fetch attempts
- **`use_adc`**: Use Application Default Credentials for Google Sheets API
- **`range`**: Cell range to fetch (e.g., "A1:Z1000")

## Usage

### Command Line Interface

#### Process Spreadsheet Sources

```bash
# Process all enabled spreadsheet sources
botnim sync spreadsheet process specs/takanon/sync_config.yaml

# Process specific sources
botnim sync spreadsheet process specs/takanon/sync_config.yaml \
  --source-ids oral-knowledge-spreadsheet committee-decisions-spreadsheet

# Process with custom settings
botnim sync spreadsheet process specs/takanon/sync_config.yaml \
  --environment production \
  --max-workers 5 \
  --verbose
```

#### Check Processing Status

```bash
# Check status of all tasks
botnim sync spreadsheet status specs/takanon/sync_config.yaml

# Check specific task
botnim sync spreadsheet status specs/takanon/sync_config.yaml \
  --task-id spreadsheet_oral-knowledge-spreadsheet_20240101_120000

# Check tasks for specific source
botnim sync spreadsheet status specs/takanon/sync_config.yaml \
  --source-id oral-knowledge-spreadsheet
```

#### Retrieve Stored Data

```bash
# Get spreadsheet data from storage
botnim sync spreadsheet data oral-knowledge-spreadsheet

# Get data with specific content hash
botnim sync spreadsheet data oral-knowledge-spreadsheet \
  --content-hash abc123def456

# Get data with custom settings
botnim sync spreadsheet data oral-knowledge-spreadsheet \
  --environment production \
  --limit 10
```

#### Clean Up Tasks

```bash
# Clean up completed tasks older than 24 hours (default)
botnim sync spreadsheet cleanup

# Clean up tasks older than 48 hours
botnim sync spreadsheet cleanup --max-age-hours 48

# Clean up with custom environment
botnim sync spreadsheet cleanup --environment production
```

### Programmatic Usage

```python
from botnim.sync.spreadsheet_fetcher import AsyncSpreadsheetProcessor, get_spreadsheet_data_from_storage
from botnim.sync.config import SyncConfig
from botnim.sync.cache import SyncCache
from botnim.vector_store.vector_store_es import VectorStoreES

# Load configuration
config = SyncConfig.from_yaml("specs/takanon/sync_config.yaml")
source = config.get_source_by_id("oral-knowledge-spreadsheet")

# Initialize components
cache = SyncCache()
vector_store = VectorStoreES(environment="staging")
processor = AsyncSpreadsheetProcessor(cache, vector_store)

# Process spreadsheet source asynchronously
import asyncio
result = asyncio.run(processor.process_spreadsheet_source(source))

print(f"Status: {result['status']}")
print(f"Task ID: {result['task_id']}")

# Check task status
task = processor.get_task_status(result['task_id'])
print(f"Task status: {task.status}")

# Get data from storage
data = get_spreadsheet_data_from_storage(source.id, vector_store)
if data:
    print(f"Retrieved {data['metadata']['row_count']} rows")

# Clean up
processor.shutdown()
```

**Real-World Example: Corporate Compliance Monitoring**

```python
from botnim.sync.spreadsheet_fetcher import AsyncSpreadsheetProcessor
from botnim.sync.config import SyncConfig
from botnim.sync.cache import SyncCache
from botnim.vector_store.vector_store_es import VectorStoreES
import asyncio
import logging
from datetime import datetime, timedelta

class ComplianceMonitor:
    """Monitor compliance data from multiple spreadsheet sources."""
    
    def __init__(self, config_path: str, environment: str = "production"):
        self.config = SyncConfig.from_yaml(config_path)
        self.cache = SyncCache()
        self.vector_store = VectorStoreES(environment=environment)
        self.processor = AsyncSpreadsheetProcessor(self.cache, self.vector_store, max_workers=5)
        self.logger = logging.getLogger(__name__)
    
    async def monitor_compliance_sources(self):
        """Monitor all compliance-related spreadsheet sources."""
        
        # Get all spreadsheet sources
        spreadsheet_sources = self.config.get_sources_by_type("spreadsheet")
        compliance_sources = [s for s in spreadsheet_sources if "compliance" in s.tags]
        
        self.logger.info(f"Monitoring {len(compliance_sources)} compliance sources")
        
        results = {}
        for source in compliance_sources:
            try:
                # Process each source
                result = await self.processor.process_spreadsheet_source(source)
                results[source.id] = result
                
                self.logger.info(f"Submitted task for {source.id}: {result['task_id']}")
                
            except Exception as e:
                self.logger.error(f"Failed to process {source.id}: {e}")
                results[source.id] = {"status": "error", "error": str(e)}
        
        return results
    
    async def wait_for_completion(self, results: dict, timeout_minutes: int = 30):
        """Wait for all tasks to complete with timeout."""
        
        start_time = datetime.now()
        timeout = timedelta(minutes=timeout_minutes)
        
        while datetime.now() - start_time < timeout:
            all_completed = True
            
            for source_id, result in results.items():
                if result.get("status") == "submitted":
                    task = self.processor.get_task_status(result["task_id"])
                    if task and task.status == "completed":
                        results[source_id]["status"] = "completed"
                        results[source_id]["result"] = task.result
                        self.logger.info(f"✅ {source_id} completed successfully")
                    elif task and task.status == "failed":
                        results[source_id]["status"] = "failed"
                        results[source_id]["error"] = task.error_message
                        self.logger.error(f"❌ {source_id} failed: {task.error_message}")
                    else:
                        all_completed = False
            
            if all_completed:
                break
            
            await asyncio.sleep(10)  # Check every 10 seconds
        
        return results
    
    def generate_compliance_report(self, results: dict):
        """Generate compliance monitoring report."""
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_sources": len(results),
            "successful": 0,
            "failed": 0,
            "pending": 0,
            "details": []
        }
        
        for source_id, result in results.items():
            status = result.get("status", "unknown")
            if status == "completed":
                report["successful"] += 1
            elif status == "failed":
                report["failed"] += 1
            else:
                report["pending"] += 1
            
            report["details"].append({
                "source_id": source_id,
                "status": status,
                "task_id": result.get("task_id"),
                "error": result.get("error"),
                "processing_time": result.get("processing_time")
            })
        
        return report
    
    async def run_compliance_monitoring(self):
        """Run complete compliance monitoring workflow."""
        
        try:
            # Step 1: Submit all compliance tasks
            self.logger.info("🚀 Starting compliance monitoring...")
            results = await self.monitor_compliance_sources()
            
            # Step 2: Wait for completion
            self.logger.info("⏳ Waiting for tasks to complete...")
            final_results = await self.wait_for_completion(results)
            
            # Step 3: Generate report
            report = self.generate_compliance_report(final_results)
            
            # Step 4: Log results
            self.logger.info(f"📊 Compliance monitoring completed:")
            self.logger.info(f"  - Total sources: {report['total_sources']}")
            self.logger.info(f"  - Successful: {report['successful']}")
            self.logger.info(f"  - Failed: {report['failed']}")
            self.logger.info(f"  - Pending: {report['pending']}")
            
            # Step 5: Alert on failures
            if report["failed"] > 0:
                self.logger.warning(f"⚠️ {report['failed']} compliance sources failed!")
                for detail in report["details"]:
                    if detail["status"] == "failed":
                        self.logger.error(f"  - {detail['source_id']}: {detail['error']}")
            
            return report
            
        finally:
            # Clean up
            self.processor.shutdown()
            self.logger.info("🧹 Compliance monitoring cleanup completed")

# Usage example
async def main():
    monitor = ComplianceMonitor("config/compliance_config.yaml")
    report = await monitor.run_compliance_monitoring()
    
    # Send report to monitoring system
    if report["failed"] > 0:
        # Send alert
        print("ALERT: Compliance monitoring failures detected!")
    else:
        print("✅ All compliance sources processed successfully")

if __name__ == "__main__":
    asyncio.run(main())
```

## Processing Workflow

### 1. Task Submission

When a spreadsheet source is processed:

- System checks if source should be processed (enabled, async strategy, etc.)
- Generates unique task ID
- Submits task to background thread pool
- Returns task ID for tracking

### 2. Background Processing

In the background thread:

- Fetches data from Google Sheets using existing API infrastructure
- Parses and structures the data
- Computes content hash for versioning
- Stores data in Elasticsearch intermediate storage
- Updates cache with processing results

### 3. Task Tracking

Throughout the process:

- Task status is tracked (pending → processing → completed/failed)
- Timestamps are recorded for each stage
- Error messages are captured if processing fails
- Results are stored for later retrieval

### 4. Data Storage

Fetched data is stored in Elasticsearch with:

- **Content**: JSON-serialized spreadsheet data
- **Metadata**: Source information, headers, row count, timestamps
- **Processing Status**: Marked as "intermediate" for later processing
- **Version Information**: Content hash and fetch timestamp

## Task Management

### Task States

- **`pending`**: Task submitted, waiting to be processed
- **`processing`**: Task currently being executed
- **`completed`**: Task completed successfully
- **`failed`**: Task failed with error

### Task Information

Each task includes:

- **Task ID**: Unique identifier
- **Source ID**: Associated spreadsheet source
- **Status**: Current processing state
- **Timestamps**: Created, started, completed times
- **Error Message**: Details if task failed
- **Result**: Processing results if successful

### Task Cleanup

Completed and failed tasks are automatically cleaned up:

- Default cleanup age: 24 hours
- Configurable via `--max-age-hours` parameter
- Helps prevent memory accumulation
- Maintains clean task history

## Error Handling

### Common Error Scenarios

1. **Google Sheets API Errors**: Authentication, permissions, rate limits
2. **Network Errors**: Connection timeouts, DNS resolution
3. **Data Processing Errors**: Invalid data format, parsing failures
4. **Storage Errors**: Elasticsearch connection, index creation

### Error Recovery

- Failed tasks are marked with error status
- Error messages are captured and stored
- Tasks can be retried by resubmitting
- Partial failures don't affect other tasks

### Monitoring

- Task status can be monitored via CLI commands
- Error messages provide detailed failure information
- Processing statistics are available
- Background task queue health can be checked

## Integration with Main Sync

### Asynchronous Operation

Spreadsheet processing runs independently:

- Does not block main sync workflow
- Can run in parallel with other source types
- Results are stored for later integration
- Main sync can access stored data when needed

### Data Access

Stored spreadsheet data can be accessed by:

- Main sync workflow for embedding and vectorization
- CLI commands for data inspection
- Programmatic access for custom processing
- Integration with other sync components

### Version Management

- Content hashes track data changes
- Duplicate detection prevents reprocessing
- Version information stored in cache
- Incremental updates supported

## Performance Considerations

### Thread Pool Management

- Configurable number of worker threads
- Default: 3 concurrent tasks
- Can be adjusted based on system resources
- Thread pool automatically manages task execution

### Memory Usage

- Tasks are processed in background threads
- Data is stored in Elasticsearch, not memory
- Task metadata is kept in memory for tracking
- Automatic cleanup prevents memory leaks

### Network Optimization

- Uses existing Google Sheets API infrastructure
- Leverages connection pooling
- Handles rate limits and retries
- Efficient data transfer and storage

## Security

### Authentication

- Uses Application Default Credentials (ADC)
- Supports service account authentication
- Secure credential management
- No hardcoded credentials

### Data Protection

- Data stored in Elasticsearch with proper access controls
- Content hashing for integrity verification
- Secure transmission over HTTPS
- Audit trail through task tracking

## Testing

### Unit Tests

Run the test suite:

```bash
python -m pytest botnim/sync/tests/test_spreadsheet_fetcher.py -v
```

### Integration Tests

Test with real sources:

```bash
# Test with a small, controlled source
botnim sync spreadsheet process test_config.yaml \
  --source-ids test-spreadsheet-source \
  --environment local
```

### Test Coverage

Tests cover:

- Task queue functionality
- Spreadsheet data fetching
- Error handling scenarios
- Integration with existing components
- CLI command functionality

## Troubleshooting

### Common Issues

1. **Task Stuck in Processing**
   - Check for long-running operations
   - Verify Google Sheets API access
   - Review error logs for details

2. **Authentication Failures**
   - Verify ADC setup
   - Check service account permissions
   - Ensure Google Sheets API is enabled

3. **Storage Errors**
   - Verify Elasticsearch connectivity
   - Check index permissions
   - Review storage configuration

4. **Data Not Retrieved**
   - Verify source configuration
   - Check task completion status
   - Review error messages

### Debug Mode

Enable verbose logging for detailed troubleshooting:

```bash
botnim sync spreadsheet process config.yaml --verbose
```

### Log Analysis

Key log messages to monitor:

- Task submission and status changes
- Google Sheets API operations
- Storage operations
- Error messages and stack traces

## Future Enhancements

### Planned Features

1. **Batch Processing**: Process multiple spreadsheets in parallel
2. **Incremental Updates**: Process only changed data
3. **Webhook Integration**: Notify external systems of new data
4. **Advanced Filtering**: More sophisticated data selection
5. **Performance Monitoring**: Detailed metrics and analytics

### Performance Optimizations

1. **Caching**: Cache spreadsheet metadata
2. **Streaming**: Process data as it's fetched
3. **Compression**: Compress stored data
4. **Indexing**: Optimize Elasticsearch queries

### Integration Enhancements

1. **Real-time Processing**: Process data as it changes
2. **Event-driven Architecture**: Trigger processing on data changes
3. **Distributed Processing**: Scale across multiple nodes
4. **Advanced Scheduling**: Sophisticated task scheduling 