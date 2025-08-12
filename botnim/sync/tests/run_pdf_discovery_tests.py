#!/usr/bin/env python3
"""
PDF Discovery Feature - Test Runner

This script runs systematic tests for the PDF discovery and processing feature.
Run this after setting up the local test environment.
"""

import subprocess
import time
import json
import os
import sys
from pathlib import Path

def run_command(command, description):
    """Run a command and return success status."""
    print(f"\n🔄 {description}")
    print(f"Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            if result.stdout:
                print("Output:", result.stdout)
            return True, result.stdout
        else:
            print(f"❌ {description} - FAILED")
            print("Error:", result.stderr)
            return False, result.stderr
    except Exception as e:
        print(f"❌ {description} - EXCEPTION: {e}")
        return False, str(e)

def check_elasticsearch():
    """Check if Elasticsearch is running."""
    return run_command(
        "curl -s -u elastic:elastic123 -X GET 'localhost:9200/_cluster/health?pretty'",
        "Checking Elasticsearch connection"
    )

def start_test_server():
    """Start the test HTTP server."""
    print("\n🌐 Starting test HTTP server...")
    print("Note: This will run in the background. Stop it with Ctrl+C when done.")
    
    # Start server in background
    server_process = subprocess.Popen(
        ["python", "-m", "http.server", "8000"],
        cwd=".",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait a moment for server to start
    time.sleep(2)
    
    # Test if server is responding
    success, _ = run_command(
        "curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/test_index.html",
        "Testing HTTP server response"
    )
    
    if success:
        print("✅ Test server started successfully")
        return server_process
    else:
        print("❌ Test server failed to start")
        server_process.terminate()
        return None

def test_basic_discovery():
    """Test basic PDF discovery functionality."""
    return run_command(
        "python -m botnim.sync.cli pdf-discover "
        "--config-file test_sync_config.yaml "
        "--source-id test-pdf-source "
        "--environment local",
        "Testing basic PDF discovery"
    )

def test_status_check():
    """Test PDF status checking."""
    return run_command(
        "python -m botnim.sync.cli pdf-status "
        "--source-id test-pdf-source "
        "--environment local "
        "--limit 10",
        "Checking PDF processing status"
    )

def test_duplicate_prevention():
    """Test duplicate prevention by running discovery twice."""
    print("\n🔄 Testing duplicate prevention...")
    
    # First run
    success1, output1 = test_basic_discovery()
    if not success1:
        return False, "First discovery run failed"
    
    # Second run (should skip all PDFs)
    success2, output2 = test_basic_discovery()
    if not success2:
        return False, "Second discovery run failed"
    
    # Check if second run shows 0 new PDFs
    if "0 new PDFs" in output2 or "No PDFs discovered" in output2:
        print("✅ Duplicate prevention working correctly")
        return True, "Duplicate prevention test passed"
    else:
        print("❌ Duplicate prevention not working")
        return False, "Duplicate prevention test failed"

def test_filtered_discovery():
    """Test discovery with file pattern filter."""
    return run_command(
        "python -m botnim.sync.cli pdf-discover "
        "--config-file test_sync_config.yaml "
        "--source-id test-pdf-source-filtered "
        "--environment local",
        "Testing filtered PDF discovery (test_document_*.pdf)"
    )

def check_elasticsearch_indices():
    """Check if Elasticsearch indices were created."""
    return run_command(
        "curl -s -u elastic:elastic123 -X GET 'localhost:9200/_cat/indices?v'",
        "Checking Elasticsearch indices"
    )

def check_pdf_tracker():
    """Check PDF processing tracker index."""
    return run_command(
        "curl -s -u elastic:elastic123 -X GET 'localhost:9200/pdf_processing_tracker/_search?pretty'",
        "Checking PDF processing tracker"
    )

def cleanup_test_files():
    """Clean up any test files."""
    return run_command(
        "find /tmp -name 'pdf_sync_*' -type d -exec rm -rf {} + 2>/dev/null || true",
        "Cleaning up test temporary files"
    )

def main():
    """Main test runner."""
    print("🧪 PDF Discovery Feature - Test Runner")
    print("=" * 50)
    
    # Test results
    results = {
        "elasticsearch": False,
        "test_server": False,
        "basic_discovery": False,
        "status_check": False,
        "duplicate_prevention": False,
        "filtered_discovery": False,
        "indices_created": False,
        "tracker_working": False
    }
    
    # Step 1: Check Elasticsearch
    success, _ = check_elasticsearch()
    results["elasticsearch"] = success
    if not success:
        print("❌ Elasticsearch not available. Please start Elasticsearch first.")
        return
    
    # Step 2: Start test server
    server_process = start_test_server()
    if server_process:
        results["test_server"] = True
    else:
        print("❌ Cannot start test server. Exiting.")
        return
    
    try:
        # Step 3: Basic discovery test
        success, _ = test_basic_discovery()
        results["basic_discovery"] = success
        
        # Step 4: Status check test
        success, _ = test_status_check()
        results["status_check"] = success
        
        # Step 5: Duplicate prevention test
        success, _ = test_duplicate_prevention()
        results["duplicate_prevention"] = success
        
        # Step 6: Filtered discovery test
        success, _ = test_filtered_discovery()
        results["filtered_discovery"] = success
        
        # Step 7: Check Elasticsearch indices
        success, _ = check_elasticsearch_indices()
        results["indices_created"] = success
        
        # Step 8: Check PDF tracker
        success, _ = check_pdf_tracker()
        results["tracker_working"] = success
        
    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        if server_process:
            server_process.terminate()
            server_process.wait()
        
        cleanup_test_files()
    
    # Print results summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
    
    passed_count = sum(results.values())
    total_count = len(results)
    
    print(f"\nOverall: {passed_count}/{total_count} tests passed")
    
    if passed_count == total_count:
        print("🎉 All tests passed! Feature is ready for staging.")
    else:
        print("⚠️  Some tests failed. Please review the issues above.")
    
    return results

if __name__ == "__main__":
    main() 