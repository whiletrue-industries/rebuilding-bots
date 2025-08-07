#!/usr/bin/env python3
"""
Test script using CLI commands to verify the enhanced logging system.

This script demonstrates how to use the existing CLI commands to test
the new logging, monitoring, and error reporting features.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a CLI command and display results."""
    print(f"\n🔧 {description}")
    print("=" * 60)
    print(f"Command: {command}")
    print("-" * 60)
    
    try:
        result = subprocess.run(
            command.split(),
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        if result.stdout:
            print("✅ Output:")
            print(result.stdout)
        
        if result.stderr:
            print("⚠️ Errors/Warnings:")
            print(result.stderr)
        
        print(f"Exit code: {result.returncode}")
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Command failed: {e}")
        return False


def main():
    """Run CLI-based tests."""
    print("🔍 CLI-Based Enhanced Logging System Test")
    print("=" * 60)
    print("This script tests the new logging system using existing CLI commands.\n")
    
    # Ensure we're in the right directory
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    # Create logs directory
    os.makedirs("./logs", exist_ok=True)
    
    # Test 1: Show cache statistics (tests basic logging)
    success1 = run_command(
        "python -m botnim.sync.cli stats",
        "Test 1: Cache Statistics (Basic Logging)"
    )
    
    # Test 2: Show sync logs (tests structured logging)
    success2 = run_command(
        "python -m botnim.sync.cli logs --limit 5",
        "Test 2: Sync Logs (Structured Logging)"
    )
    
    # Test 3: Run sync orchestration with enhanced logging
    success3 = run_command(
        "python -m botnim.sync.cli orchestrate --config-file specs/test-enhanced-logging-config.yaml --environment staging",
        "Test 3: Sync Orchestration (Enhanced Logging)"
    )
    
    # Test 4: Show comprehensive sync statistics (tests health checks and performance metrics)
    success4 = run_command(
        "python -m botnim.sync.cli sync-stats --config-file specs/test-enhanced-logging-config.yaml --environment staging",
        "Test 4: Sync Statistics (Health Checks & Performance Metrics)"
    )
    
    # Test 5: Show cache logs after sync
    success5 = run_command(
        "python -m botnim.sync.cli logs --limit 10",
        "Test 5: Cache Logs After Sync"
    )
    
    # Summary
    print("\n" + "=" * 60)
    print("🎯 CLI Test Summary")
    print("=" * 60)
    
    tests = [
        ("Cache Statistics", success1),
        ("Sync Logs", success2),
        ("Sync Orchestration", success3),
        ("Sync Statistics", success4),
        ("Post-Sync Logs", success5)
    ]
    
    passed = sum(1 for _, success in tests if success)
    total = len(tests)
    
    for test_name, success in tests:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All CLI tests passed! Enhanced logging system is working correctly.")
    else:
        print("⚠️ Some tests failed. Check the output above for details.")
    
    print("\n📋 Next Steps:")
    print("1. Check the log file at ./logs/sync.log for detailed JSON logs")
    print("2. Review the error tracking and performance metrics")
    print("3. Verify health check results in the sync statistics")
    print("4. Test with different configurations as needed")


if __name__ == "__main__":
    main() 