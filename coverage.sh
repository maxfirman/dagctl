#!/bin/bash
# Script to generate code coverage report

set -e

echo "Running tests with coverage..."
cargo llvm-cov --all-features --workspace --html

echo ""
echo "Coverage report generated!"
echo "Open target/llvm-cov/html/index.html in your browser to view the report"
echo ""

# Print summary
cargo llvm-cov --all-features --workspace
