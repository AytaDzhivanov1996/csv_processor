"""
CSV processor with filtering and aggregation capabilities.
"""

import argparse
import csv
import sys
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

try:
    from tabulate import tabulate
except ImportError:
    print("Warning: tabulate not installed. Using basic table output.")
    tabulate = None


class FilterOperator(ABC):
    """Abstract base class for filter operators."""
    
    @abstractmethod
    def apply(self, value: Any, target: Any) -> bool:
        """Apply the filter operation."""
        pass


class EqualsOperator(FilterOperator):
    """Equality filter operator."""
    
    def apply(self, value: Any, target: Any) -> bool:
        return str(value).strip() == str(target).strip()


class GreaterThanOperator(FilterOperator):
    """Greater than filter operator."""
    
    def apply(self, value: Any, target: Any) -> bool:
        try:
            return float(value) > float(target)
        except (ValueError, TypeError):
            return str(value) > str(target)


class LessThanOperator(FilterOperator):
    """Less than filter operator."""
    
    def apply(self, value: Any, target: Any) -> bool:
        try:
            return float(value) < float(target)
        except (ValueError, TypeError):
            return str(value) < str(target)


class AggregationFunction(ABC):
    """Abstract base class for aggregation functions."""
    
    @abstractmethod
    def calculate(self, values: List[Union[int, float]]) -> Union[int, float]:
        """Calculate the aggregation result."""
        pass


class AverageAggregation(AggregationFunction):
    """Average aggregation function."""
    
    def calculate(self, values: List[Union[int, float]]) -> float:
        if not values:
            return 0.0
        return sum(values) / len(values)


class MinAggregation(AggregationFunction):
    """Minimum aggregation function."""
    
    def calculate(self, values: List[Union[int, float]]) -> Union[int, float]:
        if not values:
            return 0
        return min(values)


class MaxAggregation(AggregationFunction):
    """Maximum aggregation function."""
    
    def calculate(self, values: List[Union[int, float]]) -> Union[int, float]:
        if not values:
            return 0
        return max(values)


class CSVProcessor:
    """Main CSV processing class."""
    
    def __init__(self):
        self.filter_operators = {
            'eq': EqualsOperator(),
            'gt': GreaterThanOperator(),
            'lt': LessThanOperator(),
        }
        
        self.aggregation_functions = {
            'avg': AverageAggregation(),
            'min': MinAggregation(),
            'max': MaxAggregation(),
        }
    
    def read_csv(self, filepath: str) -> tuple[List[str], List[Dict[str, str]]]:
        """Read CSV file and return headers and data."""
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                headers = reader.fieldnames or []
                data = list(reader)
                return headers, data
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {filepath}")
        except Exception as e:
            raise Exception(f"Error reading CSV file: {e}")
    
    def filter_data(self, data: List[Dict[str, str]], column: str, 
                   operator: str, value: str) -> List[Dict[str, str]]:
        """Filter data based on specified criteria."""
        if operator not in self.filter_operators:
            raise ValueError(f"Unsupported filter operator: {operator}")
        
        filter_op = self.filter_operators[operator]
        filtered_data = []
        
        for row in data:
            if column not in row:
                raise ValueError(f"Column '{column}' not found in CSV")
            
            if filter_op.apply(row[column], value):
                filtered_data.append(row)
        
        return filtered_data
    
    def aggregate_data(self, data: List[Dict[str, str]], column: str, 
                      function: str) -> Union[int, float]:
        """Aggregate data using specified function."""
        if function not in self.aggregation_functions:
            raise ValueError(f"Unsupported aggregation function: {function}")
        
        if not data:
            return 0
        
        if column not in data[0]:
            raise ValueError(f"Column '{column}' not found in CSV")
        
        # Extract numeric values
        values = []
        for row in data:
            try:
                value = float(row[column])
                values.append(value)
            except ValueError:
                raise ValueError(f"Non-numeric value found in column '{column}': {row[column]}")
        
        agg_func = self.aggregation_functions[function]
        return agg_func.calculate(values)
    
    def display_table(self, headers: List[str], data: List[Dict[str, str]]) -> None:
        """Display data as a formatted table."""
        if not data:
            print("No data to display.")
            return
        
        # Prepare table data
        table_data = []
        for row in data:
            table_data.append([row.get(header, '') for header in headers])
        
        if tabulate:
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        else:
            # Fallback to basic table format
            self._display_basic_table(headers, table_data)
    
    def _display_basic_table(self, headers: List[str], data: List[List[str]]) -> None:
        """Display table without tabulate library."""
        # Calculate column widths
        col_widths = [len(header) for header in headers]
        for row in data:
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    col_widths[i] = max(col_widths[i], len(str(cell)))
        
        # Print headers
        header_row = " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers))
        print(header_row)
        print("-" * len(header_row))
        
        # Print data rows
        for row in data:
            data_row = " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row))
            print(data_row)
    
    def display_aggregation_result(self, column: str, function: str, 
                                 result: Union[int, float]) -> None:
        """Display aggregation result."""
        print(f"\nAggregation Result:")
        print(f"Function: {function.upper()}")
        print(f"Column: {column}")
        print(f"Result: {result}")


def parse_filter_condition(condition: str) -> tuple[str, str, str]:
    """Parse filter condition string."""
    operators = ['>=', '<=', '>', '<', '=', '!=']
    
    for op in operators:
        if op in condition:
            parts = condition.split(op, 1)
            if len(parts) == 2:
                column = parts[0].strip()
                value = parts[1].strip()
                
                # Map operators to internal format
                op_mapping = {
                    '=': 'eq',
                    '>': 'gt',
                    '<': 'lt',
                    '>=': 'gte',  # Can be extended
                    '<=': 'lte',  # Can be extended
                    '!=': 'ne'    # Can be extended
                }
                
                if op in op_mapping:
                    return column, op_mapping[op], value
    
    raise ValueError(f"Invalid filter condition format: {condition}")


def parse_aggregation_condition(condition: str) -> tuple[str, str]:
    """Parse aggregation condition string."""
    if '=' not in condition:
        raise ValueError(f"Invalid aggregation condition format: {condition}")
    
    parts = condition.split('=', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid aggregation condition format: {condition}")
    
    function = parts[0].strip()
    column = parts[1].strip()
    
    return function, column


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='CSV Processor with filtering and aggregation')
    parser.add_argument('filepath', help='Path to CSV file')
    parser.add_argument('--filter', '-f', help='Filter condition (e.g., "price>500")')
    parser.add_argument('--aggregate', '-a', help='Aggregation condition (e.g., "avg=price")')
    
    args = parser.parse_args()
    
    if not args.filter and not args.aggregate:
        print("Error: Either --filter or --aggregate must be specified.")
        sys.exit(1)
    
    processor = CSVProcessor()
    
    try:
        # Read CSV file
        headers, data = processor.read_csv(args.filepath)
        
        if not data:
            print("No data found in CSV file.")
            return
        
        # Apply filter if specified
        if args.filter:
            try:
                column, operator, value = parse_filter_condition(args.filter)
                data = processor.filter_data(data, column, operator, value)
                print(f"Filtered by: {column} {operator} {value}")
                print(f"Records found: {len(data)}\n")
                processor.display_table(headers, data)
            except ValueError as e:
                print(f"Filter error: {e}")
                sys.exit(1)
        
        # Apply aggregation if specified
        if args.aggregate:
            try:
                function, column = parse_aggregation_condition(args.aggregate)
                result = processor.aggregate_data(data, column, function)
                processor.display_aggregation_result(column, function, result)
            except ValueError as e:
                print(f"Aggregation error: {e}")
                sys.exit(1)
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()