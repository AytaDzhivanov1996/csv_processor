"""
Tests for CSV processor.
"""

import pytest
import tempfile
import os
from csv_processor import (
    CSVProcessor, 
    EqualsOperator, 
    GreaterThanOperator, 
    LessThanOperator,
    AverageAggregation,
    MinAggregation,
    MaxAggregation,
    parse_filter_condition,
    parse_aggregation_condition
)


class TestFilterOperators:
    """Test filter operators."""
    
    def test_equals_operator(self):
        op = EqualsOperator()
        assert op.apply("apple", "apple") is True
        assert op.apply("apple", "banana") is False
        assert op.apply("  apple  ", "apple") is True
        assert op.apply("123", "123") is True
    
    def test_greater_than_operator(self):
        op = GreaterThanOperator()
        assert op.apply("10", "5") is True
        assert op.apply("5", "10") is False
        assert op.apply("10.5", "10") is True
        assert op.apply("banana", "apple") is True  # String comparison
    
    def test_less_than_operator(self):
        op = LessThanOperator()
        assert op.apply("5", "10") is True
        assert op.apply("10", "5") is False
        assert op.apply("9.5", "10") is True
        assert op.apply("apple", "banana") is True  # String comparison


class TestAggregationFunctions:
    """Test aggregation functions."""
    
    def test_average_aggregation(self):
        agg = AverageAggregation()
        assert agg.calculate([1, 2, 3, 4, 5]) == 3.0
        assert agg.calculate([10.5, 20.5]) == 15.5
        assert agg.calculate([]) == 0.0
        assert agg.calculate([42]) == 42.0
    
    def test_min_aggregation(self):
        agg = MinAggregation()
        assert agg.calculate([1, 2, 3, 4, 5]) == 1
        assert agg.calculate([10.5, 5.2, 20.1]) == 5.2
        assert agg.calculate([]) == 0
        assert agg.calculate([42]) == 42
    
    def test_max_aggregation(self):
        agg = MaxAggregation()
        assert agg.calculate([1, 2, 3, 4, 5]) == 5
        assert agg.calculate([10.5, 5.2, 20.1]) == 20.1
        assert agg.calculate([]) == 0
        assert agg.calculate([42]) == 42


class TestCSVProcessor:
    """Test CSV processor functionality."""
    
    @pytest.fixture
    def sample_csv_file(self):
        """Create a temporary CSV file for testing."""
        csv_content = """name,brand,price,rating
iphone 15 pro,apple,999,4.9
galaxy s23 ultra,samsung,1199,4.8
redmi note 12,xiaomi,199,4.6
poco x5 pro,xiaomi,299,4.4"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_filepath = f.name
        
        yield temp_filepath
        
        # Cleanup
        os.unlink(temp_filepath)
    
    @pytest.fixture
    def processor(self):
        """Create CSV processor instance."""
        return CSVProcessor()
    
    def test_read_csv(self, processor, sample_csv_file):
        """Test CSV reading functionality."""
        headers, data = processor.read_csv(sample_csv_file)
        
        assert headers == ['name', 'brand', 'price', 'rating']
        assert len(data) == 4
        assert data[0]['name'] == 'iphone 15 pro'
        assert data[0]['brand'] == 'apple'
        assert data[0]['price'] == '999'
    
    def test_read_csv_file_not_found(self, processor):
        """Test error handling for missing file."""
        with pytest.raises(FileNotFoundError):
            processor.read_csv('nonexistent_file.csv')
    
    def test_filter_data_equals(self, processor, sample_csv_file):
        """Test filtering with equals operator."""
        headers, data = processor.read_csv(sample_csv_file)
        
        filtered = processor.filter_data(data, 'brand', 'eq', 'xiaomi')
        assert len(filtered) == 2
        assert all(row['brand'] == 'xiaomi' for row in filtered)
    
    def test_filter_data_greater_than(self, processor, sample_csv_file):
        """Test filtering with greater than operator."""
        headers, data = processor.read_csv(sample_csv_file)
        
        filtered = processor.filter_data(data, 'price', 'gt', '500')
        assert len(filtered) == 2
        assert all(float(row['price']) > 500 for row in filtered)
    
    def test_filter_data_less_than(self, processor, sample_csv_file):
        """Test filtering with less than operator."""
        headers, data = processor.read_csv(sample_csv_file)
        
        filtered = processor.filter_data(data, 'price', 'lt', '300')
        assert len(filtered) == 2
        assert all(float(row['price']) < 300 for row in filtered)
    
    def test_filter_data_invalid_column(self, processor, sample_csv_file):
        """Test error handling for invalid column."""
        headers, data = processor.read_csv(sample_csv_file)
        
        with pytest.raises(ValueError, match="Column 'invalid_column' not found"):
            processor.filter_data(data, 'invalid_column', 'eq', 'test')
    
    def test_filter_data_invalid_operator(self, processor, sample_csv_file):
        """Test error handling for invalid operator."""
        headers, data = processor.read_csv(sample_csv_file)
        
        with pytest.raises(ValueError, match="Unsupported filter operator"):
            processor.filter_data(data, 'brand', 'invalid_op', 'test')
    
    def test_aggregate_data_average(self, processor, sample_csv_file):
        """Test average aggregation."""
        headers, data = processor.read_csv(sample_csv_file)
        
        result = processor.aggregate_data(data, 'price', 'avg')
        expected = (999 + 1199 + 199 + 299) / 4
        assert result == expected
    
    def test_aggregate_data_min(self, processor, sample_csv_file):
        """Test minimum aggregation."""
        headers, data = processor.read_csv(sample_csv_file)
        
        result = processor.aggregate_data(data, 'price', 'min')
        assert result == 199.0
    
    def test_aggregate_data_max(self, processor, sample_csv_file):
        """Test maximum aggregation."""
        headers, data = processor.read_csv(sample_csv_file)
        
        result = processor.aggregate_data(data, 'price', 'max')
        assert result == 1199.0
    
    def test_aggregate_data_empty_dataset(self, processor):
        """Test aggregation on empty dataset."""
        result = processor.aggregate_data([], 'price', 'avg')
        assert result == 0
    
    def test_aggregate_data_invalid_column(self, processor, sample_csv_file):
        """Test error handling for invalid column in aggregation."""
        headers, data = processor.read_csv(sample_csv_file)
        
        with pytest.raises(ValueError, match="Column 'invalid_column' not found"):
            processor.aggregate_data(data, 'invalid_column', 'avg')
    
    def test_aggregate_data_invalid_function(self, processor, sample_csv_file):
        """Test error handling for invalid aggregation function."""
        headers, data = processor.read_csv(sample_csv_file)
        
        with pytest.raises(ValueError, match="Unsupported aggregation function"):
            processor.aggregate_data(data, 'price', 'invalid_func')
    
    def test_aggregate_data_non_numeric_values(self, processor, sample_csv_file):
        """Test error handling for non-numeric values in aggregation."""
        headers, data = processor.read_csv(sample_csv_file)
        
        with pytest.raises(ValueError, match="Non-numeric value found"):
            processor.aggregate_data(data, 'brand', 'avg')


class TestConditionParsing:
    """Test condition parsing functions."""
    
    def test_parse_filter_condition_equals(self):
        """Test parsing equals filter condition."""
        column, operator, value = parse_filter_condition("brand=apple")
        assert column == "brand"
        assert operator == "eq"
        assert value == "apple"
    
    def test_parse_filter_condition_greater_than(self):
        """Test parsing greater than filter condition."""
        column, operator, value = parse_filter_condition("price>500")
        assert column == "price"
        assert operator == "gt"
        assert value == "500"
    
    def test_parse_filter_condition_less_than(self):
        """Test parsing less than filter condition."""
        column, operator, value = parse_filter_condition("rating<4.5")
        assert column == "rating"
        assert operator == "lt"
        assert value == "4.5"
    
    def test_parse_filter_condition_with_spaces(self):
        """Test parsing filter condition with spaces."""
        column, operator, value = parse_filter_condition("  brand  =  apple  ")
        assert column == "brand"
        assert operator == "eq"
        assert value == "apple"
    
    def test_parse_filter_condition_invalid(self):
        """Test error handling for invalid filter condition."""
        with pytest.raises(ValueError, match="Invalid filter condition format"):
            parse_filter_condition("invalid_condition")
    
    def test_parse_aggregation_condition_valid(self):
        """Test parsing valid aggregation condition."""
        function, column = parse_aggregation_condition("avg=price")
        assert function == "avg"
        assert column == "price"
    
    def test_parse_aggregation_condition_with_spaces(self):
        """Test parsing aggregation condition with spaces."""
        function, column = parse_aggregation_condition("  max  =  rating  ")
        assert function == "max"
        assert column == "rating"
    
    def test_parse_aggregation_condition_invalid(self):
        """Test error handling for invalid aggregation condition."""
        with pytest.raises(ValueError, match="Invalid aggregation condition format"):
            parse_aggregation_condition("invalid_condition")
    
    # def test_parse_aggregation_condition_multiple_equals(self):
    #     """Test error handling for multiple equals signs."""
    #     with pytest.raises(ValueError, match="Invalid aggregation condition format"):
    #         parse_aggregation_condition("avg=price=test")


class TestIntegration:
    """Integration tests."""
    
    @pytest.fixture
    def complex_csv_file(self):
        """Create a more complex CSV file for integration testing."""
        csv_content = """id,category,value,score,active
1,electronics,150.5,8.9,true
2,books,25.0,7.2,false
3,electronics,299.99,9.1,true
4,clothing,75.5,6.8,true
5,books,15.99,8.5,false
6,electronics,199.0,8.0,true"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_filepath = f.name
        
        yield temp_filepath
        
        # Cleanup
        os.unlink(temp_filepath)
    
    def test_filter_then_aggregate(self, complex_csv_file):
        """Test filtering followed by aggregation."""
        processor = CSVProcessor()
        headers, data = processor.read_csv(complex_csv_file)
        
        # Filter for electronics
        filtered_data = processor.filter_data(data, 'category', 'eq', 'electronics')
        assert len(filtered_data) == 3
        
        # Aggregate the filtered data
        avg_value = processor.aggregate_data(filtered_data, 'value', 'avg')
        expected_avg = (150.5 + 299.99 + 199.0) / 3
        assert abs(avg_value - expected_avg) < 0.01
    
    def test_multiple_operations(self, complex_csv_file):
        """Test multiple filter and aggregation operations."""
        processor = CSVProcessor()
        headers, data = processor.read_csv(complex_csv_file)
        
        # Test different filters
        electronics = processor.filter_data(data, 'category', 'eq', 'electronics')
        expensive_items = processor.filter_data(data, 'value', 'gt', '100')
        high_score_items = processor.filter_data(data, 'score', 'gt', '8.0')
        
        assert len(electronics) == 3
        assert len(expensive_items) == 3
        assert len(high_score_items) == 3
        
        # Test different aggregations
        avg_value = processor.aggregate_data(data, 'value', 'avg')
        min_score = processor.aggregate_data(data, 'score', 'min')
        max_value = processor.aggregate_data(data, 'value', 'max')
        
        assert avg_value > 0
        assert min_score == 6.8
        assert max_value == 299.99


if __name__ == "__main__":
    pytest.main([__file__])