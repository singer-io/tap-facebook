import unittest
from tap_facebook import parse_action_breakdowns, ALL_ACTION_BREAKDOWNS


class TestParseActionBreakdowns(unittest.TestCase):
    """Test suite for parse_action_breakdowns function"""

    def test_none_returns_all_breakdowns(self):
        """Test that None input returns all default breakdowns"""
        result = parse_action_breakdowns(None)
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        self.assertEqual(len(result), 3)

    def test_empty_string_returns_all_breakdowns(self):
        """Test that empty string returns all default breakdowns"""
        result = parse_action_breakdowns("")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        self.assertEqual(len(result), 3)

    def test_non_string_type_returns_all_breakdowns(self):
        """Test that non-string types return all default breakdowns with warning"""
        result = parse_action_breakdowns(123)
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        
        result = parse_action_breakdowns(['action_type'])
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        
        result = parse_action_breakdowns({'breakdown': 'action_type'})
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)

    def test_single_valid_breakdown(self):
        """Test parsing a single valid breakdown"""
        result = parse_action_breakdowns("action_type")
        self.assertEqual(result, ['action_type'])
        self.assertEqual(len(result), 1)

    def test_multiple_valid_breakdowns(self):
        """Test parsing multiple valid breakdowns"""
        result = parse_action_breakdowns("action_type,action_destination")
        self.assertEqual(result, ['action_type', 'action_destination'])
        self.assertEqual(len(result), 2)

    def test_all_valid_breakdowns(self):
        """Test parsing all three valid breakdowns"""
        result = parse_action_breakdowns("action_type,action_target_id,action_destination")
        self.assertEqual(result, ['action_type', 'action_target_id', 'action_destination'])
        self.assertEqual(len(result), 3)

    def test_whitespace_handling(self):
        """Test that whitespace is properly stripped"""
        result = parse_action_breakdowns(" action_type , action_destination ")
        self.assertEqual(result, ['action_type', 'action_destination'])
        
        result = parse_action_breakdowns("  action_type  ,  action_target_id  ")
        self.assertEqual(result, ['action_type', 'action_target_id'])

    def test_case_insensitivity(self):
        """Test that input is case-insensitive"""
        result = parse_action_breakdowns("ACTION_TYPE,Action_Destination")
        self.assertEqual(result, ['action_type', 'action_destination'])
        
        result = parse_action_breakdowns("ACTION_TARGET_ID")
        self.assertEqual(result, ['action_target_id'])

    def test_mixed_valid_and_invalid(self):
        """Test parsing mix of valid and invalid breakdowns"""
        result = parse_action_breakdowns("action_type,invalid_breakdown,action_destination")
        self.assertEqual(result, ['action_type', 'action_destination'])
        self.assertEqual(len(result), 2)
        
        result = parse_action_breakdowns("invalid1,action_type,invalid2")
        self.assertEqual(result, ['action_type'])

    def test_all_invalid_returns_all_breakdowns(self):
        """Test that all invalid values returns default breakdowns"""
        result = parse_action_breakdowns("invalid1,invalid2,invalid3")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        
        result = parse_action_breakdowns("foo,bar,baz")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)

    def test_empty_values_skipped(self):
        """Test that empty values from comma-separation are skipped"""
        result = parse_action_breakdowns("action_type,,action_destination")
        self.assertEqual(result, ['action_type', 'action_destination'])
        
        result = parse_action_breakdowns(",action_type,")
        self.assertEqual(result, ['action_type'])
        
        result = parse_action_breakdowns(",,action_type,,")
        self.assertEqual(result, ['action_type'])

    def test_only_commas_returns_all_breakdowns(self):
        """Test that only commas returns default breakdowns"""
        result = parse_action_breakdowns(",,,")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)

    def test_duplicate_breakdowns(self):
        """Test handling of duplicate breakdown values"""
        result = parse_action_breakdowns("action_type,action_type,action_destination")
        # Should not include duplicates since deduplication is performed
        self.assertEqual(result, ['action_type', 'action_destination'])

    def test_whitespace_only_string(self):
        """Test whitespace-only string is treated as empty"""
        result = parse_action_breakdowns("   ")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        
        result = parse_action_breakdowns("\t\n")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)

    def test_partial_match_not_accepted(self):
        """Test that partial matches are not accepted"""
        result = parse_action_breakdowns("action_typ")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)
        
        result = parse_action_breakdowns("action_type_extra")
        self.assertEqual(result, ALL_ACTION_BREAKDOWNS)

    def test_order_preserved(self):
        """Test that order of valid breakdowns is preserved"""
        result = parse_action_breakdowns("action_destination,action_type")
        self.assertEqual(result, ['action_destination', 'action_type'])
        
        result = parse_action_breakdowns("action_target_id,action_destination,action_type")
        self.assertEqual(result, ['action_target_id', 'action_destination', 'action_type'])