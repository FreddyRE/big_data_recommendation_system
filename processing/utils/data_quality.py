# 2-processing/bronze-to-silver/utils/data_quality.py
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import re
import json
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class ValidationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ValidationRule:
    """Data validation rule definition."""
    field: str
    rule_type: str
    parameters: Dict[str, Any]
    severity: ValidationSeverity
    description: str
    
@dataclass
class ValidationResult:
    """Result of a single validation."""
    rule: ValidationRule
    passed: bool
    message: str
    field_value: Any = None
    suggested_fix: str = None

class DataQualityValidator:
    """
    Comprehensive data quality validation for bronze to silver transformation.
    Implements configurable validation rules with detailed reporting.
    """
    
    def __init__(self, config):
        self.config = config
        self.validation_rules = self._load_validation_rules()
        self.stats_tracker = ValidationStatsTracker()
        
    def _load_validation_rules(self) -> Dict[str, List[ValidationRule]]:
        """Load validation rules for each data type."""
        return {
            'clickstream': self._get_clickstream_rules(),
            'user_features': self._get_user_features_rules(),
            'product_features': self._get_product_features_rules()
        }
    
    def _get_clickstream_rules(self) -> List[ValidationRule]:
        """Define validation rules for clickstream data."""
        return [
            ValidationRule(
                field='user_id',
                rule_type='required',
                parameters={},
                severity=ValidationSeverity.ERROR,
                description='User ID must be present'
            ),
            ValidationRule(
                field='user_id',
                rule_type='format',
                parameters={'pattern': r'^[a-zA-Z0-9_-]+$', 'min_length': 5, 'max_length': 50},
                severity=ValidationSeverity.ERROR,
                description='User ID must be alphanumeric with underscores/hyphens, 5-50 chars'
            ),
            ValidationRule(
                field='timestamp',
                rule_type='timestamp_valid',
                parameters={'format': 'iso8601'},
                severity=ValidationSeverity.ERROR,
                description='Timestamp must be valid ISO8601 format'
            ),
            ValidationRule(
                field='timestamp',
                rule_type='timestamp_range',
                parameters={'max_days_old': 30, 'max_days_future': 1},
                severity=ValidationSeverity.WARNING,
                description='Timestamp should be within reasonable range'
            ),
            ValidationRule(
                field='event_type',
                rule_type='enum',
                parameters={'allowed_values': ['view', 'click', 'purchase', 'add_to_cart', 'search']},
                severity=ValidationSeverity.ERROR,
                description='Event type must be one of allowed values'
            ),
            ValidationRule(
                field='product_id',
                rule_type='conditional_required',
                parameters={'condition_field': 'event_type', 'condition_values': ['view', 'click', 'purchase', 'add_to_cart']},
                severity=ValidationSeverity.ERROR,
                description='Product ID required for product-related events'
            ),
            ValidationRule(
                field='session_id',
                rule_type='required',
                parameters={},
                severity=ValidationSeverity.WARNING,
                description='Session ID should be present for better analytics'
            ),
            ValidationRule(
                field='page_url',
                rule_type='url_valid',
                parameters={},
                severity=ValidationSeverity.WARNING,
                description='Page URL should be valid if present'
            ),
            ValidationRule(
                field='revenue',
                rule_type='numeric_range',
                parameters={'min_value': 0, 'max_value': 10000},
                severity=ValidationSeverity.WARNING,
                description='Revenue should be reasonable positive value'
            )
        ]
    
    def _get_user_features_rules(self) -> List[ValidationRule]:
        """Define validation rules for user features."""
        return [
            ValidationRule(
                field='user_id',
                rule_type='required',
                parameters={},
                severity=ValidationSeverity.ERROR,
                description='User ID must be present'
            ),
            ValidationRule(
                field='user_id',
                rule_type='format',
                parameters={'pattern': r'^[a-zA-Z0-9_-]+, 'min_length': 5, 'max_length': 50},
                severity=ValidationSeverity.ERROR,
                description='User ID must be alphanumeric with underscores/hyphens'
            ),
            ValidationRule(
                field='age',
                rule_type='numeric_range',
                parameters={'min_value': 13, 'max_value': 120},
                severity=ValidationSeverity.WARNING,
                description='Age should be between 13 and 120'
            ),
            ValidationRule(
                field='email',
                rule_type='email_valid',
                parameters={},
                severity=ValidationSeverity.WARNING,
                description='Email should be valid format if present'
            ),
            ValidationRule(
                field='signup_date',
                rule_type='timestamp_valid',
                parameters={'format': 'iso8601'},
                severity=ValidationSeverity.ERROR,
                description='Signup date must be valid timestamp'
            ),
            ValidationRule(
                field='country',
                rule_type='country_code',
                parameters={'format': 'iso2'},
                severity=ValidationSeverity.WARNING,
                description='Country should be valid ISO2 code'
            )
        ]
    
    def _get_product_features_rules(self) -> List[ValidationRule]:
        """Define validation rules for product features."""
        return [
            ValidationRule(
                field='product_id',
                rule_type='required',
                parameters={},
                severity=ValidationSeverity.ERROR,
                description='Product ID must be present'
            ),
            ValidationRule(
                field='product_id',
                rule_type='format',
                parameters={'pattern': r'^[a-zA-Z0-9_-]+, 'min_length': 3, 'max_length': 50},
                severity=ValidationSeverity.ERROR,
                description='Product ID must be alphanumeric'
            ),
            ValidationRule(
                field='price',
                rule_type='numeric_range',
                parameters={'min_value': 0, 'max_value': 100000},
                severity=ValidationSeverity.ERROR,
                description='Price must be positive and reasonable'
            ),
            ValidationRule(
                field='category',
                rule_type='required',
                parameters={},
                severity=ValidationSeverity.WARNING,
                description='Category should be present'
            ),
            ValidationRule(
                field='brand',
                rule_type='string_length',
                parameters={'min_length': 1, 'max_length': 100},
                severity=ValidationSeverity.WARNING,
                description='Brand should be reasonable length'
            ),
            ValidationRule(
                field='availability',
                rule_type='enum',
                parameters={'allowed_values': ['in_stock', 'out_of_stock', 'discontinued']},
                severity=ValidationSeverity.WARNING,
                description='Availability must be valid status'
            )
        ]
    
    async def validate_batch(self, records: List[Dict], data_type: str) -> Dict[str, Any]:
        """Validate a batch of records."""
        if data_type not in self.validation_rules:
            logger.warning(f"No validation rules defined for data type: {data_type}")
            return {
                'valid': [True] * len(records),
                'validation_results': [],
                'stats': {'total': len(records), 'valid': len(records), 'invalid': 0}
            }
        
        rules = self.validation_rules[data_type]
        batch_results = []
        valid_flags = []
        
        for record in records:
            record_results = await self._validate_single_record(record, rules)
            batch_results.append(record_results)
            
            # Determine if record is valid (no ERROR or CRITICAL failures)
            has_critical_errors = any(
                not result.passed and result.rule.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]
                for result in record_results
            )
            valid_flags.append(not has_critical_errors)
        
        # Update statistics
        stats = self._calculate_batch_stats(batch_results, valid_flags)
        self.stats_tracker.update_stats(data_type, stats)
        
        return {
            'valid': valid_flags,
            'validation_results': batch_results,
            'stats': stats,
            'summary': self._generate_validation_summary(batch_results, data_type)
        }
    
    async def _validate_single_record(self, record: Dict, rules: List[ValidationRule]) -> List[ValidationResult]:
        """Validate a single record against all rules."""
        results = []
        
        for rule in rules:
            try:
                result = await self._apply_validation_rule(record, rule)
                results.append(result)
            except Exception as e:
                logger.error(f"Error applying rule {rule.rule_type} to field {rule.field}: {e}")
                results.append(ValidationResult(
                    rule=rule,
                    passed=False,
                    message=f"Validation error: {str(e)}",
                    suggested_fix="Check data format and rule configuration"
                ))
        
        return results
    
    async def _apply_validation_rule(self, record: Dict, rule: ValidationRule) -> ValidationResult:
        """Apply a single validation rule to a record."""
        field_value = record.get(rule.field)
        
        if rule.rule_type == 'required':
            passed = field_value is not None and field_value != ""
            message = f"Field {rule.field} is required" if not passed else "Field present"
            suggested_fix = f"Ensure {rule.field} is provided" if not passed else None
            
        elif rule.rule_type == 'format':
            passed = self._validate_format(field_value, rule.parameters)
            message = f"Field {rule.field} format validation {'passed' if passed else 'failed'}"
            suggested_fix = f"Ensure {rule.field} matches pattern {rule.parameters.get('pattern')}" if not passed else None
            
        elif rule.rule_type == 'enum':
            allowed_values = rule.parameters['allowed_values']
            passed = field_value in allowed_values
            message = f"Field {rule.field} value {'valid' if passed else 'invalid'}"
            suggested_fix = f"Use one of: {allowed_values}" if not passed else None
            
        elif rule.rule_type == 'numeric_range':
            passed = self._validate_numeric_range(field_value, rule.parameters)
            message = f"Field {rule.field} numeric range validation {'passed' if passed else 'failed'}"
            suggested_fix = f"Value should be between {rule.parameters.get('min_value')} and {rule.parameters.get('max_value')}" if not passed else None
            
        elif rule.rule_type == 'timestamp_valid':
            passed = self._validate_timestamp(field_value, rule.parameters)
            message = f"Field {rule.field} timestamp validation {'passed' if passed else 'failed'}"
            suggested_fix = "Use ISO8601 format (YYYY-MM-DDTHH:MM:SSZ)" if not passed else None
            
        elif rule.rule_type == 'timestamp_range':
            passed = self._validate_timestamp_range(field_value, rule.parameters)
            message = f"Field {rule.field} timestamp range validation {'passed' if passed else 'failed'}"
            suggested_fix = "Check if timestamp is within reasonable range" if not passed else None
            
        elif rule.rule_type == 'conditional_required':
            passed = self._validate_conditional_required(record, rule.parameters)
            message = f"Conditional requirement for {rule.field} {'satisfied' if passed else 'failed'}"
            suggested_fix = f"Provide {rule.field} when {rule.parameters['condition_field']} is in {rule.parameters['condition_values']}" if not passed else None
            
        elif rule.rule_type == 'url_valid':
            passed = self._validate_url(field_value)
            message = f"Field {rule.field} URL validation {'passed' if passed else 'failed'}"
            suggested_fix = "Use valid URL format (http://... or https://...)" if not passed else None
            
        elif rule.rule_type == 'email_valid':
            passed = self._validate_email(field_value)
            message = f"Field {rule.field} email validation {'passed' if passed else 'failed'}"
            suggested_fix = "Use valid email format (user@domain.com)" if not passed else None
            
        elif rule.rule_type == 'country_code':
            passed = self._validate_country_code(field_value, rule.parameters)
            message = f"Field {rule.field} country code validation {'passed' if passed else 'failed'}"
            suggested_fix = "Use valid ISO2 country code (e.g., US, GB, FR)" if not passed else None
            
        elif rule.rule_type == 'string_length':
            passed = self._validate_string_length(field_value, rule.parameters)
            message = f"Field {rule.field} length validation {'passed' if passed else 'failed'}"
            suggested_fix = f"String length should be between {rule.parameters.get('min_length')} and {rule.parameters.get('max_length')}" if not passed else None
            
        else:
            passed = False
            message = f"Unknown validation rule type: {rule.rule_type}"
            suggested_fix = "Check validation rule configuration"
        
        return ValidationResult(
            rule=rule,
            passed=passed,
            message=message,
            field_value=field_value,
            suggested_fix=suggested_fix
        )
    
    def _validate_format(self, value: Any, parameters: Dict) -> bool:
        """Validate field format using regex pattern and length constraints."""
        if value is None:
            return True  # Format validation only applies to non-null values
        
        value_str = str(value)
        
        # Check length constraints
        if 'min_length' in parameters and len(value_str) < parameters['min_length']:
            return False
        if 'max_length' in parameters and len(value_str) > parameters['max_length']:
            return False
        
        # Check pattern
        if 'pattern' in parameters:
            pattern = parameters['pattern']
            return bool(re.match(pattern, value_str))
        
        return True
    
    def _validate_numeric_range(self, value: Any, parameters: Dict) -> bool:
        """Validate numeric value is within specified range."""
        if value is None:
            return True
        
        try:
            num_value = float(value)
            if 'min_value' in parameters and num_value < parameters['min_value']:
                return False
            if 'max_value' in parameters and num_value > parameters['max_value']:
                return False
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_timestamp(self, value: Any, parameters: Dict) -> bool:
        """Validate timestamp format."""
        if value is None:
            return True
        
        try:
            if parameters.get('format') == 'iso8601':
                # Try to parse ISO8601 format
                datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                return True
        except (ValueError, AttributeError):
            try:
                # Fallback to general datetime parsing
                pd.to_datetime(value)
                return True
            except:
                return False
        
        return False
    
    def _validate_timestamp_range(self, value: Any, parameters: Dict) -> bool:
        """Validate timestamp is within reasonable range."""
        if value is None:
            return True
        
        try:
            timestamp = pd.to_datetime(value)
            now = datetime.now()
            
            # Check if too old
            if 'max_days_old' in parameters:
                max_old_date = now - timedelta(days=parameters['max_days_old'])
                if timestamp < max_old_date:
                    return False
            
            # Check if too far in future
            if 'max_days_future' in parameters:
                max_future_date = now + timedelta(days=parameters['max_days_future'])
                if timestamp > max_future_date:
                    return False
            
            return True
        except:
            return False
    
    def _validate_conditional_required(self, record: Dict, parameters: Dict) -> bool:
        """Validate conditional requirement based on other field values."""
        condition_field = parameters['condition_field']
        condition_values = parameters['condition_values']
        
        condition_value = record.get(condition_field)
        if condition_value in condition_values:
            # Field is required, check if it exists
            field_name = parameters.get('field')  # This should be set by the caller
            return record.get(field_name) is not None and record.get(field_name) != ""
        
        return True  # Not required in this case
    
    def _validate_url(self, value: Any) -> bool:
        """Validate URL format."""
        if value is None:
            return True
        
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+), re.IGNORECASE)
        
        return bool(url_pattern.match(str(value)))
    
    def _validate_email(self, value: Any) -> bool:
        """Validate email format."""
        if value is None:
            return True
        
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})
        return bool(email_pattern.match(str(value)))
    
    def _validate_country_code(self, value: Any, parameters: Dict) -> bool:
        """Validate country code format."""
        if value is None:
            return True
        
        # ISO2 country codes
        iso2_codes = {
            'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT',
            'AU', 'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI',
            'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY',
            'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN',
            'CO', 'CR', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM',
            'DO', 'DZ', 'EC', 'EE', 'EG', 'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK',
            'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 'GE', 'GF', 'GG', 'GH', 'GI', 'GL',
            'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM',
            'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR',
            'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN',
            'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS',
            'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 'MK',
            'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW',
            'MX', 'MY', 'MZ', 'NA', 'NC', 'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP',
            'NR', 'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM',
            'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW',
            'SA', 'SB', 'SC', 'SD', 'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM',
            'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF',
            'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW',
            'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI',
            'VN', 'VU', 'WF', 'WS', 'YE', 'YT', 'ZA', 'ZM', 'ZW'
        }
        
        if parameters.get('format') == 'iso2':
            return str(value).upper() in iso2_codes
        
        return True
    
    def _validate_string_length(self, value: Any, parameters: Dict) -> bool:
        """Validate string length constraints."""
        if value is None:
            return True
        
        value_str = str(value)
        
        if 'min_length' in parameters and len(value_str) < parameters['min_length']:
            return False
        if 'max_length' in parameters and len(value_str) > parameters['max_length']:
            return False
        
        return True
    
    def _calculate_batch_stats(self, batch_results: List[List[ValidationResult]], 
                             valid_flags: List[bool]) -> Dict[str, Any]:
        """Calculate comprehensive statistics for a batch."""
        total_records = len(batch_results)
        valid_records = sum(valid_flags)
        invalid_records = total_records - valid_records
        
        # Count validation issues by severity
        severity_counts = {severity.value: 0 for severity in ValidationSeverity}
        rule_failure_counts = {}
        
        for record_results in batch_results:
            for result in record_results:
                if not result.passed:
                    severity_counts[result.rule.severity.value] += 1
                    rule_key = f"{result.rule.field}_{result.rule.rule_type}"
                    rule_failure_counts[rule_key] = rule_failure_counts.get(rule_key, 0) + 1
        
        return {
            'total': total_records,
            'valid': valid_records,
            'invalid': invalid_records,
            'validity_rate': valid_records / total_records if total_records > 0 else 0,
            'severity_counts': severity_counts,
            'top_failures': sorted(rule_failure_counts.items(), 
                                 key=lambda x: x[1], reverse=True)[:10],
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_validation_summary(self, batch_results: List[List[ValidationResult]], 
                                   data_type: str) -> Dict[str, Any]:
        """Generate human-readable validation summary."""
        total_issues = sum(len([r for r in record_results if not r.passed]) 
                          for record_results in batch_results)
        
        critical_issues = sum(len([r for r in record_results 
                                 if not r.passed and r.rule.severity == ValidationSeverity.CRITICAL]) 
                            for record_results in batch_results)
        
        error_issues = sum(len([r for r in record_results 
                              if not r.passed and r.rule.severity == ValidationSeverity.ERROR]) 
                         for record_results in batch_results)
        
        return {
            'data_type': data_type,
            'total_validation_issues': total_issues,
            'critical_issues': critical_issues,
            'error_issues': error_issues,
            'quality_score': max(0, 100 - (critical_issues * 10 + error_issues * 5)),
            'recommendation': self._get_quality_recommendation(critical_issues, error_issues, total_issues)
        }
    
    def _get_quality_recommendation(self, critical: int, errors: int, total: int) -> str:
        """Get quality improvement recommendations."""
        if critical > 0:
            return "CRITICAL: Address critical data quality issues before proceeding"
        elif errors > total * 0.1:
            return "HIGH: Significant data quality issues detected, review data sources"
        elif errors > 0:
            return "MEDIUM: Minor data quality issues, monitor and improve data collection"
        else:
            return "GOOD: Data quality is acceptable"

class ValidationStatsTracker:
    """Track validation statistics over time."""
    
    def __init__(self):
        self.stats_history = {}
    
    def update_stats(self, data_type: str, stats: Dict[str, Any]) -> None:
        """Update statistics for a data type."""
        if data_type not in self.stats_history:
            self.stats_history[data_type] = []
        
        self.stats_history[data_type].append(stats)
        
        # Keep only last 100 entries to prevent memory issues
        if len(self.stats_history[data_type]) > 100:
            self.stats_history[data_type] = self.stats_history[data_type][-100:]
    
    def get_trend_analysis(self, data_type: str, days: int = 7) -> Dict[str, Any]:
        """Get trend analysis for data quality over time."""
        if data_type not in self.stats_history:
            return {}
        
        recent_stats = self.stats_history[data_type][-days:]
        if not recent_stats:
            return {}
        
        validity_rates = [s['validity_rate'] for s in recent_stats]
        
        return {
            'average_validity_rate': np.mean(validity_rates),
            'validity_trend': 'improving' if validity_rates[-1] > validity_rates[0] else 'declining',
            'stability': np.std(validity_rates),
            'recent_issues': recent_stats[-1]['severity_counts'] if recent_stats else {}
        }