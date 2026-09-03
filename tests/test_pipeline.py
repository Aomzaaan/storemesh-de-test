"""Unit tests for pipeline transformation functions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest

from pipeline import clean_phone, _clean_orders_logic


# ═══════════════════════════════════════════════════════════
# Test 1: clean_phone (unchanged — คงเดิมได้เลย)
# ═══════════════════════════════════════════════════════════
class TestCleanPhone:
    """Tests for phone number cleaning logic."""

    def test_removes_special_characters(self):
        assert clean_phone("+1 (555) 123-4567") == "15551234567"

    def test_already_clean_number(self):
        assert clean_phone("15551234567") == "15551234567"

    def test_thai_phone_format(self):
        assert clean_phone("+66 89-123-4567") == "66891234567"

    def test_handles_none(self):
        assert clean_phone(None) is None

    def test_handles_nan(self):
        assert clean_phone(pd.NA) is None

    def test_handles_empty_string(self):
        assert clean_phone("") == ""

    def test_removes_letters(self):
        assert clean_phone("call 555-1234 pls") == "5551234"

    def test_handles_int_input(self):
        assert clean_phone(15551234567) == "15551234567"

    def test_handles_float_dropping_decimal(self):
        assert clean_phone(15551234567.0) == "15551234567"

    def test_only_special_chars_returns_empty(self):
        assert clean_phone("+++") == ""


# ═══════════════════════════════════════════════════════════
# Test 2: _clean_orders_logic (pure function → no Prefect)
# ═══════════════════════════════════════════════════════════
class TestCleanOrders:
    """Tests for order cleaning + currency conversion."""

    @pytest.fixture
    def sample_orders(self):
        """Dummy orders DataFrame."""
        return pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "customer_id": [100, 101, 102, 103, 104],
                "order_date": [
                    "2023-01-01",
                    "2023-01-02",
                    "2023-01-03",
                    "2023-01-04",
                    "2023-01-05",
                ],
                "total_amount": [100.0, -50.0, 0.0, 200.0, 300.0],
                "currency": ["USD", "USD", "USD", "EUR", None],
                "status": ["COMPLETED"] * 5,
            }
        )

    @pytest.fixture
    def sample_rates(self):
        """Dummy exchange rates DataFrame."""
        return pd.DataFrame(
            {
                "currency": ["EUR", "GBP"],
                "date": ["2023-01-04", "2023-01-05"],
                "rate_to_usd": [1.1, 1.25],
            }
        )

    def test_filters_negative_amounts(self, sample_orders, sample_rates):
        """Orders with total_amount < 0 should be removed."""
        result = _clean_orders_logic(sample_orders, sample_rates)
        assert len(result) == 3
        assert (result["total_amount"] > 0).all()

    def test_filters_zero_amount(self, sample_orders, sample_rates):
        """Orders with total_amount == 0 should be removed."""
        result = _clean_orders_logic(sample_orders, sample_rates)
        assert 3 not in result["order_id"].values

    def test_currency_conversion_eur(self, sample_orders, sample_rates):
        """EUR order should be converted using exchange rate."""
        result = _clean_orders_logic(sample_orders, sample_rates)
        eur_row = result[result["order_id"] == 4].iloc[0]
        assert eur_row["usd_amount"] == pytest.approx(220.0)

    def test_missing_currency_defaults_to_usd(self, sample_orders, sample_rates):
        """Missing currency should default to USD (rate = 1.0)."""
        result = _clean_orders_logic(sample_orders, sample_rates)
        row = result[result["order_id"] == 5].iloc[0]
        assert row["currency"] == "USD"
        assert row["usd_amount"] == pytest.approx(300.0)

    def test_missing_rate_defaults_to_one(self, sample_orders, sample_rates):
        """Order with currency but missing rate → default rate = 1.0."""
        orders = pd.concat(
            [
                sample_orders,
                pd.DataFrame(
                    {
                        "order_id": [6],
                        "customer_id": [105],
                        "order_date": ["2023-01-06"],
                        "total_amount": [1000.0],
                        "currency": ["JPY"],
                        "status": ["COMPLETED"],
                    }
                ),
            ],
            ignore_index=True,
        )

        result = _clean_orders_logic(orders, sample_rates)
        jpy_row = result[result["order_id"] == 6].iloc[0]
        assert jpy_row["usd_amount"] == pytest.approx(1000.0)

    def test_usd_amount_column_added(self, sample_orders, sample_rates):
        """Cleaned orders should have usd_amount column."""
        result = _clean_orders_logic(sample_orders, sample_rates)
        assert "usd_amount" in result.columns
