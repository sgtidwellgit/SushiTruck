import pandas as pd
import pytest

from sushitruck import wasabi


def test_flatten_nested_object():
    df = wasabi.flatten({"user": {"id": 1, "name": "Alice"}, "score": 0.9})
    assert list(df.columns) == ["user_id", "user_name", "score"]
    assert df.iloc[0]["user_id"] == 1
    assert df.iloc[0]["user_name"] == "Alice"


def test_flatten_list_of_records():
    data = [
        {"event": {"type": "click", "ts": 1700000000}, "user_id": 42},
        {"event": {"type": "view", "ts": 1700000001}, "user_id": 43},
    ]
    df = wasabi.flatten(data)
    assert set(df.columns) == {"event_type", "event_ts", "user_id"}
    assert len(df) == 2


def test_flatten_raw_json_string():
    df = wasabi.flatten('{"price": 142.5, "meta": {"source": "bloomberg"}}')
    assert df.iloc[0]["price"] == 142.5
    assert df.iloc[0]["meta_source"] == "bloomberg"


def test_flatten_respects_max_depth():
    df = wasabi.flatten({"a": {"b": {"c": 1}}}, max_depth=1)
    assert "a_b" in df.columns
    assert df.iloc[0]["a_b"] == {"c": 1}


def test_flatten_drop_empty_removes_all_null_columns():
    data = [{"a": 1, "b": None}, {"a": 2, "b": None}]
    df = wasabi.flatten(data, drop_empty=True)
    assert "b" not in df.columns

    df_kept = wasabi.flatten(data, drop_empty=False)
    assert "b" in df_kept.columns


def test_normalize_coerces_and_renames():
    df = pd.DataFrame({"price": ["1.5", "2.5"], "symbol": ["AAA", "BBB"]})
    schema = {
        "price": {"dtype": float, "nullable": False, "rename": "close_price"},
        "symbol": {"dtype": str, "nullable": False},
    }
    result = wasabi.normalize(df, schema, coerce=True)
    assert "close_price" in result.columns
    assert result["close_price"].dtype == float
    assert result["close_price"].tolist() == [1.5, 2.5]


def test_normalize_raises_on_null_in_non_nullable():
    df = pd.DataFrame({"price": [1.0, None]})
    schema = {"price": {"dtype": float, "nullable": False}}
    with pytest.raises(ValueError):
        wasabi.normalize(df, schema)


def test_normalize_fills_default_before_null_check():
    df = pd.DataFrame({"price": [1.0, None]})
    schema = {"price": {"dtype": float, "nullable": False, "default": 0.0}}
    result = wasabi.normalize(df, schema)
    assert result["price"].tolist() == [1.0, 0.0]


def test_normalize_strict_rejects_extra_columns():
    df = pd.DataFrame({"price": [1.0], "extra": ["x"]})
    schema = {"price": {"dtype": float, "nullable": True}}
    with pytest.raises(ValueError):
        wasabi.normalize(df, schema, strict=True)


def test_normalize_raises_on_missing_schema_column():
    df = pd.DataFrame({"price": [1.0]})
    schema = {"price": {"dtype": float}, "symbol": {"dtype": str}}
    with pytest.raises(ValueError):
        wasabi.normalize(df, schema)


def test_normalize_coerces_datetime():
    df = pd.DataFrame({"ts": ["2024-01-01", "2024-01-02"]})
    schema = {"ts": {"dtype": "datetime64[ns]", "nullable": True}}
    result = wasabi.normalize(df, schema)
    assert pd.api.types.is_datetime64_any_dtype(result["ts"])


def test_infer_schema_detects_types():
    df = pd.DataFrame({
        "price": [1.0, 2.0],
        "count": [1, 2],
        "name": ["a", "b"],
        "flag": [True, False],
    })
    schema = wasabi.infer_schema(df)
    assert schema["price"]["dtype"] is float
    assert schema["count"]["dtype"] is int
    assert schema["name"]["dtype"] is str
    assert schema["flag"]["dtype"] is bool
    assert schema["price"]["nullable"] is False


def test_infer_schema_detects_nullable():
    df = pd.DataFrame({"price": [1.0, None]})
    schema = wasabi.infer_schema(df)
    assert schema["price"]["nullable"] is True


def test_normalize_warns_on_extra_columns_when_not_strict():
    df = pd.DataFrame({"price": ["1.5"], "extra": [1]})

    with pytest.warns(UserWarning, match="extra"):
        result = wasabi.normalize(df, {"price": {"dtype": float}})

    assert "extra" in result.columns
    assert result["price"].dtype == float


def test_normalize_does_not_warn_when_columns_match():
    import warnings

    df = pd.DataFrame({"price": ["1.5"]})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        wasabi.normalize(df, {"price": {"dtype": float}})


def test_batch_normalizer_warns_once_even_when_extra_columns_vary():
    import warnings

    normalize = wasabi._BatchNormalizer({"price": {"dtype": float}})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(3):
            out = normalize(pd.DataFrame({"price": ["1"], "a": [1]}))
        normalize(pd.DataFrame({"price": ["1"], "b": [1]}))

    messages = [str(w.message) for w in caught]
    assert len(messages) == 1
    assert "'a'" in messages[0] and "shown once" in messages[0]
    assert out["price"].dtype == float


def test_extra_columns_warning_lists_at_most_ten_names():
    df = pd.DataFrame({"keep": [1], **{f"c{i:02d}": [i] for i in range(25)}})

    with pytest.warns(UserWarning) as record:
        wasabi.normalize(df, {"keep": {"dtype": int}})

    message = str(record[0].message)
    assert "'c09'" in message and "'c10'" not in message
    assert message.endswith("and 15 more")


def test_normalize_still_warns_on_every_direct_call():
    import warnings

    df = pd.DataFrame({"price": ["1"], "a": [1]})
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        wasabi.normalize(df, {"price": {"dtype": float}})
        wasabi.normalize(df, {"price": {"dtype": float}})

    assert len(caught) == 2
