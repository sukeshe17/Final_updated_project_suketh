
from src.data_validations.count_check import count_val
# from src.data_validations.duplicate_validation import duplicate_check
# from src.data_validations.uniqueness_check import uniqueness_check
# from src.data_validations.null_validation import null_value_check
def test_count_check(read_data,read_config):
    source, target = read_data
    read_config = read_config
    key_columns = read_config['validations']['count_check']['key_columns']
    status = count_val(source=source, target=target,key_columns=key_columns)
    assert status == 'PASS'