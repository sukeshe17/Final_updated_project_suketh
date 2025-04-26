from src.data_validations.count_check import count_val
from src.data_validations.duplicate_validation import duplicate_check
from src.data_validations.uniqueness_check import uniqueness_check
from src.data_validations.null_validation import null_value_check
from src.data_validations.records_only_in_source import records_only_in_source
from src.data_validations.records_only_target import records_only_in_target
from src.data_validations.data_compare import data_compare
def test_count_check(read_data,read_config):
    source, target = read_data
    read_config = read_config
    key_columns = read_config['validations']['count_check']['key_columns']
    status = count_val(source=source, target=target,key_columns=key_columns)
    assert status == 'PASS'


# def test_duplicate_check(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     key_columns = read_config['validations']['duplicate_check']['key_columns']
#     status = duplicate_check( df=target,key_col=key_columns)
#     assert status == 'PASS'
#
# def test_uniqueness_check(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     unique_cols = read_config['validations']['uniqueness_check']['unique_columns']
#     status = uniqueness_check( df=target,unique_cols=unique_cols)
#     assert status == 'PASS'
#
# def test_null_check(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     null_cols = read_config['validations']['null_check']['null_columns']
#     status = null_value_check( df=target,null_cols=null_cols)
#     assert status == 'PASS'
#
# def test_records_only_source(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     key_columns = read_config['validations']['count_check']['key_columns']
#     status = records_only_in_source( source_df=source, target_df=target, key_columns=key_columns)
#     assert status == 'PASS'
#
# def test_records_only_target(read_data,read_config):
#     source, target = read_data
#     read_config = read_config
#     key_columns = read_config['validations']['count_check']['key_columns']
#     status = records_only_in_target( source_df=source, target_df=target, key_columns=key_columns)
#     assert status == 'PASS'
#
# def test_data_compare_check(read_data,read_config):
#     source, target = read_data
#
#     read_config = read_config
#     key_columns = read_config['validations']['data_compare_check']['key_column']
#     num_records = read_config['validations']['data_compare_check']['num_records']
#     validate_columns = read_config['validations']['data_compare_check']['validate_columns']
#     status = data_compare(source=source, target=target, key_column=key_columns)
#     assert status == 'PASS'