from src.data_validations.duplicate_validation import duplicate_check
def test_two(read_data):
    source, target = read_data
    source.show()
    target.show()
    assert True

def test_duplicate_check(read_data,read_config):
    source, target = read_data
    read_config = read_config
    key_columns = read_config['validations']['duplicate_check']['key_columns']
    status = duplicate_check( df=target,key_col=key_columns)
    assert status == 'PASS'