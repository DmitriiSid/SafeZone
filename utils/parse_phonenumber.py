import phonenumbers
import phonenumbers.geocoder
import pandas
def py_parse_phonenumber(NUM: str):
    try:
        num = phonenumbers.parse(NUM, 'CZ')
        phonenumbers.is_possible_number_with_reason(num)
        return {
            'number': num.national_number,
            'prefix': num.country_code,
            'country_code': phonenumbers.region_code_for_number(num),
            'valid': phonenumbers.is_valid_number(num),
            'possible': phonenumbers.is_possible_number(num),
            'parsed': True
        }
    except:
        return {'number': NUM, 'prefix': None, 'country_code': None, 'valid': False, 'possible': False, 'parsed': False}
def udf(df: pandas.DataFrame):
    return df[0].map(py_parse_phonenumber)
udf._sf_vectorized_input = pandas.DataFrame